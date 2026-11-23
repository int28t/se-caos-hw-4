#include "net.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static Peer peers[MAX_PEERS];
typedef struct Session {
  Model model;
  uint64_t start, tick, lastArrival[MAX_STREAMS];
  unsigned received;
  bool started, failed;
} Session;
static bool Reject(Peer *p, const char *reason) {
  p->closing = true;
  return NetQueue(p, reason);
}
static void Event(void *context, int stream, uint64_t sequence, uint64_t now,
                  const char *text) {
  Session *s = context;
  char line[LINE_SIZE];
  int n = snprintf(line, sizeof(line), "EVENT %" PRIu64 " %" PRIu64 " %d %s\n",
                   sequence, now, stream, text);
  if (n < 0 || (size_t)n >= sizeof(line)) {
    s->failed = true;
    return;
  }
  if (stream < 0 && !s->failed && !WriteText(STDOUT_FILENO, line))
    s->failed = true;
  for (unsigned i = 0; i < MAX_PEERS; ++i) {
    Peer *p = &peers[i];
    if (p->fd >= 0 && !p->closing &&
        (p->role == 2 || (p->role == 1 && p->stream == stream)) &&
        !NetQueue(p, line))
      p->closing = true;
  }
}
static bool Watch(Peer *p, const Model *m) {
  Snapshot view;
  ModelSnapshot(m, &view);
  if (!NetQueue(p, "WELCOME WATCH\n"))
    return false;
  char line[LINE_SIZE];
  for (unsigned i = 0; i < m->cfg.streamCount; ++i) {
    const Stream *f = &m->cfg.streams[i];
    int n = snprintf(
        line, sizeof(line), "META STREAM %u %s %u %u %u %u %u %u\n", i, f->name,
        (unsigned)f->kind, (unsigned)f->direction, (unsigned)f->maneuver,
        f->capacity, f->crossingTicks, m->cfg.conflicts[i]);
    if (n < 0 || (size_t)n >= sizeof(line) || !NetQueue(p, line))
      return false;
  }
  for (unsigned i = 0; i < m->cfg.phaseCount; ++i) {
    int n = snprintf(line, sizeof(line), "META PHASE %u %s %u\n", i,
                     m->cfg.phases[i].name, m->cfg.phases[i].mask);
    if (n < 0 || (size_t)n >= sizeof(line) || !NetQueue(p, line))
      return false;
  }
  int n = snprintf(line, sizeof(line),
                   "META TIMING %u %u %u %u\nSNAP_BEGIN %" PRIu64 " %" PRIu64
                   "\nSTATE %u %u %u %u %" PRIu64 " %u\n",
                   m->cfg.tickMs, m->cfg.minGreen, m->cfg.maxGreen,
                   m->cfg.clearance, view.sequence, view.now, view.phase,
                   (unsigned)view.state, view.signals, view.active, view.passed,
                   (unsigned)view.stop);
  if (n < 0 || (size_t)n >= sizeof(line) || !NetQueue(p, line))
    return false;
  for (unsigned i = 0; i < view.count; ++i) {
    const FlowView *f = &view.flows[i];
    n = snprintf(line, sizeof(line),
                 "FLOW %u %u %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64
                 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %u\n",
                 i, f->queued, f->headId, f->activeId, f->leaveAt, f->arrived,
                 f->passed, f->refused, f->waitSum, f->waitMax, f->peak);
    if (n < 0 || (size_t)n >= sizeof(line) || !NetQueue(p, line))
      return false;
  }
  n = snprintf(line, sizeof(line), "SNAP_END %" PRIu64 "\n", view.sequence);
  return n > 0 && (size_t)n < sizeof(line) && NetQueue(p, line);
}
static uint64_t Time(const Session *s) { return (NetNow() - s->start) * NS; }
static bool Line(Peer *p, const char *line, void *context) {
  Session *s = context;
  const Config *c = &s->model.cfg;
  if (p->role) {
    if (!strcmp(line, "PING"))
      return NetQueue(p, "PONG\n");
    if (s->model.stop)
      return true;
    uint64_t values[2];
    if (p->role != 1 || !s->started ||
        !NetNumbers(line, "ARRIVE ", values, 2) || values[1] > 1 ||
        (values[1] && c->streams[p->stream].kind != CAR))
      return Reject(p, "ERROR command\n");
    ModelAdvance(&s->model, Time(s));
    if (s->model.stop)
      return true;
    unsigned i = (unsigned)p->stream;
    uint64_t current = s->model.now / s->model.tick;
    if (values[0] > current || current - values[0] > 1 ||
        ((s->received & (1u << i)) && values[0] <= s->lastArrival[i]))
      return Reject(p, "ERROR tick\n");
    s->received |= 1u << i;
    s->lastArrival[i] = values[0];
    bool accepted = ModelArrive(&s->model, i, values[1] != 0, s->model.now);
    char reply[128];
    int n = snprintf(reply, sizeof(reply), "ACK %" PRIu64 " %" PRIu64 " %u\n",
                     values[0], s->model.nextId, (unsigned)accepted);
    ModelAdvance(&s->model, s->model.now);
    return !p->closing && n > 0 && (size_t)n < sizeof(reply) &&
           NetQueue(p, reply);
  }
  if (!strcmp(line, "HELLO 1 WATCH")) {
    if (s->started)
      ModelAdvance(&s->model, Time(s));
    p->role = 2;
    return Watch(p, &s->model);
  }
  const char *prefix = "HELLO 1 FLOW ";
  if (strncmp(line, prefix, strlen(prefix)))
    return Reject(p, "ERROR hello\n");
  for (unsigned i = 0; i < c->streamCount; ++i) {
    if (strcmp(line + strlen(prefix), c->streams[i].name))
      continue;
    for (unsigned j = 0; j < MAX_PEERS; ++j)
      if (peers[j].fd >= 0 && peers[j].role == 1 && peers[j].stream == (int)i)
        return Reject(p, "ERROR occupied\n");
    p->role = 1;
    p->stream = (int)i;
    char reply[128];
    int n = snprintf(reply, sizeof(reply), "WELCOME %u %u %u %u %u\n", i,
                     c->seed, c->streams[i].rate, (unsigned)c->streams[i].kind,
                     c->emergencyRate);
    return n > 0 && (size_t)n < sizeof(reply) && NetQueue(p, reply);
  }
  return Reject(p, "ERROR stream\n");
}
static bool Registered(unsigned count) {
  unsigned mask = 0;
  for (unsigned i = 0; i < MAX_PEERS; ++i)
    if (peers[i].fd >= 0 && peers[i].role == 1 && !peers[i].closing)
      mask |= 1u << (unsigned)peers[i].stream;
  return mask == (1u << count) - 1;
}
static unsigned Delay(const Session *s) {
  if (!s->started)
    return 100;
  const Model *m = &s->model;
  uint64_t next = (m->now / m->tick + 1) * m->tick;
  uint64_t phase = m->since + (m->state == CLEARANCE ? m->cfg.clearance
                               : m->now - m->since < m->cfg.minGreen * m->tick
                                   ? m->cfg.minGreen
                                   : m->cfg.maxGreen) *
                                  m->tick;
  if ((m->state == GREEN || m->state == CLEARANCE) && phase > m->now &&
      phase < next)
    next = phase;
  for (unsigned i = 0; i < m->cfg.streamCount; ++i)
    if (m->flows[i].active.id && m->flows[i].leaveAt < next)
      next = m->flows[i].leaveAt;
  if (m->cfg.ticks && m->cfg.ticks * m->tick < next)
    next = m->cfg.ticks * m->tick;
  uint64_t wait = next > m->now ? (next - m->now + NS - 1) / NS : 0;
  return wait > 100 ? 100 : (unsigned)wait;
}
int main(int argc, char **argv) {
  if (argc != 4 || !NetAddress(argv[1], argv[2])) {
    fprintf(stderr, "Использование: crossroad_server BIND_IP PORT CONFIG\n");
    return 2;
  }
  Session s = {.tick = UINT64_MAX};
  Config c;
  if (!ConfigLoad(&c, argv[3]))
    return 2;
  if (!NetSignals()) {
    perror("sigaction");
    return 1;
  }
  int flags = fcntl(STDOUT_FILENO, F_GETFL);
  if (flags < 0 || fcntl(STDOUT_FILENO, F_SETFL, flags | O_NONBLOCK) < 0)
    return 1;
  int listener = NetListen(argv[1], argv[2]);
  if (listener < 0) {
    perror("listen");
    (void)fcntl(STDOUT_FILENO, F_SETFL, flags);
    return 1;
  }
  for (unsigned i = 0; i < MAX_PEERS; ++i)
    peers[i].fd = -1;
  ModelInit(&s.model, &c, Event, &s);
  uint64_t boot = NetNow();
  while (!interrupted && !s.model.stop) {
    if (!s.started && Registered(c.streamCount)) {
      s.started = true;
      s.start = NetNow();
    }
    if (s.started) {
      ModelAdvance(&s.model, Time(&s));
      uint64_t tick = s.model.now / s.model.tick;
      if (!s.model.stop && tick != s.tick) {
        s.tick = tick;
        char line[64];
        int n = snprintf(line, sizeof(line), "TICK %" PRIu64 "\n", tick);
        if (n < 0 || (size_t)n >= sizeof(line))
          s.failed = true;
        else
          for (unsigned i = 0; i < MAX_PEERS; ++i)
            if (peers[i].fd >= 0 && peers[i].role == 1 && !peers[i].closing &&
                !NetQueue(&peers[i], line))
              peers[i].closing = true;
      }
    } else if (NetNow() - boot >= 10000)
      s.failed = true;
    if (s.failed)
      ModelStop(&s.model, ERROR, 0);
    if (s.model.stop)
      break;
    bool ready;
    if (NetWait(peers, MAX_PEERS, listener, &ready, Delay(&s)) < 0) {
      if (errno == EINTR)
        continue;
      perror("select");
      s.failed = true;
      continue;
    }
    if (ready) {
      int fd = accept(listener, NULL, NULL);
      if (fd >= 0) {
        unsigned i = 0;
        while (i < MAX_PEERS && peers[i].fd >= 0)
          ++i;
        if (i == MAX_PEERS || !NetPrepare(fd))
          close(fd);
        else
          peers[i] = (Peer){.fd = fd, .stream = -1, .last = NetNow()};
      } else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
        s.failed = true;
    }
    for (unsigned i = 0; i < MAX_PEERS; ++i) {
      Peer *p = &peers[i];
      if (p->fd >= 0 &&
          ((p->readable && !NetRead(p, Line, &s)) ||
           (p->writable && !NetFlush(p)) || NetNow() - p->last >= 5000 ||
           (p->closing && p->size == p->sent)))
        NetClose(p);
    }
  }
  if (interrupted)
    ModelStop(&s.model, SIGNAL, (unsigned)interrupted);
  if (close(listener) < 0)
    s.failed = true;
  unsigned status = s.model.stop == SIGNAL              ? 128 + s.model.signo
                    : s.failed || s.model.stop == ERROR ? 1
                    : s.model.stop == UNREACHABLE       ? 3
                                                        : 0;
  char stop[64];
  int n = snprintf(stop, sizeof(stop), "STOP %u %u\n", (unsigned)s.model.stop,
                   status);
  for (unsigned i = 0; i < MAX_PEERS; ++i) {
    Peer *p = &peers[i];
    if (p->fd >= 0 && !p->closing &&
        (n < 0 || (size_t)n >= sizeof(stop) || !NetQueue(p, stop)))
      NetClose(p);
    p->closing = true;
  }
  uint64_t deadline = NetNow() + 2000;
  while (NetNow() < deadline) {
    bool pending = false, ready;
    for (unsigned i = 0; i < MAX_PEERS; ++i)
      pending |= peers[i].fd >= 0 && peers[i].size > peers[i].sent;
    if (!pending)
      break;
    if (NetWait(peers, MAX_PEERS, -1, &ready, 100) < 0 && errno != EINTR)
      break;
    for (unsigned i = 0; i < MAX_PEERS; ++i)
      if (peers[i].writable && !NetFlush(&peers[i]))
        NetClose(&peers[i]);
  }
  for (unsigned i = 0; i < MAX_PEERS; ++i)
    NetClose(&peers[i]);
  if (fcntl(STDOUT_FILENO, F_SETFL, flags) < 0)
    s.failed = true;
  return s.failed ? 1 : (int)status;
}
