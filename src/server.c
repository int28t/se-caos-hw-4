#include "net.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static Peer peers[MAX_PEERS];
static bool Reject(Peer *p, const char *reason) {
  p->closing = true;
  return NetQueue(p, reason);
}
static bool Line(Peer *p, const char *line, void *context) {
  const Config *c = context;
  if (p->role)
    return !strcmp(line, "PING") ? NetQueue(p, "PONG\n")
                                 : Reject(p, "ERROR command\n");
  if (!strcmp(line, "HELLO 1 WATCH")) {
    p->role = 2;
    return NetQueue(p, "WELCOME WATCH\n");
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
int main(int argc, char **argv) {
  if (argc != 4 || !NetAddress(argv[1], argv[2])) {
    fprintf(stderr, "Использование: crossroad_server BIND_IP PORT CONFIG\n");
    return 2;
  }
  Config c;
  if (!ConfigLoad(&c, argv[3]))
    return 2;
  if (!NetSignals()) {
    perror("sigaction");
    return 1;
  }
  int listener = NetListen(argv[1], argv[2]);
  if (listener < 0) {
    perror("listen");
    return 1;
  }
  for (unsigned i = 0; i < MAX_PEERS; ++i)
    peers[i].fd = -1;
  int status = 0;
  while (!interrupted) {
    bool ready;
    int result = NetWait(peers, MAX_PEERS, listener, &ready);
    if (result < 0) {
      if (errno == EINTR)
        continue;
      perror("select");
      status = 1;
      break;
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
      } else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
        perror("accept");
        status = 1;
        break;
      }
    }
    for (unsigned i = 0; i < MAX_PEERS; ++i) {
      Peer *p = &peers[i];
      if (p->fd < 0)
        continue;
      if ((p->readable && !NetRead(p, Line, &c)) ||
          (p->writable && !NetFlush(p)) || NetNow() - p->last >= 5000 ||
          (p->closing && p->size == p->sent))
        NetClose(p);
    }
  }
  for (unsigned i = 0; i < MAX_PEERS; ++i)
    NetClose(&peers[i]);
  if (close(listener) < 0)
    status = 1;
  return interrupted ? 128 + interrupted : status;
}
