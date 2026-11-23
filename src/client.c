#include "net.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct Generator {
  uint64_t values[5], tick, last;
  bool pending, emitted;
} Generator;
static unsigned Random(const Generator *g, unsigned draw) {
  uint64_t x = g->values[1] + UINT64_C(0x9e3779b97f4a7c15) *
                                  (g->values[0] * UINT64_C(0x100000000) +
                                   2 * g->tick + draw + 1);
  x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
  x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
  return (unsigned)((x ^ (x >> 31)) % 1000);
}
static bool Line(Peer *p, const char *line, void *context) {
  Generator *g = context;
  if (!strcmp(line, "PONG"))
    return p->role != 0;
  if (p->role) {
    uint64_t v[3];
    if (NetNumbers(line, "TICK ", v, 1)) {
      if (g->emitted && v[0] <= g->last)
        return false;
      g->tick = v[0];
      g->pending = true;
      return true;
    }
    if (strncmp(line, "EVENT ", 6) &&
        (!NetNumbers(line, "ACK ", v, 3) || v[2] > 1))
      return false;
  } else {
    const uint64_t limits[] = {MAX_STREAMS - 1, UINT32_MAX, 1000, 1, 1000};
    if (!NetNumbers(line, "WELCOME ", g->values, 5))
      return false;
    for (unsigned i = 0; i < 5; ++i)
      if (g->values[i] > limits[i])
        return false;
    p->role = 1;
  }
  char output[LINE_SIZE + 1];
  int n = snprintf(output, sizeof(output), "%s\n", line);
  return n > 0 && (size_t)n < sizeof(output) &&
         WriteText(STDOUT_FILENO, output);
}
int main(int argc, char **argv) {
  if (argc != 4 || !NetAddress(argv[1], argv[2]) ||
      strlen(argv[3]) >= NAME_SIZE || !*argv[3]) {
    fprintf(stderr, "Использование: crossroad_client SERVER_IP PORT STREAM\n");
    return 2;
  }
  for (const char *s = argv[3]; *s; ++s)
    if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') ||
          (*s >= '0' && *s <= '9') || *s == '_' || *s == '-'))
      return 2;
  if (!NetSignals()) {
    perror("sigaction");
    return 1;
  }
  int flags = fcntl(STDOUT_FILENO, F_GETFL);
  if (flags < 0 || fcntl(STDOUT_FILENO, F_SETFL, flags | O_NONBLOCK) < 0) {
    perror("stdout");
    return 1;
  }
  Generator generator = {0};
  Peer p = {.fd = -1};
  uint64_t deadline = NetNow() + 10000, ping = 0, retryAt = 0;
  bool pending = false;
  while (!interrupted && NetNow() < deadline) {
    if (p.fd < 0 && NetNow() >= retryAt) {
      retryAt = NetNow() + 100;
      p.fd = NetConnect(argv[1], argv[2], &pending);
      if (p.fd >= 0) {
        char hello[64];
        int n = snprintf(hello, sizeof(hello), "HELLO 1 FLOW %s\n", argv[3]);
        if (n < 0 || (size_t)n >= sizeof(hello) || !NetQueue(&p, hello))
          break;
      }
    }
    bool ready;
    int result = NetWait(&p, 1, -1, &ready, 100);
    if (result < 0) {
      if (errno == EINTR)
        continue;
      break;
    }
    if (pending && (p.readable || p.writable)) {
      int error = 0;
      socklen_t length = sizeof(error);
      if (getsockopt(p.fd, SOL_SOCKET, SO_ERROR, &error, &length) < 0 ||
          error) {
        NetClose(&p);
        pending = false;
        continue;
      }
      pending = false;
    }
    if (p.fd < 0 || pending)
      continue;
    if ((p.writable && !NetFlush(&p)) ||
        (p.readable && !NetRead(&p, Line, &generator)) || p.closing)
      break;
    if (generator.pending) {
      generator.pending = false;
      generator.emitted = true;
      generator.last = generator.tick;
      if (Random(&generator, 0) < generator.values[2]) {
        char request[96];
        unsigned emergency = generator.values[3] == CAR &&
                             Random(&generator, 1) < generator.values[4];
        int n = snprintf(request, sizeof(request), "ARRIVE %llu %u\n",
                         (unsigned long long)generator.tick, emergency);
        if (n < 0 || (size_t)n >= sizeof(request) || !NetQueue(&p, request))
          break;
      }
    }
    uint64_t now = NetNow();
    if (p.role) {
      deadline = p.last + 5000;
      if (now >= ping) {
        if (!NetQueue(&p, "PING\n"))
          break;
        ping = now + 1000;
      }
    }
  }
  NetClose(&p);
  if (fcntl(STDOUT_FILENO, F_SETFL, flags) < 0)
    perror("stdout flags");
  if (!interrupted)
    fprintf(stderr, "Соединение завершено или истёк срок ожидания\n");
  return interrupted ? 128 + interrupted : 1;
}
