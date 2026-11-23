#include "net.h"
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>

static bool Line(Peer *p, const char *line, void *context) {
  (void)context;
  if (!strcmp(line, "PONG"))
    return p->role != 0;
  if (p->role || strncmp(line, "WELCOME ", 8))
    return false;
  char fields[LINE_SIZE];
  strcpy(fields, line + 8);
  const uint64_t limits[] = {MAX_STREAMS - 1, UINT32_MAX, 1000, 1, 1000};
  char *save, *word = strtok_r(fields, " ", &save);
  for (unsigned i = 0; i < 5; ++i) {
    uint64_t value;
    if (!word || !Number(word, limits[i], &value))
      return false;
    word = strtok_r(NULL, " ", &save);
  }
  if (word)
    return false;
  p->role = 1;
  return printf("%s\n", line) >= 0 && fflush(stdout) == 0;
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
    int result = NetWait(&p, 1, -1, &ready);
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
        (p.readable && !NetRead(&p, Line, NULL)))
      break;
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
  if (!interrupted)
    fprintf(stderr, "Соединение завершено или истёк срок ожидания\n");
  return interrupted ? 128 + interrupted : 1;
}
