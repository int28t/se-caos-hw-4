#include "net.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%d: %s\n", __LINE__, #x); exit(1); } } while (0)
// clang-format off
static bool LongLine(Peer *p, const char *line, void *ctx) {
  (void)p; (void)ctx; CHECK(strlen(line) == LINE_SIZE - 1); return true;
}
// clang-format on
static unsigned lines;
static bool Line(Peer *p, const char *line, void *ctx) {
  (void)p;
  (void)ctx;
  CHECK(!strcmp(line, "PING"));
  ++lines;
  return true;
}
int main(void) {
  CHECK(NetSignals());
  CHECK(!NetPrepare(FD_SETSIZE) && errno == ERANGE);
  Peer invalid = {.fd = FD_SETSIZE};
  bool ready;
  CHECK(NetWait(&invalid, 1, -1, &ready) == -1 && errno == ERANGE);
  int fds[2];
  CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
  CHECK(NetPrepare(fds[0]) && NetPrepare(fds[1]));
  Peer p = {.fd = fds[0]};
  CHECK(write(fds[1], "PI", 2) == 2 && NetRead(&p, Line, NULL) && !lines);
  CHECK(write(fds[1], "NG\nPING\n", 8) == 8 && NetRead(&p, Line, NULL) &&
        lines == 2);
  char large[SEND_SIZE + 1];
  memset(large, 'x', SEND_SIZE);
  large[SEND_SIZE] = 0;
  CHECK(NetQueue(&p, large) && !NetQueue(&p, "x"));
  int small = 1024;
  CHECK(setsockopt(p.fd, SOL_SOCKET, SO_SNDBUF, &small, sizeof(small)) == 0);
  CHECK(NetFlush(&p));
  CHECK(p.sent > 0 && p.sent < p.size);
  size_t received = 0;
  for (unsigned i = 0; i < 1000 && (p.size || received < SEND_SIZE); ++i) {
    CHECK(NetFlush(&p));
    ssize_t n = read(fds[1], large, sizeof(large));
    if (n > 0) {
      for (ssize_t j = 0; j < n; ++j)
        CHECK(large[j] == 'x');
      received += (size_t)n;
    } else
      CHECK(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));
  }
  CHECK(received == SEND_SIZE && !p.size && NetQueue(&p, "PING\n"));
  memset(large, 'x', LINE_SIZE);
  large[LINE_SIZE - 1] = '\n';
  CHECK(write(fds[1], large, LINE_SIZE) == LINE_SIZE &&
        NetRead(&p, LongLine, NULL));
  CHECK(shutdown(fds[1], SHUT_WR) == 0 && !NetRead(&p, Line, NULL));
  NetClose(&p);
  CHECK(close(fds[1]) == 0);
  puts("protocol buffers: framing, partial send, overflow, EOF and fd guard "
       "passed");
  return 0;
}
