#include "net.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

volatile sig_atomic_t interrupted;
static void Signal(int number) { interrupted = number; }
bool NetSignals(void) {
  struct sigaction a = {0};
  a.sa_handler = Signal;
  if (sigemptyset(&a.sa_mask) < 0 || sigaction(SIGINT, &a, NULL) < 0 ||
      sigaction(SIGTERM, &a, NULL) < 0)
    return false;
  a.sa_handler = SIG_IGN;
  return sigaction(SIGPIPE, &a, NULL) == 0;
}
uint64_t NetNow(void) {
  struct timespec t;
  if (clock_gettime(CLOCK_MONOTONIC, &t) < 0) {
    perror("clock_gettime");
    _exit(1);
  }
  return (uint64_t)t.tv_sec * 1000 + (uint64_t)t.tv_nsec / 1000000;
}
static bool Address(struct sockaddr_in *a, const char *ip, const char *port) {
  uint64_t n;
  *a = (struct sockaddr_in){.sin_family = AF_INET};
  if (!Number(port, 65535, &n) || !n ||
      inet_pton(AF_INET, ip, &a->sin_addr) != 1) {
    errno = EINVAL;
    return false;
  }
  a->sin_port = htons((uint16_t)n);
  return true;
}
bool NetAddress(const char *ip, const char *port) {
  struct sockaddr_in a;
  return Address(&a, ip, port);
}
bool NetPrepare(int fd) {
  if (fd < 0 || fd >= FD_SETSIZE) {
    errno = ERANGE;
    return false;
  }
  int flags = fcntl(fd, F_GETFL);
  return flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0 &&
         fcntl(fd, F_SETFD, FD_CLOEXEC) == 0;
}
static int Socket(struct sockaddr_in *a, const char *ip, const char *port) {
  if (!Address(a, ip, port))
    return -1;
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd >= 0 && !NetPrepare(fd)) {
    close(fd);
    return -1;
  }
  return fd;
}
int NetListen(const char *ip, const char *port) {
  struct sockaddr_in a;
  int fd = Socket(&a, ip, port), yes = 1;
  if (fd < 0)
    return -1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == 0 &&
      bind(fd, (struct sockaddr *)&a, sizeof(a)) == 0 &&
      listen(fd, MAX_PEERS) == 0)
    return fd;
  close(fd);
  return -1;
}
int NetConnect(const char *ip, const char *port, bool *pending) {
  struct sockaddr_in a;
  int fd = Socket(&a, ip, port);
  *pending = false;
  if (fd < 0)
    return -1;
  if (connect(fd, (struct sockaddr *)&a, sizeof(a)) == 0)
    return fd;
  if (errno == EINPROGRESS) {
    *pending = true;
    return fd;
  }
  close(fd);
  return -1;
}
void NetClose(Peer *p) {
  if (p->fd >= 0)
    (void)close(p->fd);
  *p = (Peer){.fd = -1, .stream = -1};
}
bool NetQueue(Peer *p, const char *text) {
  size_t n = strlen(text);
  if (n > SEND_SIZE - (p->size - p->sent))
    return false;
  if (p->sent) {
    memmove(p->output, p->output + p->sent, p->size - p->sent);
    p->size -= p->sent;
    p->sent = 0;
  }
  memcpy(p->output + p->size, text, n);
  p->size += n;
  return true;
}
bool NetFlush(Peer *p) {
  if (p->sent == p->size)
    return true;
  ssize_t n = send(p->fd, p->output + p->sent, p->size - p->sent, 0);
  if (n < 0)
    return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR;
  if (!n)
    return false;
  p->sent += (size_t)n;
  if (p->sent == p->size)
    p->sent = p->size = 0;
  return true;
}
bool NetRead(Peer *p, LineCallback callback, void *context) {
  // Ограничиваем работу одного peer за проход общего цикла.
  for (unsigned batch = 0; batch < 4 && !p->closing; ++batch) {
    char buffer[LINE_SIZE];
    ssize_t n = recv(p->fd, buffer, sizeof(buffer), 0);
    if (n < 0)
      return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR;
    if (!n) {
      // FIN закрывает чтение, но не отменяет уже поставленные ответы.
      p->closing = true;
      return p->used == 0;
    }
    for (size_t i = 0; i < (size_t)n && !p->closing; ++i) {
      char c = buffer[i];
      if (!c || (p->used == LINE_SIZE - 1 && c != '\n'))
        return false;
      if (c == '\n') {
        p->input[p->used] = 0;
        p->last = NetNow();
        if (!callback(p, p->input, context))
          return false;
        p->used = 0;
      } else
        p->input[p->used++] = c;
    }
  }
  return true;
}
int NetWait(Peer *peers, size_t count, int listener, bool *acceptReady,
            unsigned timeoutMs) {
  fd_set readSet, writeSet;
  FD_ZERO(&readSet);
  FD_ZERO(&writeSet);
  int maximum = -1;
  *acceptReady = false;
  if (listener >= 0) {
    if (listener >= FD_SETSIZE) {
      errno = ERANGE;
      return -1;
    }
    FD_SET(listener, &readSet);
    maximum = listener;
  }
  for (size_t i = 0; i < count; ++i) {
    Peer *p = &peers[i];
    p->readable = p->writable = false;
    if (p->fd < 0)
      continue;
    if (p->fd >= FD_SETSIZE) {
      errno = ERANGE;
      return -1;
    }
    if (!p->closing)
      FD_SET(p->fd, &readSet);
    if (p->size > p->sent)
      FD_SET(p->fd, &writeSet);
    if (p->fd > maximum)
      maximum = p->fd;
  }
  struct timeval timeout = {.tv_sec = (time_t)(timeoutMs / 1000),
                            .tv_usec =
                                (suseconds_t)((timeoutMs % 1000) * 1000)};
  int result = select(maximum + 1, &readSet, &writeSet, NULL, &timeout);
  if (result <= 0)
    return result;
  *acceptReady = listener >= 0 && FD_ISSET(listener, &readSet);
  for (size_t i = 0; i < count; ++i)
    if (peers[i].fd >= 0) {
      peers[i].readable = FD_ISSET(peers[i].fd, &readSet);
      peers[i].writable = FD_ISSET(peers[i].fd, &writeSet);
    }
  return result;
}

bool NetNumbers(const char *line, const char *prefix, uint64_t *values,
                size_t count) {
  size_t skip = strlen(prefix);
  if (strncmp(line, prefix, skip) || strlen(line) >= LINE_SIZE)
    return false;
  char text[LINE_SIZE];
  strcpy(text, line + skip);
  char *word = text;
  for (size_t i = 0; i < count; ++i) {
    char *space = strchr(word, ' ');
    if ((i + 1 < count) != (space != NULL))
      return false;
    if (space)
      *space = 0;
    if (!Number(word, UINT64_MAX, &values[i]))
      return false;
    if (space)
      word = space + 1;
  }
  return true;
}
