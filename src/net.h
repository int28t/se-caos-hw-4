#ifndef NET_H
#define NET_H
#include "crossroad.h"
#include <signal.h>
#define MAX_PEERS 64
#define LINE_SIZE 1024
#define SEND_SIZE 32768

typedef struct Peer {
  int fd, role, stream;
  char input[LINE_SIZE], output[SEND_SIZE];
  size_t used, sent, size;
  uint64_t last;
  bool readable, writable, closing;
} Peer;
typedef bool (*LineCallback)(Peer *peer, const char *line, void *context);
extern volatile sig_atomic_t interrupted;
bool NetSignals(void);
uint64_t NetNow(void);
bool NetNumbers(const char *line, const char *prefix, uint64_t *values,
                size_t count);
bool NetAddress(const char *ip, const char *port);
bool NetPrepare(int fd);
int NetListen(const char *ip, const char *port);
// -1: ошибка, иначе fd; pending требует проверки SO_ERROR после writable.
int NetConnect(const char *ip, const char *port, bool *pending);
void NetClose(Peer *p);
bool NetQueue(Peer *p, const char *text);
// EOF полной строки: closing=true, очередь отправки сохраняется.
// EOF посреди строки или ошибка: false.
bool NetRead(Peer *p, LineCallback callback, void *context);
bool NetFlush(Peer *p);
int NetWait(Peer *peers, size_t count, int listener, bool *acceptReady,
            unsigned timeoutMs);
#endif
