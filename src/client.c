#include "net.h"
#include <stdio.h>
#include <string.h>
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
  if (NetStop(p, line)) {
  } else if (p->role) {
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
static bool Generate(Peer *p, void *context) {
  Generator *g = context;
  if (!g->pending)
    return true;
  g->pending = false;
  g->emitted = true;
  g->last = g->tick;
  if (Random(g, 0) >= g->values[2])
    return true;
  char request[96];
  unsigned emergency = g->values[3] == CAR && Random(g, 1) < g->values[4];
  int n = snprintf(request, sizeof(request), "ARRIVE %llu %u\n",
                   (unsigned long long)g->tick, emergency);
  return n > 0 && (size_t)n < sizeof(request) && NetQueue(p, request);
}
int main(int argc, char **argv) {
  if (argc != 4 || !NetAddress(argv[1], argv[2]) || !ValidName(argv[3])) {
    fprintf(stderr, "Использование: crossroad_client SERVER_IP PORT STREAM\n");
    return 2;
  }
  Generator generator = {0};
  char hello[64];
  int n = snprintf(hello, sizeof(hello), "HELLO 1 FLOW %s\n", argv[3]);
  if (n < 0 || (size_t)n >= sizeof(hello))
    return 2;
  return NetClient(argv[1], argv[2], hello, Line, Generate, &generator);
}
