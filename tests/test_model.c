#include "crossroad.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); exit(1); } } while (0)
static unsigned events, pedestrians, refusals;
static uint64_t sequence, eventTime;
static void Event(void *context, int stream, uint64_t seq, uint64_t now,
                  const char *text) {
  CHECK(context == &events && stream >= -1 && stream < MAX_STREAMS);
  CHECK(seq == sequence + 1 && now >= eventTime && *text);
  sequence = seq;
  eventTime = now;
  ++events;
  pedestrians += strstr(text, "Запрос пешехода") != NULL;
  refusals += strstr(text, "Отказ:") != NULL;
}
static void Init(Model *m, Config *c) {
  events = pedestrians = refusals = 0;
  sequence = eventTime = 0;
  ModelInit(m, c, Event, &events);
}
static void Input(const char *text, size_t n, bool valid) {
  int fd = open("input.conf", O_WRONLY | O_CREAT | O_TRUNC, 0600);
  CHECK(fd >= 0 && write(fd, text, n) == (ssize_t)n && close(fd) == 0);
  Config c;
  CHECK(ConfigLoad(&c, "input.conf") == valid);
}
static void ConfigTests(const char *dir, Config *base) {
  Config c;
  char path[1024];
  CHECK(snprintf(path, sizeof(path), "%s/minimal.conf", dir) > 0);
  CHECK(ConfigLoad(base, path) && base->streamCount == 2 &&
        base->phaseCount == 2);
  const char *bad[] = {"ticks -1",
                       "seed 4294967296",
                       "ticks 1x",
                       "ticks 18446744073709551616",
                       "ticks 307445735\ntick_ms 60000",
                       "min_green 9\nmax_green 2",
                       "phase bad A P",
                       "conflict A A 0",
                       "stream A car north straight 0 1 3",
                       "unknown 1"};
  char text[2048];
  const char *tail = "\nstream A car north straight 2 1 1000\nstream P "
                     "pedestrian east walk 2 1 1000\nphase a A\nphase p P\n";
  for (unsigned i = 0; i < sizeof(bad) / sizeof(*bad); ++i) {
    int n = snprintf(text, sizeof(text), "%s%s", tail, bad[i]);
    CHECK(n > 0);
    Input(text, (size_t)n, false);
  }
  Input("ticks 1\0junk", 12, false);
  Input(tail, strlen(tail), true);
  CHECK(mkfifo("input.fifo", 0600) == 0);
  CHECK(!ConfigLoad(base, "input.fifo") && !ConfigLoad(base, "."));
  CHECK(unlink("input.fifo") == 0 && unlink("input.conf") == 0);
  CHECK(ConfigLoad(base, path));
  uint64_t number;
  CHECK(Number("18446744073709551615", UINT64_MAX, &number) &&
        number == UINT64_MAX);
  CHECK(!Number("", 9, &number) && !Number("+1", 9, &number) &&
        !Number("10", 9, &number));
  CHECK(snprintf(path, sizeof(path), "%s/demo.conf", dir) > 0);
  CHECK(ConfigLoad(&c, path) && c.streamCount == 5);
  CHECK(snprintf(path, sizeof(path), "%s/compatible.conf", dir) > 0);
  CHECK(ConfigLoad(&c, path) && c.phases[0].mask == 3);
  CHECK(snprintf(path, sizeof(path), "%s/emergency.conf", dir) > 0);
  CHECK(ConfigLoad(&c, path) && c.priority);
  char limit[TEXT_SIZE + 1];
  memset(limit, '#', sizeof(limit));
  memcpy(limit, tail, strlen(tail));
  Input(limit, TEXT_SIZE, true);
  Input(limit, sizeof(limit), false);
  const char *last = "stream A car north straight 1 1 0\nphase a A";
  Input(last, strlen(last), true);
  CHECK(unlink("input.conf") == 0);
}
static void ModelTests(Config c) {
  c.ticks = c.target = 0;
  c.minGreen = 2;
  c.maxGreen = 4;
  c.clearance = 1;
  c.tickMs = 1;
  c.streams[0].crossingTicks = 4;
  c.streams[0].capacity = 2;
  Model m;
  Init(&m, &c);
  ModelAdvance(&m, 0);
  CHECK(ModelArrive(&m, 0, false, NS));
  ModelAdvance(&m, NS);
  CHECK(m.flows[0].active.id == 1 && m.active == 1);
  CHECK(ModelArrive(&m, 0, true, NS) && ModelArrive(&m, 0, false, NS));
  CHECK(!ModelArrive(&m, 0, false, NS) && refusals == 1);
  CHECK(ModelArrive(&m, 1, false, NS) && pedestrians == 1);
  ModelAdvance(&m, 2 * NS);
  CHECK(m.state == GREEN && m.active == 1);
  ModelAdvance(&m, 4 * NS);
  CHECK(m.state == DRAIN && m.active == 1);
  ModelAdvance(&m, 5 * NS - 1);
  CHECK(m.passed == 0 && m.state == DRAIN);
  ModelAdvance(&m, 5 * NS);
  CHECK(m.passed == 1 && m.state == CLEARANCE);
  ModelAdvance(&m, 6 * NS - 1);
  CHECK(m.state == CLEARANCE && !m.active);
  ModelAdvance(&m, 6 * NS);
  CHECK(m.state == GREEN && m.phase == 0 && m.flows[0].active.emergency);
  Snapshot s;
  ModelSnapshot(&m, &s);
  CHECK(s.sequence == m.sequence && s.flows[0].queued == 1 &&
        s.flows[0].activeId == m.flows[0].active.id);
  ModelStop(&m, SIGNAL, 2);
  uint64_t seq = m.sequence;
  ModelAdvance(&m, 100 * NS);
  ModelStop(&m, ERROR, 0);
  CHECK(!ModelArrive(&m, 0, false, 100 * NS) && m.sequence == seq &&
        m.stop == SIGNAL);
  CHECK(m.passed == 1 && m.active == 1 && m.flows[0].arrived == 4);
  for (unsigned strategy = 0; strategy < 2; ++strategy) {
    c.strategy = (Strategy)strategy;
    c.priority = false;
    Init(&m, &c);
    ModelAdvance(&m, 0);
    CHECK(m.phase == 0);
    ModelAdvance(&m, 4 * NS);
    CHECK(m.state == CLEARANCE);
    ModelAdvance(&m, 5 * NS);
    CHECK(m.phase == 1);
  }
  c.phases[0].mask = 3;
  c.phaseCount = 1;
  c.conflicts[0] = 1;
  c.conflicts[1] = 2;
  c.target = 1;
  Init(&m, &c);
  CHECK(ModelArrive(&m, 0, false, 0) && ModelArrive(&m, 1, false, 0));
  ModelAdvance(&m, 0);
  CHECK(m.active == 1);
  ModelAdvance(&m, 4 * NS);
  CHECK(m.stop == TARGET && m.passed == 1 && !m.active);
  c.target = 0;
  Init(&m, &c);
  CHECK(ModelArrive(&m, 0, false, 0) && ModelArrive(&m, 1, false, 0));
  ModelAdvance(&m, 0);
  CHECK(m.active == 3);
  c.ticks = 1;
  Init(&m, &c);
  CHECK(ModelArrive(&m, 0, false, 0));
  ModelAdvance(&m, 0);
  ModelAdvance(&m, 2 * NS);
  CHECK(m.stop == PERIOD && m.now == NS && m.passed == 0 && m.active == 1);
  c.ticks = 0;
  c.target = 1;
  c.streams[0].rate = c.streams[1].rate = 0;
  Init(&m, &c);
  ModelAdvance(&m, 0);
  CHECK(m.stop == UNREACHABLE);
  c.target = 0;
  Init(&m, &c);
  ModelAdvance(&m, 100 * NS);
  CHECK(m.stop == RUNNING);
  CHECK(!ModelArrive(&m, MAX_STREAMS, false, 100 * NS));
  CHECK(!ModelArrive(&m, 1, true, 100 * NS));
  Init(&m, &c);
  ModelAdvance(&m, NS);
  ModelAdvance(&m, 0);
  CHECK(m.stop == ERROR);
}
static void SelectionTests(Config c) {
  c.ticks = c.target = 0;
  c.minGreen = 2;
  c.maxGreen = 4;
  c.clearance = 1;
  c.tickMs = 1;
  Model m;
  for (unsigned strategy = 0; strategy < 2; ++strategy) {
    c.strategy = (Strategy)strategy;
    c.priority = false;
    Init(&m, &c);
    CHECK(ModelArrive(&m, 1, false, 0));
    ModelAdvance(&m, 0);
    CHECK(m.phase == (strategy == ADAPTIVE ? 1u : 0u));
  }
  c.strategy = FIXED;
  c.priority = true;
  Init(&m, &c);
  ModelAdvance(&m, 0);
  CHECK(ModelArrive(&m, 1, false, 0));
  CHECK(ModelArrive(&m, 0, true, NS));
  // Приоритет своего направления не вызывает преждевременный переход.
  ModelAdvance(&m, 2 * NS - 1);
  CHECK(m.state == GREEN && m.phase == 0);
  c.streams[1].kind = CAR;
  Init(&m, &c);
  ModelAdvance(&m, 0);
  CHECK(ModelArrive(&m, 1, false, 0) && ModelArrive(&m, 1, true, NS));
  ModelAdvance(&m, 2 * NS - 1);
  CHECK(m.state == GREEN && !m.active);
  ModelAdvance(&m, 2 * NS);
  CHECK(m.state == CLEARANCE);
  ModelAdvance(&m, 3 * NS);
  CHECK(m.phase == 1 && m.flows[1].active.id == 1);
  CHECK(!m.flows[1].active.emergency); // Экстренный не обгоняет голову FIFO.
  c.strategy = ADAPTIVE;
  c.priority = false;
  Init(&m, &c);
  CHECK(ModelArrive(&m, 0, false, 0));
  CHECK(ModelArrive(&m, 1, false, NS));
  ModelAdvance(&m, NS);
  CHECK(m.phase == 0);
  Init(&m, &c);
  CHECK(ModelArrive(&m, 0, false, 0));
  CHECK(ModelArrive(&m, 1, false, 0) && ModelArrive(&m, 1, false, 0));
  ModelAdvance(&m, 0);
  CHECK(m.phase == 1); // При равном возрасте выбирается длина.
  c.phaseCount = 1;
  c.phases[0].mask = 1;
  c.streamCount = 1;
  c.target = 80;
  Init(&m, &c);
  for (uint64_t t = 0; !m.stop && t < 1000; ++t) {
    (void)ModelArrive(&m, 0, false, t * NS);
    ModelAdvance(&m, t * NS);
  }
  CHECK(m.stop == TARGET && m.passed == 80 &&
        m.flows[0].head == 80 % MAX_QUEUE);
  CHECK(m.flows[0].arrived == m.passed + m.flows[0].refused + m.flows[0].count);
}
int main(int argc, char **argv) {
  CHECK(argc == 2);
  char current[4096], data[8192], temporary[64];
  CHECK(getcwd(current, sizeof(current)) != NULL);
  CHECK(snprintf(data, sizeof(data), "%s%s%s", argv[1][0] == '/' ? "" : current,
                 argv[1][0] == '/' ? "" : "/", argv[1]) > 0);
  CHECK(snprintf(temporary, sizeof(temporary), "model-test-%ld",
                 (long)getpid()) > 0);
  CHECK(mkdir(temporary, 0700) == 0 && chdir(temporary) == 0);
  Config c;
  ConfigTests(data, &c);
  ModelTests(c);
  SelectionTests(c);
  CHECK(chdir(current) == 0 && rmdir(temporary) == 0);
  puts("model: config, queues, phases, timing and shutdown passed");
  return 0;
}
