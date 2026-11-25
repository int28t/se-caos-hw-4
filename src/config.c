#include "crossroad.h"
#include <stdio.h>
#include <string.h>

static int Index(const char *s, const char *const *names, unsigned count) {
  for (unsigned i = 0; i < count; ++i)
    if (!strcmp(s, names[i]))
      return (int)i;
  return -1;
}
static int StreamIndex(const Config *c, const char *s) {
  for (unsigned i = 0; i < c->streamCount; ++i)
    if (!strcmp(s, c->streams[i].name))
      return (int)i;
  return -1;
}
static bool Line(Config *c, char **t, unsigned count) {
  uint64_t a, b, n;
  if (!strcmp(t[0], "stream") && count == 8) {
    const char *const kinds[] = {"car", "pedestrian"};
    const char *const directions[] = {"north", "east", "south", "west"};
    const char *const maneuvers[] = {"straight", "right", "left", "walk"};
    int kind = Index(t[2], kinds, 2), dir = Index(t[3], directions, 4);
    int turn = Index(t[4], maneuvers, 4);
    if (c->streamCount == MAX_STREAMS || !ValidName(t[1]) ||
        StreamIndex(c, t[1]) >= 0 || kind < 0 || dir < 0 || turn < 0 ||
        (kind == PEDESTRIAN) != (turn == WALK) ||
        !Number(t[5], MAX_QUEUE, &a) || !a || !Number(t[6], 1000, &b) || !b ||
        !Number(t[7], 1000, &n))
      return false;
    unsigned i = c->streamCount++;
    c->streams[i] = (Stream){.kind = (Kind)kind,
                             .direction = (Direction)dir,
                             .maneuver = (Maneuver)turn,
                             .capacity = (unsigned)a,
                             .crossingTicks = (unsigned)b,
                             .rate = (unsigned)n};
    strcpy(c->streams[i].name, t[1]);
    c->conflicts[i] = UINT32_MAX;
    return true;
  }
  if (!strcmp(t[0], "phase") && count >= 3) {
    if (c->phaseCount == MAX_PHASES || !ValidName(t[1]))
      return false;
    for (unsigned i = 0; i < c->phaseCount; ++i)
      if (!strcmp(c->phases[i].name, t[1]))
        return false;
    Phase p = {0};
    strcpy(p.name, t[1]);
    for (unsigned i = 2; i < count; ++i) {
      int j = StreamIndex(c, t[i]);
      if (j < 0 || (p.mask & (1u << (unsigned)j)))
        return false;
      p.mask |= 1u << (unsigned)j;
    }
    c->phases[c->phaseCount++] = p;
    return true;
  }
  if (!strcmp(t[0], "conflict") && count == 4) {
    int i = StreamIndex(c, t[1]), j = StreamIndex(c, t[2]);
    if (i < 0 || j < 0 || !Number(t[3], 1, &n) || (i == j && !n))
      return false;
    if (n) {
      c->conflicts[i] |= 1u << (unsigned)j;
      c->conflicts[j] |= 1u << (unsigned)i;
    } else {
      c->conflicts[i] &= ~(1u << (unsigned)j);
      c->conflicts[j] &= ~(1u << (unsigned)i);
    }
    return true;
  }
  if (count != 2)
    return false;
  if (!strcmp(t[0], "strategy")) {
    if (strcmp(t[1], "fixed") && strcmp(t[1], "adaptive"))
      return false;
    c->strategy = !strcmp(t[1], "fixed") ? FIXED : ADAPTIVE;
    return true;
  }
  if (!Number(t[1], !strcmp(t[0], "seed") ? UINT32_MAX : 1000000000, &n))
    return false;
  if (!strcmp(t[0], "ticks") || !strcmp(t[0], "target")) {
    *(!strcmp(t[0], "ticks") ? &c->ticks : &c->target) = n;
    return true;
  }
  struct {
    const char *name;
    unsigned *value, min, max;
  } fields[] = {{"seed", &c->seed, 0, UINT32_MAX},
                {"min_green", &c->minGreen, 1, 10000},
                {"max_green", &c->maxGreen, 1, 10000},
                {"clearance", &c->clearance, 1, 10000},
                {"emergency_rate", &c->emergencyRate, 0, 1000},
                {"tick_ms", &c->tickMs, 1, 60000},
                {"delay_ms", &c->delayMs, 0, 60000}};
  if (!strcmp(t[0], "priority") && n <= 1) {
    c->priority = n != 0;
    return true;
  }
  for (unsigned i = 0; i < sizeof(fields) / sizeof(*fields); ++i)
    if (!strcmp(t[0], fields[i].name) && n >= fields[i].min &&
        n <= fields[i].max) {
      *fields[i].value = (unsigned)n;
      return true;
    }
  return false;
}
bool ConfigLoad(Config *c, const char *path) {
  char text[TEXT_SIZE + 1];
  if (!ReadText(path, text, sizeof(text)))
    return false;
  *c = (Config){.ticks = 120,
                .seed = 39,
                .minGreen = 4,
                .maxGreen = 10,
                .clearance = 2,
                .emergencyRate = 20,
                .tickMs = 1,
                .priority = true,
                .strategy = ADAPTIVE};
  char *save, *line = strtok_r(text, "\n", &save);
  while (line) {
    char *comment = strchr(line, '#'), *tokens[MAX_STREAMS + 2], *word;
    if (comment)
      *comment = 0;
    unsigned count = 0;
    for (char *t = strtok_r(line, " \r\t", &word); t;
         t = strtok_r(NULL, " \r\t", &word)) {
      if (count == MAX_STREAMS + 2)
        goto fail;
      tokens[count++] = t;
    }
    if (count && !Line(c, tokens, count))
      goto fail;
    line = strtok_r(NULL, "\n", &save);
  }
  if (!c->streamCount || !c->phaseCount || c->minGreen > c->maxGreen ||
      c->ticks > UINT64_MAX / ((uint64_t)c->tickMs * NS))
    goto fail;
  uint32_t covered = 0;
  for (unsigned p = 0; p < c->phaseCount; ++p) {
    uint32_t mask = c->phases[p].mask;
    covered |= mask;
    for (unsigned i = 0; i < c->streamCount; ++i)
      if ((mask & (1u << i)) && (c->conflicts[i] & mask & ~(1u << i)))
        goto fail;
  }
  for (unsigned i = 0; i < c->streamCount; ++i)
    if (!(covered & (1u << i)) || c->streams[i].crossingTicks > c->maxGreen)
      goto fail;
  return true;
fail:
  fprintf(stderr, "Некорректный вход: %s\n", path);
  return false;
}
