#ifndef CROSSROAD_H
#define CROSSROAD_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define MAX_STREAMS 16
#define MAX_PHASES 16
#define MAX_QUEUE 64
#define NAME_SIZE 32
#define TEXT_SIZE 16384
#define NS UINT64_C(1000000)

typedef enum Kind { CAR, PEDESTRIAN } Kind;
typedef enum Direction { NORTH, EAST, SOUTH, WEST } Direction;
typedef enum Maneuver { STRAIGHT, RIGHT, LEFT, WALK } Maneuver;
typedef enum Strategy { FIXED, ADAPTIVE } Strategy;
typedef struct Stream {
  char name[NAME_SIZE];
  Kind kind;
  Direction direction;
  Maneuver maneuver;
  unsigned capacity, crossingTicks, rate;
} Stream;
typedef struct Phase {
  char name[NAME_SIZE];
  uint32_t mask;
} Phase;
typedef struct Config {
  Stream streams[MAX_STREAMS];
  Phase phases[MAX_PHASES];
  uint32_t conflicts[MAX_STREAMS];
  unsigned streamCount, phaseCount, minGreen, maxGreen, clearance;
  unsigned emergencyRate, tickMs, delayMs, seed;
  uint64_t ticks, target;
  Strategy strategy;
  bool priority;
} Config;
typedef struct Person {
  uint64_t id, arrived;
  bool emergency;
} Person;
typedef struct Flow {
  Person queue[MAX_QUEUE], active;
  unsigned head, count, peak;
  uint64_t entered, leaveAt, arrived, passed, refused, waitSum, waitMax;
} Flow;
typedef enum State { INITIAL, GREEN, DRAIN, CLEARANCE } State;
typedef enum Stop { RUNNING, PERIOD, TARGET, SIGNAL, ERROR, UNREACHABLE } Stop;
// Callback вызывается синхронно; строка живёт только до его возврата.
// stream == -1 обозначает общее событие. Модель из callback не изменяется.
typedef void (*EventCallback)(void *context, int stream, uint64_t sequence,
                              uint64_t now, const char *text);
typedef struct Model {
  Config cfg;
  Flow flows[MAX_STREAMS];
  EventCallback event;
  void *context;
  uint64_t now, tick, nextId, sequence, since, display, passed;
  unsigned phase, next, signals, active, signo;
  State state;
  Stop stop;
} Model;
typedef struct FlowView {
  uint64_t headId, activeId, leaveAt, arrived, passed, refused, waitSum,
      waitMax;
  unsigned queued, peak;
} FlowView;
typedef struct Snapshot {
  uint64_t sequence, now, passed;
  unsigned phase, signals, active, count;
  State state;
  Stop stop;
  FlowView flows[MAX_STREAMS];
} Snapshot;

bool Number(const char *text, uint64_t limit, uint64_t *value);
bool ReadText(const char *path, char *text, size_t capacity);
bool ConfigLoad(Config *cfg, const char *path);
// cfg должна пройти ConfigLoad; временем владеет вызывающий сервер.
void ModelInit(Model *m, const Config *cfg, EventCallback event, void *context);
bool ModelArrive(Model *m, unsigned stream, bool emergency, uint64_t now);
void ModelAdvance(Model *m, uint64_t now);
void ModelStop(Model *m, Stop reason, unsigned signo);
void ModelSnapshot(const Model *m, Snapshot *snapshot);
#endif
