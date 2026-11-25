// Модель изменяется только вызывающим процессом, время передаётся явно.
#include "crossroad.h"
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>

static void Emit(Model *m, int stream, const char *format, ...) {
  char text[768];
  va_list args;
  va_start(args, format);
  int n = vsnprintf(text, sizeof(text), format, args);
  va_end(args);
  if (n < 0 || (size_t)n >= sizeof(text) || m->sequence == UINT64_MAX) {
    m->stop = ERROR;
    return;
  }
  ++m->sequence;
  if (m->event)
    m->event(m->context, stream, m->sequence, m->now, text);
}
void ModelInit(Model *m, const Config *c, EventCallback event, void *context) {
  *m = (Model){.cfg = *c,
               .event = event,
               .context = context,
               .tick = (uint64_t)c->tickMs * NS};
}
void ModelStop(Model *m, Stop reason, unsigned signo) {
  if (m->stop || reason == RUNNING)
    return;
  m->stop = reason;
  m->signo = signo;
  Emit(m, -1, "Завершение: причина=%u сигнал=%u", (unsigned)reason, signo);
  uint64_t arrived = 0, refused = 0, queued = 0, wait = 0, maximum = 0;
  unsigned active = 0, peak = 0;
  for (unsigned i = 0; i < m->cfg.streamCount; ++i) {
    const Flow *f = &m->flows[i];
    arrived += f->arrived;
    refused += f->refused;
    queued += f->count;
    wait += f->waitSum;
    active += f->active.id != 0;
    if (f->waitMax > maximum)
      maximum = f->waitMax;
    if (f->peak > peak)
      peak = f->peak;
    Emit(m, (int)i,
         "Итог потока: поток=%s прибыло=%" PRIu64 " прошло=%" PRIu64
         " отказов=%" PRIu64 " ожидают=%u в_зоне=%u",
         m->cfg.streams[i].name, f->arrived, f->passed, f->refused, f->count,
         (unsigned)(f->active.id != 0));
  }
  Emit(m, -1,
       "ИТОГО: прибыло=%" PRIu64 " прошло=%" PRIu64 " отказов=%" PRIu64
       " ожидают=%" PRIu64 " в_зоне=%u ожидание=%" PRIu64
       " среднее=%.2f максимум=%" PRIu64 " пик=%u",
       arrived, m->passed, refused, queued, active, wait,
       m->passed ? (double)wait / (double)m->passed : 0.0, maximum, peak);
}
// Обратное время — ошибка вызывающего кода. Период не допускает поздних
// мутаций.
static bool Time(Model *m, uint64_t now) {
  if (m->stop)
    return false;
  if (now < m->now) {
    ModelStop(m, ERROR, 0);
    return false;
  }
  uint64_t end = m->cfg.ticks * m->tick;
  m->now = end && now > end ? end : now;
  return true;
}
bool ModelArrive(Model *m, unsigned i, bool emergency, uint64_t now) {
  if (!Time(m, now))
    return false;
  if (m->cfg.ticks && m->now == m->cfg.ticks * m->tick) {
    ModelAdvance(m, m->now);
    return false;
  }
  if (i >= m->cfg.streamCount || (emergency && m->cfg.streams[i].kind != CAR))
    return false;
  if (m->nextId == UINT64_MAX) {
    ModelStop(m, ERROR, 0);
    return false;
  }
  Flow *f = &m->flows[i];
  Person p = {.id = ++m->nextId, .arrived = m->now, .emergency = emergency};
  ++f->arrived;
  Emit(m, (int)i, "Прибытие: поток=%s id=%" PRIu64 " экстренный=%u",
       m->cfg.streams[i].name, p.id, (unsigned)emergency);
  if (f->count == m->cfg.streams[i].capacity) {
    ++f->refused;
    Emit(m, (int)i, "Отказ: поток=%s id=%" PRIu64, m->cfg.streams[i].name,
         p.id);
    return false;
  }
  f->queue[(f->head + f->count) % MAX_QUEUE] = p;
  if (++f->count > f->peak)
    f->peak = f->count;
  Emit(m, (int)i, "Очередь: поток=%s id=%" PRIu64 " размер=%u",
       m->cfg.streams[i].name, p.id, f->count);
  if (m->cfg.streams[i].kind == PEDESTRIAN)
    Emit(m, (int)i, "Запрос пешехода: поток=%s id=%" PRIu64,
         m->cfg.streams[i].name, p.id);
  return true;
}
static unsigned Choose(const Model *m, bool *urgent) {
  unsigned best = m->next, bestLength = 0;
  uint64_t oldest = UINT64_MAX, emergency = UINT64_MAX;
  for (unsigned n = 0; n < m->cfg.phaseCount; ++n) {
    unsigned phase = (m->next + n) % m->cfg.phaseCount, length = 0;
    uint64_t first = UINT64_MAX, special = UINT64_MAX;
    for (unsigned i = 0; i < m->cfg.streamCount; ++i) {
      const Flow *f = &m->flows[i];
      if (!(m->cfg.phases[phase].mask & (1u << i)) || !f->count)
        continue;
      length += f->count;
      if (f->queue[f->head].arrived < first)
        first = f->queue[f->head].arrived;
      for (unsigned j = 0; m->cfg.priority && j < f->count; ++j) {
        Person p = f->queue[(f->head + j) % MAX_QUEUE];
        if (p.emergency && p.id < special)
          special = p.id;
      }
    }
    if (special < emergency ||
        (special == emergency && m->cfg.strategy == ADAPTIVE &&
         (first < oldest || (first == oldest && length > bestLength)))) {
      best = phase;
      emergency = special;
      oldest = first;
      bestLength = length;
    }
  }
  *urgent = emergency != UINT64_MAX;
  return best;
}
static void Controller(Model *m) {
  bool urgent;
  unsigned chosen = Choose(m, &urgent);
  uint64_t elapsed = m->now - m->since;
  if (m->state == GREEN &&
      (elapsed >= m->cfg.maxGreen * m->tick ||
       (elapsed >= m->cfg.minGreen * m->tick && chosen != m->phase &&
        (m->cfg.strategy == ADAPTIVE || urgent)))) {
    m->state = DRAIN;
    Emit(m, -1, "Въезд закрыт: фаза=%s", m->cfg.phases[m->phase].name);
  }
  if (m->state == DRAIN && !m->active) {
    m->state = CLEARANCE;
    m->signals = 0;
    m->since = m->now;
    Emit(m, -1, "Конец фазы: имя=%s", m->cfg.phases[m->phase].name);
    Emit(m, -1, "Интервал: длительность=%u", m->cfg.clearance);
    Emit(m, -1, "Сигналы: маска=0");
  }
  if (m->state == INITIAL ||
      (m->state == CLEARANCE &&
       m->now - m->since >= m->cfg.clearance * m->tick)) {
    m->phase = chosen;
    m->next = (chosen + 1) % m->cfg.phaseCount;
    m->state = GREEN;
    m->since = m->now;
    m->signals = m->cfg.phases[chosen].mask;
    Emit(m, -1, "Начало фазы: имя=%s", m->cfg.phases[chosen].name);
    Emit(m, -1, "Сигналы: маска=%u", m->signals);
  }
}
void ModelAdvance(Model *m, uint64_t now) {
  if (!Time(m, now))
    return;
  unsigned population = 0;
  bool possible = false;
  for (unsigned i = 0; i < m->cfg.streamCount; ++i) {
    Flow *f = &m->flows[i];
    possible |=
        m->cfg.streams[i].rate != 0 || f->count != 0 || f->active.id != 0;
    if (f->active.id && m->now >= f->leaveAt) {
      uint64_t wait = (f->entered - f->active.arrived) / m->tick;
      ++f->passed;
      ++m->passed;
      f->waitSum += wait;
      if (wait > f->waitMax)
        f->waitMax = wait;
      Emit(m, (int)i, "Выход: поток=%s id=%" PRIu64 " ожидание=%" PRIu64,
           m->cfg.streams[i].name, f->active.id, wait);
      f->active.id = 0;
      m->active &= ~(1u << i);
    }
    population += f->active.id != 0;
  }
  if (m->cfg.target && m->passed == m->cfg.target)
    ModelStop(m, TARGET, 0);
  else if (m->cfg.ticks && m->now == m->cfg.ticks * m->tick)
    ModelStop(m, PERIOD, 0);
  else if (!m->cfg.ticks && m->cfg.target && !possible)
    ModelStop(m, UNREACHABLE, 0);
  if (m->stop)
    return;
  Controller(m);
  for (unsigned i = 0; i < m->cfg.streamCount; ++i) {
    Flow *f = &m->flows[i];
    if (m->state != GREEN || !(m->signals & (1u << i)) || !f->count ||
        f->active.id || (m->cfg.conflicts[i] & m->active) ||
        (m->cfg.target && m->passed + population >= m->cfg.target))
      continue;
    uint64_t duration = m->cfg.streams[i].crossingTicks * m->tick;
    if (m->now > UINT64_MAX - duration) {
      ModelStop(m, ERROR, 0);
      return;
    }
    f->active = f->queue[f->head];
    f->head = (f->head + 1) % MAX_QUEUE;
    --f->count;
    f->entered = m->now;
    f->leaveAt = m->now + duration;
    m->active |= 1u << i;
    ++population;
    Emit(m, (int)i,
         "Разрешение: поток=%s id=%" PRIu64 " очередь=%u срок=%" PRIu64,
         m->cfg.streams[i].name, f->active.id, f->count, f->leaveAt);
  }
  if (m->now >= m->display) {
    Emit(m, -1, "Состояние: фаза=%u состояние=%u активные=%u прошло=%" PRIu64,
         m->phase, (unsigned)m->state, m->active, m->passed);
    uint64_t delay = m->cfg.delayMs ? m->cfg.delayMs * NS : m->tick;
    m->display = m->now > UINT64_MAX - delay ? UINT64_MAX : m->now + delay;
  }
}
void ModelSnapshot(const Model *m, Snapshot *s) {
  *s = (Snapshot){.sequence = m->sequence,
                  .now = m->now,
                  .passed = m->passed,
                  .phase = m->phase,
                  .signals = m->signals,
                  .active = m->active,
                  .count = m->cfg.streamCount,
                  .state = m->state,
                  .stop = m->stop};
  for (unsigned i = 0; i < m->cfg.streamCount; ++i) {
    const Flow *f = &m->flows[i];
    s->flows[i] = (FlowView){.headId = f->count ? f->queue[f->head].id : 0,
                             .activeId = f->active.id,
                             .leaveAt = f->active.id ? f->leaveAt : 0,
                             .arrived = f->arrived,
                             .passed = f->passed,
                             .refused = f->refused,
                             .waitSum = f->waitSum,
                             .waitMax = f->waitMax,
                             .queued = f->count,
                             .peak = f->peak};
  }
}
