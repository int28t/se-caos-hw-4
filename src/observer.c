#include "net.h"
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct View {
  int log;
  uint64_t sequence;
  bool snapshot, ready;
} View;
static bool Line(Peer *p, const char *line, void *context) {
  View *v = context;
  uint64_t values[2];
  if (!strcmp(line, "PONG"))
    return p->role != 0;
  if (NetStop(p, line)) {
    // Сохраняем финальный STOP также в журнале наблюдателя.
  } else if (!p->role) {
    if (strcmp(line, "WELCOME WATCH"))
      return false;
    p->role = 2;
  } else if (NetNumbers(line, "SNAP_BEGIN ", values, 2)) {
    if (v->snapshot || v->ready)
      return false;
    v->sequence = values[0];
    v->snapshot = true;
  } else if (NetNumbers(line, "SNAP_END ", values, 1)) {
    if (!v->snapshot || values[0] != v->sequence)
      return false;
    v->snapshot = false;
    v->ready = true;
  } else if (!strncmp(line, "EVENT ", 6)) {
    char number[21], separator;
    if (!v->ready ||
        sscanf(line, "EVENT %20[0-9]%c", number, &separator) != 2 ||
        separator != ' ' || !Number(number, UINT64_MAX, values) ||
        v->sequence == UINT64_MAX || values[0] != v->sequence + 1)
      return false;
    v->sequence = values[0];
  } else if (strncmp(line, "META ", 5) &&
             !(v->snapshot &&
               (!strncmp(line, "FLOW ", 5) || !strncmp(line, "STATE ", 6))))
    return false;
  char output[LINE_SIZE + 1];
  int n = snprintf(output, sizeof(output), "%s\n", line);
  return n > 0 && (size_t)n < sizeof(output) &&
         WriteText(STDOUT_FILENO, output) &&
         (v->log < 0 || WriteText(v->log, output));
}
int main(int argc, char **argv) {
  if ((argc != 3 && argc != 4) || !NetAddress(argv[1], argv[2])) {
    fprintf(stderr,
            "Использование: crossroad_observer SERVER_IP PORT [NEW_LOG]\n");
    return 2;
  }
  View view = {.log = -1};
  if (argc == 4) {
    // Только новый обычный файл: конфиг и чужой существующий лог не затираются.
    view.log = open(argv[3],
                    O_WRONLY | O_CREAT | O_EXCL | O_NONBLOCK | O_CLOEXEC, 0600);
    struct stat st;
    if (view.log < 0 || fstat(view.log, &st) < 0 || !S_ISREG(st.st_mode)) {
      perror("log");
      if (view.log >= 0)
        close(view.log);
      return 1;
    }
  }
  int status =
      NetClient(argv[1], argv[2], "HELLO 1 WATCH\n", Line, NULL, &view);
  if (view.log >= 0 && close(view.log) < 0)
    status = 1;
  return status;
}
