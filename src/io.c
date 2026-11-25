#include "crossroad.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

bool ValidName(const char *s) {
  size_t n = strlen(s);
  if (!n || n >= NAME_SIZE)
    return false;
  for (; *s; ++s)
    if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') ||
          (*s >= '0' && *s <= '9') || *s == '_' || *s == '-'))
      return false;
  return true;
}
bool Number(const char *s, uint64_t limit, uint64_t *value) {
  if (!*s)
    return false;
  uint64_t n = 0;
  for (; *s; ++s) {
    if (*s < '0' || *s > '9' || n > limit / 10 ||
        (n == limit / 10 && (unsigned)(*s - '0') > limit % 10))
      return false;
    n = n * 10 + (unsigned)(*s - '0');
  }
  *value = n;
  return true;
}
bool ReadText(const char *path, char *text, size_t capacity) {
  int fd = open(path, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
  if (fd < 0) {
    perror(path);
    return false;
  }
  struct stat st;
  size_t used = 0;
  bool ok = capacity > 0 && fstat(fd, &st) == 0 && S_ISREG(st.st_mode);
  while (ok) {
    ssize_t n = read(fd, text + used, capacity - used);
    if (n < 0 && errno == EINTR)
      continue;
    if (n < 0 || used == capacity) {
      ok = false;
      break;
    }
    if (!n) {
      text[used] = 0;
      break;
    }
    if (memchr(text + used, 0, (size_t)n)) {
      ok = false;
      break;
    }
    used += (size_t)n;
  }
  if (close(fd) < 0)
    ok = false;
  if (!ok)
    fprintf(stderr, "Не удалось прочитать обычный текстовый файл: %s\n", path);
  return ok;
}

// Для неблокирующего fd EAGAIN означает отказ вывода, а не вечное ожидание.
bool WriteText(int fd, const char *text) {
  size_t used = 0, size = strlen(text);
  while (used < size) {
    ssize_t n = write(fd, text + used, size - used);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return false;
    used += (size_t)n;
  }
  return true;
}
