#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
case "${1:-none}" in
  none) build_dir=build; sanitizer=OFF ;;
  asan) build_dir=build-asan; sanitizer=ON ;;
  *) echo 'Usage: tests/run.sh [none|asan]' >&2; exit 2 ;;
esac
# При смене компилятора CMake сбрасывает кэш; затем явно задаём режим сборки.
if [ -n "${CC:-}" ]; then
  cmake -S . -B "$build_dir" "-DCMAKE_C_COMPILER=$CC"
fi
cmake -S . -B "$build_dir" -DCMAKE_BUILD_TYPE=Debug "-DENABLE_SANITIZERS=$sanitizer"
cmake --build "$build_dir" --parallel
ctest --test-dir "$build_dir" --output-on-failure
