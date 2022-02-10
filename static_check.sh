## env settings
WORKSPACE=$(cd `dirname $0`; pwd)
BUILD_DIR="cmake-build-debug"
LLVM_ROOT="/opt/homebrew/opt/llvm/"
TIDY_BIN="${LLVM_ROOT}/bin/clang-tidy"
TIDY_SCRIPT="${LLVM_ROOT}/share/clang/run-clang-tidy.py"

DIR_TO_CHECK="${1}"

command -v "${TIDY_SCRIPT}" > /dev/null 2>&1
if [[ $? == 0 ]]; then
  # Check files by script, with parallel
  python3 "${TIDY_SCRIPT}" -p "${WORKSPACE}/${BUILD_DIR}/" -j 4 "${DIR_TO_CHECK}/*"
else
  # Check each file by clang-tidy
  for f in $(find "${DIR_TO_CHECK}" -name '*.cpp' -o -name "*.h"); do
    echo "Checking $f"
      "${TIDY_BIN}" -p "${WORKSPACE}/${BUILD_DIR}/compile_commands.json" "$f"
  done
fi
