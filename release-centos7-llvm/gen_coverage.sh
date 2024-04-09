set -x

## Reference: https://clang.llvm.org/docs/SourceBasedCodeCoverage.html

export NPARALL=${NPARALL:-$(nproc)}
if [ ! -z "${FILTER}" ]; then
    export DBMS_FILTER="--gtest_filter=${FILTER}"
else
    export DBMS_FILTER=""
fi

# Executable path
## Try to locate the binary under some default directories
TEST_BIN="dbms/gtests_dbms"
if [ -f "../out/build/dev-coverage/${TEST_BIN}" ]; then
    PROF_DIR=$(realpath "../out/build/dev-coverage")
elif [ -f "../cmake-build-debug-cov/${TEST_BIN}" ]; then
    PROF_DIR=$(realpath "../cmake-build-debug-cov")
else
    PROF_DIR=$(realpath "../cmake-build-debug-cov")
    echo "testing binaries is not exist, building ..."
    mkdir -p "${PROF_DIR}"
    # Build the testing binary
    cd "${PROF_DIR}" && cmake .. -GNinja -DCMAKE_BUILD_TYPE=Debug -DTEST_LLVM_COVERAGE=ON && ninja -j "${NPARALL}" gtests_dbms && cd -
fi

echo "using the testing binaries from ${PROF_DIR}"

COVERAGE_DIR="${PROF_DIR}/coverage"
[ -d "${COVERAGE_DIR}" ] || mkdir "${COVERAGE_DIR}"

# Run the testing binary and generate prof raw data
export LLVM_PROFILE_FILE="${COVERAGE_DIR}/gtest_dbms.profraw"
echo "running ${TEST_BIN} with filter ${DBMS_FILTER} ..."
cd "${PROF_DIR}"
"${PROF_DIR}/${TEST_BIN}" "${DBMS_FILTER}" > ${COVERAGE_DIR}/dbms_test.output.log 2>&1
cd -
## TODO: Add more testing binaries

# Collect the prof raw data and generate cov report
llvm-profdata merge -sparse "${COVERAGE_DIR}/*.profraw" -o "${COVERAGE_DIR}/merged.profdata"

export LD_LIBRARY_PATH=.
llvm-cov export \
    "${PROF_DIR}/${TEST_BIN}" \
    --format=lcov \
    --instr-profile "${COVERAGE_DIR}/merged.profdata" \
    --ignore-filename-regex "/usr/include/.*" \
    --ignore-filename-regex "/usr/local/.*" \
    --ignore-filename-regex "/usr/lib/.*" \
    --ignore-filename-regex ".*/contrib/.*" \
    --ignore-filename-regex ".*/dbms/src/Debug/.*" \
    --ignore-filename-regex ".*/dbms/src/Client/.*" \
    > "${COVERAGE_DIR}/lcov.info"

REPORT_DIR="${PROF_DIR}/report"
[ -d "${REPORT_DIR}" ] || mkdir "${REPORT_DIR}"
genhtml "${COVERAGE_DIR}/lcov.info" -o "${REPORT_DIR}" --ignore-errors source

echo "The coverage report is built under ${REPORT_DIR}. Checkout the ${REPORT_DIR}/index.html"
echo "You can use "python3 -m http --directory ${REPORT_DIR} 12345" to check out the files by webbrowser"
