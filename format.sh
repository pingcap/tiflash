#!/bin/bash

CLANG_FORMAT=${CLANG_FORMAT:-clang-format}
command -v ${CLANG_FORMAT} >/dev/null 2>&1
if [[ $? != 0 ]]; then
    echo ${CLANG_FORMAT} missing. Try to install clang-format or set env variable CLANG_FORMAT
else
    find dbms libs -name "*.cpp" -or -name "*.h" -exec ${CLANG_FORMAT} -i {} +
fi
