#!/bin/bash

clang_format=$(bash -c "compgen -c clang-format | grep 'clang-format' | sort --version-sort | head -n1")

if [[ ! -z ${clang_format} ]]; then
  find ./ -name "*.h" -or -name "*.cc" | xargs ${clang_format} -i
else
  echo clang-format missing. try to install
fi
