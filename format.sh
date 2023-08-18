#!/bin/bash
# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CLANG_FORMAT=${CLANG_FORMAT:-clang-format}
command -v ${CLANG_FORMAT} >/dev/null 2>&1
if [[ $? != 0 ]]; then
    echo ${CLANG_FORMAT} missing. Try to install clang-format or set env variable CLANG_FORMAT
else
    find dbms libs -name "*.cpp" -or -name "*.h" -exec ${CLANG_FORMAT} -i {} +
fi
