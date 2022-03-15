#!/bin/bash
# Copyright 2022 PingCAP, Ltd.
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


function check_arm_arch() {
    local ARCH=$(uname -i)
    if [[ "$ARCH" =~ ^(aarch64.*|AARCH64.*) || "$ARCH" == arm* ]]; then
        echo 1
    else
        echo 0
    fi
}

export -f check_arm_arch
