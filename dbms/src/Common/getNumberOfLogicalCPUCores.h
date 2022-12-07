// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/Exception.h>
#include <common/types.h>

inline UInt16 getNumberOfLogicalCPUCores(UInt16 max_logical_cpu_cores = 0)
{
    static UInt64 n = max_logical_cpu_cores;
    if (unlikely(n == 0))
        throw DB::Exception("Number of logical cpu cores is illegal 0, ensure this function is not called before ServerInfo is initialized");
    return n;
}

// We should call this function before Context has been created,
// which will call `getNumberOfLogicalCPUCores`, or we can not
// set cpu cores any more.
inline void setNumberOfLogicalCPUCores(UInt16 max_logical_cpu_cores)
{
    getNumberOfLogicalCPUCores(max_logical_cpu_cores);
}
