// Copyright 2023 PingCAP, Inc.
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

#include <Flash/Executor/toRU.h>
#include <common/likely.h>

namespace DB
{

// Convert cpu time nanoseconds to cpu time millisecond, and round up.
UInt64 toCPUTimeMillisecond(UInt64 cpu_time_ns)
{
    if (unlikely(cpu_time_ns == 0))
        return 0;

    return static_cast<double>(cpu_time_ns) / 1'000'000L;
}

// 1 ru = 3 millisecond cpu time
RU toRU(UInt64 cpu_time_ns)
{
    if (unlikely(cpu_time_ns == 0))
        return 0;

    auto cpu_time_millisecond = toCPUTimeMillisecond(cpu_time_ns);
    return static_cast<double>(cpu_time_millisecond) / 3;
}
} // namespace DB
