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

#include <Flash/Executor/toCPUTimeSecond.h>
#include <common/likely.h>

namespace DB
{
/**
 * cpu time second used by tiflash compute = vcores * second
 */
UInt64 toCPUTimeSecond(UInt64 cpu_time_ns)
{
    if (unlikely(cpu_time_ns == 0))
        return 0;

    static constexpr double aru_rate = 1.0;
    double cpu_time_second = static_cast<double>(cpu_time_ns) / 1000'000'000L;
    auto ceil_cpu_time_second = ceil(cpu_time_second);
    return ceil_cpu_time_second * aru_rate;
}
} // namespace DB
