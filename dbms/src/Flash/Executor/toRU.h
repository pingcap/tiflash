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

#pragma once

#include <common/types.h>

namespace DB
{
using RU = double;

// Convert cpu time nanoseconds to cpu time millisecond, and round up.
UInt64 toCPUTimeMillisecond(UInt64 cpu_time_ns);

// Convert cpu time nanoseconds to Request Unit.
RU cpuTimeToRU(UInt64 cpu_time_ns);
RU bytesToRU(UInt64 bytes);

static constexpr UInt64 bytes_of_one_ru = 1024 * 64;
static constexpr UInt64 bytes_of_one_hundred_ru = 100 * bytes_of_one_ru;
} // namespace DB
