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

#include <Common/Stopwatch.h>

#include <atomic>

namespace DB::DM::tests
{
class TimestampGenerator
{
public:
    TimestampGenerator()
        : t(clock_gettime_ns(CLOCK_MONOTONIC))
    {}

    std::vector<uint64_t> get(int count)
    {
        uint64_t start = t.fetch_add(count, std::memory_order_relaxed);
        std::vector<uint64_t> v(count);
        for (int i = 0; i < count; i++)
        {
            v[i] = start + i;
        }
        return v;
    }

    uint64_t get() { return t.fetch_add(1, std::memory_order_relaxed); }

private:
    std::atomic<uint64_t> t;
};
} // namespace DB::DM::tests