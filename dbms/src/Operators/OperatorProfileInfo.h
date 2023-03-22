// Copyright 2023 PingCAP, Ltd.
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
#include <Core/Block.h>

namespace DB
{
/// Information for Operator profiling
struct OperatorProfileInfo
{
    bool started = false;
    Stopwatch total_stopwatch{CLOCK_MONOTONIC_COARSE};

    size_t rows = 0;
    size_t blocks = 0;
    size_t bytes = 0;
    // execution time is the total time spent on current Operator
    UInt64 execution_time = 0;

    void update(const Block & block, UInt64 time)
    {
        ++blocks;
        rows += block.rows();
        bytes += block.bytes();
        execution_time += time;
    }

    void updateTime(UInt64 time)
    {
        execution_time += time;
    }

    void start()
    {
        total_stopwatch.start();
    }
};

using OperatorProfileInfoPtr = std::shared_ptr<OperatorProfileInfo>;

} // namespace DB
