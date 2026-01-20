// Copyright 2025 PingCAP, Inc.
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

#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

#include <condition_variable>
#include <memory>

namespace DB
{
enum class CTEOpStatus
{
    OK,
    BLOCK_NOT_AVAILABLE,
    END_OF_FILE,
    CANCELLED,
    SINK_NOT_REGISTERED
};

struct BlockWithCounter
{
    BlockWithCounter(const Block & block_, Int16 counter_)
        : block(block_)
        , counter(counter_)
    {}
    Block block;
    Int16 counter;
};

struct CTEPartition
{
    std::unique_ptr<std::mutex> mu;
    std::vector<BlockWithCounter> blocks;
    std::vector<size_t> fetch_block_idxs;
    size_t memory_usages = 0;
    std::unique_ptr<PipeConditionVariable> pipe_cv;

#ifndef NDEBUG
    std::unique_ptr<std::mutex> mu_for_test;
    std::unique_ptr<std::condition_variable> cv_for_test;
#endif
};
} // namespace DB
