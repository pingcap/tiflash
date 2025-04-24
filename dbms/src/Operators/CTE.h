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

#include <Common/RWLock.h>
#include <Core/Block.h>
#include <Core/CTESpill.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>

#include <atomic>
#include <shared_mutex>
#include <utility>

namespace DB
{
enum class Status
{
    Ok,
    Waiting,
    IOOut,
    IOIn,
    Eof,
    Cancelled
};

class CTE : public NotifyFuture
{
public:
    ~CTE() override = default;

    enum CTEStatus
    {
        Normal = 0,
        NeedSpill,
        InSpilling,
    };

    std::pair<Status, Block> tryGetBlockAt(size_t idx);
    std::pair<Status, Block> getBlockFromDisk(size_t idx);
    CTEStatus getStatus();
    Status pushBlock(const Block & block);
    void notifyEOF();

    // TODO should have return value to indicate if spill successes
    void spillBlocks();

    void registerTask(TaskPtr && task) override;

private:
    std::shared_mutex rw_lock;
    Blocks blocks;

    size_t memory_usage = 0;
    size_t memory_threshold = 0; // TODO initialize it

    CTEStatus cte_status = CTEStatus::Normal;

    // TODO handle this, some blocks can not be spilled when spill is in execution, they can only be stored temporary
    Blocks tmp_blocks;

    // Protecting cte_status and tmp_blocks
    std::shared_mutex aux_rw_lock;

    // TODO tasks waiting for notify may be lack of data or waiting for spill, do we need two pipe_cv to handle this?
    PipeConditionVariable pipe_cv;

    CTESpill cte_spill;

    bool is_eof = false;
    bool is_spill_triggered = false;
};
} // namespace DB
