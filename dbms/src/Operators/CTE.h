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

#include <Core/Block.h>
#include <Common/RWLock.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>

#include <shared_mutex>
#include <utility>

namespace DB
{
enum class FetchStatus
{
    Ok,
    Waiting,
    Eof,
    Cancelled
};

class CTE : public NotifyFuture
{
public:
    std::pair<FetchStatus, Block> tryGetBlockAt(size_t idx);
    FetchStatus checkAvailableBlockAt(size_t idx);
    void pushBlock(const Block & block);
    void notifyEOF();

    void registerTask(TaskPtr && task) override;

private:
    // Return true if CTE has data
    inline bool hasDataNoLock() const { return !this->blocks.empty() || this->spill_triggered; }

    std::shared_mutex rw_lock;
    Blocks blocks;

    // Tasks in WAITING_FOR_NOTIFY are saved in this deque
    std::deque<TaskPtr> waiting_tasks;
    PipeConditionVariable pipe_cv;

    bool is_eof = false;

    bool spill_triggered = false; // TODO this var may be useless, just a placement so far
    // TODO spill
};
} // namespace DB
