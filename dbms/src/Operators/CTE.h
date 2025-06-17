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
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <tipb/select.pb.h>

#include <mutex>
#include <shared_mutex>

namespace DB
{
enum class CTEOpStatus
{
    Ok,
    BlockNotAvailable,
    Eof,
    Cancelled
};

class CTE
{
public:
    CTEOpStatus tryGetBlockAt(size_t idx, Block & block);

    bool pushBlock(const Block & block);
    void notifyEOF() { this->notifyImpl<true>(true); }
    void notifyCancel() { this->notifyImpl<true>(false); }

    void checkBlockAvailableAndRegisterTask(TaskPtr && task, size_t expected_block_fetch_idx);
    void registerTask(TaskPtr && task, NotifyType type);
    void notifyTaskDirectly(TaskPtr && task) { this->pipe_cv.notifyTaskDirectly(std::move(task)); }

    void addResp(const tipb::SelectResponse & resp)
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        this->resp.MergeFrom(resp);
    }

    void tryToGetResp(tipb::SelectResponse & resp)
    {
        if (!this->get_resp)
        {
            this->get_resp = true;
            resp.CopyFrom(this->resp);
        }
    }

private:
    CTEOpStatus checkBlockAvailableNoLock(size_t idx)
    {
        if unlikely (this->is_cancelled)
            return CTEOpStatus::Cancelled;

        auto block_num = this->blocks.size();
        if (block_num <= idx)
            return this->is_eof ? CTEOpStatus::Eof : CTEOpStatus::BlockNotAvailable;

        return CTEOpStatus::Ok;
    }

    template <bool has_lock>
    void notifyImpl(bool is_eof)
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock, std::defer_lock);
        if constexpr (has_lock)
            lock.lock();

        if likely (is_eof)
            this->is_eof = true;
        else
            this->is_cancelled = true;

        // Just in case someone is in WAITING_FOR_NOTIFY status
        this->pipe_cv.notifyAll();
    }

    std::shared_mutex rw_lock;
    Blocks blocks;
    size_t memory_usage = 0;

    PipeConditionVariable pipe_cv;

    bool is_eof = false;
    bool is_cancelled = false;

    bool get_resp = false;
    tipb::SelectResponse resp;
};
} // namespace DB
