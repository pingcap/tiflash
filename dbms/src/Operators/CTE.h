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
#include <tipb/select.pb.h>

#include <mutex>
#include <shared_mutex>

namespace DB
{
enum class Status
{
    Ok,
    BlockUnavailable, // It means that we do not have specified block so far
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

    CTEStatus getStatus();
    Status tryGetBunchBlocks(size_t idx, std::deque<Block> & queue);
    bool pushBlock(const Block & block);
    void notifyEOF() { this->notifyImpl<true>(true); }
    void notifyCancel() { this->notifyImpl<true>(false); }

    // TODO should have return value to indicate if spill successes
    void spillBlocks();

    void registerTask(TaskPtr && task) override;

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

    // Return true if CTE has data
    inline bool hasDataNoLock() const { return !this->blocks.empty(); }

    std::shared_mutex rw_lock;
    Blocks blocks;
    size_t memory_usage = 0;
    size_t memory_threshold = 0;

    CTEStatus cte_status = CTEStatus::Normal;

    // TODO handle this, some blocks can not be spilled when spill is in execution, they can only be stored temporary
    Blocks tmp_blocks;

    // Protecting cte_status and tmp_blocks
    std::shared_mutex aux_rw_lock;

    // TODO tasks waiting for notify may be lack of data or waiting for spill, do we need two pipe_cv to handle this?
    PipeConditionVariable pipe_cv;

    CTESpill cte_spill;
    bool is_spill_triggered = false;

    bool is_eof = false;
    bool is_cancelled = false;

    bool get_resp = false;
    tipb::SelectResponse resp;
};
} // namespace DB
