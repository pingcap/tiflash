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
#include <tipb/select.pb.h>

#include <mutex>
#include <shared_mutex>

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
    ~CTE() override
    {
        auto * log = &Poco::Logger::get("LRUCache");
        LOG_INFO(log, fmt::format("xzxdebug CTE is destructured block num: {}, row num: {}", this->block_num, this->row_num));
    }

    FetchStatus tryGetBunchBlocks(size_t idx, std::deque<Block> & queue);
    bool pushBlock(const Block & block);
    void notifyEOF() { this->notifyImpl<true>(true); }
    void notifyCancel() { this->notifyImpl<true>(false); }

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

    Int64 blockNumForTest()
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        return this->blocks.size();
    }

private:
    template <bool has_lock>
    void notifyImpl(bool is_eof)
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock, std::defer_lock);
        if constexpr (has_lock)
            lock.lock();

        auto * log = &Poco::Logger::get("LRUCache");
        
        if likely (is_eof)
        {
            LOG_INFO(log, "xzxdebug set eof true");
            this->is_eof = true;
        }
        else
            this->is_cancelled = true;

        // Just in case someone is in WAITING_FOR_NOTIFY status
        this->pipe_cv.notifyAll();
    }

    // Return true if CTE has data
    inline bool hasDataNoLock() const { return !this->blocks.empty(); }

    std::shared_mutex rw_lock;
    Blocks blocks;
    bool first_print = false; // TODO remove
    size_t memory_usage = 0;

    // Tasks in WAITING_FOR_NOTIFY are saved in this deque
    std::deque<TaskPtr> waiting_tasks;
    PipeConditionVariable pipe_cv;

    bool is_eof = false;
    bool is_cancelled = false;

    bool get_resp = false;
    tipb::SelectResponse resp;

    Int64 block_num = 0;
    Int64 row_num = 0;
};
} // namespace DB
