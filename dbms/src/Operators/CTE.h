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
#include <absl/base/optimization.h>
#include <tipb/select.pb.h>

#include <mutex>
#include <unordered_map>

namespace DB
{
enum class CTEOpStatus
{
    Ok,
    BlockNotAvailable,
    Eof,
    Cancelled,
    Error
};

struct IdxWithPadding
{
    explicit IdxWithPadding(size_t idx_)
        : idx(idx_)
    {}

    size_t idx;

    // To avoid false sharing
    char padding[ABSL_CACHELINE_SIZE]{};
};

class CTE
{
public:
    size_t getCTEReaderID()
    {
        std::lock_guard<std::mutex> lock(this->mu);
        auto ret = this->next_cte_reader_id;
        this->next_cte_reader_id++;
        this->fetch_block_idxs.insert(std::make_tuple(ret, IdxWithPadding(0)));
        return ret;
    }

    CTEOpStatus tryGetBlockAt(size_t cte_reader_id, Block & block);

    bool pushBlock(const Block & block);
    void notifyEOF() { this->notifyImpl<true>(true); }
    void notifyCancel() { this->notifyImpl<true>(false); }

    void notifyError(const String & err_msg)
    {
        std::lock_guard<std::mutex> lock(this->mu);
        this->err_msg = err_msg;
    }

    String getError()
    {
        std::lock_guard<std::mutex> lock(this->mu);
        return this->err_msg;
    }

    void checkBlockAvailableAndRegisterTask(TaskPtr && task, size_t cte_reader_id);

    void registerTask(TaskPtr && task, NotifyType type);
    void notifyTaskDirectly(TaskPtr && task) { this->pipe_cv.notifyTaskDirectly(std::move(task)); }

    void addResp(const tipb::SelectResponse & resp)
    {
        std::lock_guard<std::mutex> lock(this->mu);
        this->resp.MergeFrom(resp);
    }

    void tryToGetResp(tipb::SelectResponse & resp)
    {
        std::lock_guard<std::mutex> lock(this->mu);
        if (!this->get_resp)
        {
            this->get_resp = true;
            resp.CopyFrom(this->resp);
        }
    }

private:
    CTEOpStatus checkBlockAvailableNoLock(size_t cte_reader_id)
    {
        if unlikely (this->is_cancelled)
            return CTEOpStatus::Cancelled;

        if (this->blocks.size() <= this->fetch_block_idxs[cte_reader_id].idx)
            return this->is_eof ? CTEOpStatus::Eof : CTEOpStatus::BlockNotAvailable;

        return CTEOpStatus::Ok;
    }

    template <bool has_lock>
    void notifyImpl(bool is_eof)
    {
        std::unique_lock<std::mutex> lock(this->mu, std::defer_lock);
        if constexpr (has_lock)
            lock.lock();

        if likely (is_eof)
            this->is_eof = true;
        else
            this->is_cancelled = true;

        // Just in case someone is in WAITING_FOR_NOTIFY status
        this->pipe_cv.notifyAll();
    }

    // Suppose there are 100 read threads and 10 write threads, for write threads
    // they are hard to get mu lock and the write operation will be blocked
    // for longer time. In order to make read operation and write operation to
    // have same opportunity to get the mu, we introduce read_mu and write_mu,
    // so that there are only one read thread and one write thread to try to
    // get the mu.
    std::mutex read_mu;
    std::mutex write_mu;

    std::mutex mu;
    Blocks blocks;
    size_t memory_usage = 0;
    std::unordered_map<size_t, IdxWithPadding> fetch_block_idxs;
    size_t next_cte_reader_id = 0;

    PipeConditionVariable pipe_cv;

    bool is_eof = false;
    bool is_cancelled = false;

    bool get_resp = false;
    tipb::SelectResponse resp;

    String err_msg;
};
} // namespace DB
