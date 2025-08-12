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

#include <Common/Exception.h>
#include <Common/RWLock.h>
#include <Core/Block.h>
#include <Operators/CTEPartition.h>
#include <tipb/select.pb.h>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>

namespace DB
{
class CTE
{
public:
    explicit CTE(size_t partition_num_, size_t expected_sink_num_, size_t expected_source_num_)
        : partition_num(partition_num_)
        , expected_sink_num(expected_sink_num_)
        , expected_source_num(expected_source_num_)
    {
        for (size_t i = 0; i < this->partition_num; i++)
        {
            this->partitions.push_back(CTEPartition());
            this->partitions.back().fetch_block_idxs.resize(this->expected_source_num, 0);
            this->partitions.back().mu = std::make_unique<std::mutex>();
            this->partitions.back().pipe_cv = std::make_unique<PipeConditionVariable>();
        }
    }

    size_t getCTEReaderID()
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        RUNTIME_CHECK_MSG(
            this->next_cte_reader_id < this->expected_source_num,
            "next_cte_reader_id: {}, expected_source_num: {}",
            this->next_cte_reader_id,
            this->expected_source_num);
        auto cte_reader_id = this->next_cte_reader_id;
        this->next_cte_reader_id++;
        return cte_reader_id;
    }

    CTEOpStatus tryGetBlockAt(size_t cte_reader_id, size_t partition_id, Block & block);

    bool pushBlock(size_t partition_id, const Block & block);
    void notifyEOF() { this->notifyImpl<false>(true, ""); }
    void notifyCancel(const String & msg) { this->notifyImpl<true>(false, msg); }

    String getError()
    {
        std::shared_lock<std::shared_mutex> lock(this->rw_lock);
        return this->err_msg;
    }

    void checkBlockAvailableAndRegisterTask(TaskPtr && task, size_t cte_reader_id, size_t partition_id);

    void registerTask(size_t partition_id, TaskPtr && task, NotifyType type);
    void notifyTaskDirectly(size_t partition_id, TaskPtr && task)
    {
        this->partitions[partition_id].pipe_cv->notifyTaskDirectly(std::move(task));
    }

    void addResp(const tipb::SelectResponse & resp)
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        this->resp.MergeFrom(resp);
    }

    void tryToGetResp(tipb::SelectResponse & resp)
    {
        std::shared_lock<std::shared_mutex> lock(this->rw_lock);
        if (!this->get_resp)
        {
            this->get_resp = true;
            resp.CopyFrom(this->resp);
        }
    }

    void registerSink()
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        this->registered_sink_num++;
    }

    template <bool need_lock>
    bool areAllSinksRegistered()
    {
        if constexpr (need_lock)
        {
            std::shared_lock<std::shared_mutex> lock(this->rw_lock);
            return this->registered_sink_num == this->expected_sink_num;
        }
        else
        {
            return this->registered_sink_num == this->expected_sink_num;
        }
    }

    void checkSourceConcurrency(size_t concurrency) const
    {
        RUNTIME_CHECK_MSG(
            concurrency == this->partition_num,
            "concurrency: {}, partition_num: {}",
            concurrency,
            this->partition_num);
    }

    void checkSinkConcurrency(size_t concurrency) const
    {
        RUNTIME_CHECK_MSG(
            concurrency <= this->partition_num,
            "concurrency: {}, partition_num: {}",
            concurrency,
            this->partition_num);
    }

    void sinkExit()
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        this->sink_exit_num++;
        if (this->getSinkExitNumNoLock() == this->getExpectedSinkNum())
            this->notifyEOF();
    }

    void sourceExit()
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        this->source_exit_num++;
    }

    bool allExit()
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        return this->getTotalExitNumNoLock() == this->getExpectedTotalNum();
    }

private:
    Int32 getTotalExitNumNoLock() const noexcept { return this->sink_exit_num + this->source_exit_num; }
    Int32 getSinkExitNumNoLock() const noexcept { return this->sink_exit_num; }
    Int32 getExpectedSinkNum() const noexcept { return this->expected_sink_num; }
    Int32 getExpectedTotalNum() const noexcept { return this->getExpectedSinkNum() + this->expected_source_num; }

    CTEOpStatus checkBlockAvailableNoLock(size_t cte_reader_id, size_t partition_id)
    {
        if unlikely (this->is_cancelled)
            return CTEOpStatus::CANCELLED;

        if (this->partitions[partition_id].blocks.size()
            <= this->partitions[partition_id].fetch_block_idxs[cte_reader_id])
            return this->is_eof ? CTEOpStatus::END_OF_FILE : CTEOpStatus::BLOCK_NOT_AVAILABLE;

        return CTEOpStatus::OK;
    }

    template<bool need_lock>
    void notifyImpl(bool is_eof, const String & msg)
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock, std::defer_lock);
        if constexpr (need_lock)
            lock.lock();

        if likely (is_eof)
            this->is_eof = true;
        else
        {
            this->is_cancelled = true;
            this->err_msg = msg;
        }

        for (auto & partition : this->partitions)
            partition.pipe_cv->notifyAll();
    }

    const size_t partition_num;
    std::vector<CTEPartition> partitions;

    std::shared_mutex rw_lock;
    size_t next_cte_reader_id = 0;
    bool is_eof = false;
    bool is_cancelled = false;

    bool get_resp = false;
    tipb::SelectResponse resp;

    String err_msg;

    const size_t expected_sink_num;
    const size_t expected_source_num;
    size_t registered_sink_num = 0;

    Int32 sink_exit_num = 0;
    Int32 source_exit_num = 0;
};
} // namespace DB
