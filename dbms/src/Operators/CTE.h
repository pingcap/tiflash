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
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Interpreters/CTESpillContext.h>
#include <Operators/CTEPartition.h>
#include <tipb/select.pb.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>

namespace DB
{
inline String genInfo(const String & name, const std::map<size_t, std::atomic_size_t> & data)
{
    String info = fmt::format("{}: ", name);
    for (const auto & item : data)
        info = fmt::format("{}, <{}, {}>", info, item.first, item.second.load());
    return info;
}

class CTE
{
public:
    CTE(size_t partition_num_, size_t expected_sink_num_, size_t expected_source_num_)
        : partition_num(partition_num_)
        , expected_sink_num(expected_sink_num_)
        , expected_source_num(expected_source_num_)
    {
        for (size_t i = 0; i < this->partition_num; i++)
        {
            this->partitions.push_back(std::make_shared<CTEPartition>(i, expected_source_num_));
            for (size_t cte_reader_id = 0; cte_reader_id < expected_source_num_; cte_reader_id++)
                this->partitions.back()->fetch_block_idxs.insert(std::make_pair(cte_reader_id, 0));
            this->partitions.back()->mu = std::make_unique<std::mutex>();
            this->partitions.back()->pipe_cv = std::make_unique<PipeConditionVariable>();
        }
    }

    void initForTest()
    {
        for (size_t i = 0; i < this->partition_num; i++)
        {
            this->partitions[i]->mu_for_test = std::make_unique<std::mutex>();
            this->partitions[i]->cv_for_test = std::make_unique<std::condition_variable>();
        }
    }

    // ------------------------
    // TODO remove, for test
    std::mutex mu_test;
    std::atomic_size_t total_recv_blocks = 0;
    std::atomic_size_t total_recv_rows = 0;
    std::atomic_size_t total_spilled_blocks = 0;
    std::atomic_size_t total_spilled_rows = 0;
    std::map<size_t, std::atomic_size_t> total_fetch_blocks;
    std::map<size_t, std::atomic_size_t> total_fetch_rows;
    std::map<size_t, std::atomic_size_t> total_fetch_blocks_in_disk;
    std::map<size_t, std::atomic_size_t> total_fetch_rows_in_disk;
    // ------------------------

    ~CTE()
    {
        String info;
        info = fmt::format(
            "total_recv_blocks: {}, total_recv_rows: {}, total_spilled_blocks: {}, total_spilled_rows: {}, ",
            total_recv_blocks.load(),
            total_recv_rows.load(),
            total_spilled_blocks.load(),
            total_spilled_rows.load());
        info = fmt::format("{} | {}", info, genInfo("total_fetch_blocks", this->total_fetch_blocks));
        info = fmt::format("{} | {}", info, genInfo("total_fetch_rows", this->total_fetch_rows));
        info = fmt::format("{} | {}", info, genInfo("total_fetch_blocks_in_disk", this->total_fetch_blocks_in_disk));
        info = fmt::format("{} | {}", info, genInfo("total_fetch_rows_in_disk", this->total_fetch_rows_in_disk));

        auto * log = &Poco::Logger::get("LRUCache");
        LOG_INFO(log, fmt::format("xzxdebug CTE {}", info));

        for (auto & p : this->partitions)
            p->debugOutput();
    }

    void initCTESpillContextAndPartitionConfig(
        const SpillConfig & spill_config,
        const Block & spill_block_schema,
        UInt64 operator_spill_threshold,
        Context & context);

    void checkPartitionNum(size_t partition_num) const
    {
        RUNTIME_CHECK_MSG(
            this->partition_num == partition_num,
            "expect partition num: {}, actual: {}",
            this->partition_num,
            partition_num);
    }

    size_t getCTEReaderID()
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        RUNTIME_CHECK_MSG(
            this->next_cte_reader_id < this->expected_source_num,
            "next_cte_reader_id: {}, expected_source_num: {}",
            this->next_cte_reader_id,
            this->expected_source_num);
        return this->next_cte_reader_id++;
    }

    CTEOpStatus tryGetBlockAt(size_t cte_reader_id, size_t partition_id, Block & block);
    template <bool for_test>
    CTEOpStatus pushBlock(size_t partition_id, const Block & block);
    template <bool for_test>
    void notifyEOF()
    {
        this->notifyImpl<false, for_test>(true, "");
    }
    template <bool for_test>
    void notifyCancel(const String & msg)
    {
        this->notifyImpl<true, for_test>(false, msg);
    }

    String getError()
    {
        std::shared_lock<std::shared_mutex> lock(this->rw_lock);
        return this->err_msg;
    }

    CTEOpStatus checkBlockAvailableForTest(size_t cte_reader_id, size_t partition_id);

    void checkBlockAvailableAndRegisterTask(TaskPtr && task, size_t cte_reader_id, size_t partition_id);
    void checkInSpillingAndRegisterTask(TaskPtr && task, size_t partition_id);

    CTEOpStatus getBlockFromDisk(size_t cte_reader_id, size_t partition_id, Block & block);
    CTEOpStatus spillBlocks(size_t partition_id);

    void registerTask(size_t partition_id, TaskPtr && task, NotifyType type)
    {
        task->setNotifyType(type);
        this->partitions[partition_id]->pipe_cv->registerTask(std::move(task));
    }

    void notifyTaskDirectly(size_t partition_id, TaskPtr && task)
    {
        this->partitions[partition_id]->pipe_cv->notifyTaskDirectly(std::move(task));
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

    LoggerPtr getLog() const { return this->partition_config->log; }

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

    template <bool for_test>
    void sinkExit()
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        this->sink_exit_num++;
        if (this->getSinkExitNumNoLock() == this->getExpectedSinkNum())
            this->notifyEOF<for_test>();
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

    CTEOpStatus checkBlockAvailable(size_t cte_reader_id, size_t partition_id)
    {
        return this->checkBlockAvailableImpl<true>(cte_reader_id, partition_id);
    }

    CTEOpStatus checkBlockAvailableNoLock(size_t cte_reader_id, size_t partition_id)
    {
        return this->checkBlockAvailableImpl<false>(cte_reader_id, partition_id);
    }

    std::shared_ptr<CTEPartition> & getPartitionForTest(size_t partition_idx)
    {
        return this->partitions[partition_idx];
    }

private:
    template <bool need_lock>
    CTEOpStatus checkBlockAvailableImpl(size_t cte_reader_id, size_t partition_id)
    {
        std::shared_lock<std::shared_mutex> cte_lock(this->rw_lock, std::defer_lock);
        std::unique_lock<std::mutex> partition_lock(*(this->partitions[partition_id]->mu), std::defer_lock);

        if constexpr (need_lock)
        {
            cte_lock.lock();
            partition_lock.lock();
        }

        if unlikely (this->is_cancelled)
            return CTEOpStatus::CANCELLED;

        if (this->partitions[partition_id]->isBlockAvailableInMemoryNoLock(cte_reader_id))
            return CTEOpStatus::OK;

        return this->is_eof ? CTEOpStatus::END_OF_FILE : CTEOpStatus::BLOCK_NOT_AVAILABLE;
    }

    Int32 getTotalExitNumNoLock() const noexcept { return this->sink_exit_num + this->source_exit_num; }
    Int32 getSinkExitNumNoLock() const noexcept { return this->sink_exit_num; }
    Int32 getExpectedSinkNum() const noexcept { return this->expected_sink_num; }
    Int32 getExpectedTotalNum() const noexcept { return this->getExpectedSinkNum() + this->expected_source_num; }

    template <bool need_lock, bool for_test>
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
        {
            if constexpr (for_test)
                partition->cv_for_test->notify_all();
            else
                partition->pipe_cv->notifyAll();
        }
    }

    const size_t partition_num;
    std::vector<std::shared_ptr<CTEPartition>> partitions;

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

    std::shared_ptr<CTESpillContext> cte_spill_context;
    std::shared_ptr<CTEPartitionSharedConfig> partition_config;
};

class CTEIONotifier : public NotifyFuture
{
public:
    CTEIONotifier(std::shared_ptr<CTE> cte_, size_t partition_id_)
        : cte(cte_)
        , partition_id(partition_id_)
    {}

    void registerTask(TaskPtr && task) override
    {
        this->cte->checkInSpillingAndRegisterTask(std::move(task), this->partition_id);
    }

private:
    std::shared_ptr<CTE> cte;
    size_t partition_id;
};
} // namespace DB
