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
#include <Operators/CTEPartition.h>
#include <tipb/select.pb.h>

#include <mutex>
#include <shared_mutex>
#include <utility>

namespace DB
{
class CTE
{
public:
    explicit CTE(size_t partition_num_, size_t expected_sink_num_)
        : partition_num(partition_num_)
        , expected_sink_num(expected_sink_num_)
    {
        RUNTIME_CHECK(this->partition_num > 0);
        for (size_t i = 0; i < this->partition_num; i++)
            this->partitions.push_back(CTEPartition(i));
    }

    ~CTE()
    {
        for (auto & p : this->partitions)
            p.debugOutput();
    }

    void initCTESpillContext(
        const SpillConfig & spill_config_,
        const Block & spill_block_schema_,
        UInt64 operator_spill_threshold_,
        const String & query_id_and_cte_id);

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
        auto cte_reader_id = this->next_cte_reader_id;
        this->next_cte_reader_id++;
        for (auto & item : this->partitions)
            item.fetch_block_idxs.insert(std::make_pair(cte_reader_id, 0));
        return cte_reader_id;
    }

    CTEOpStatus tryGetBlockAt(size_t cte_reader_id, size_t partition_id, Block & block);
    CTEOpStatus pushBlock(size_t partition_id, const Block & block);
    void notifyEOF() { this->notifyImpl(true, ""); }
    void notifyCancel(const String & msg) { this->notifyImpl(false, msg); }

    String getError()
    {
        std::shared_lock<std::shared_mutex> lock(this->rw_lock);
        return this->err_msg;
    }

    void checkBlockAvailableAndRegisterTask(TaskPtr && task, size_t cte_reader_id, size_t partition_id);
    void checkInSpillingAndRegisterTask(TaskPtr && task, size_t partition_id);

    CTEOpStatus getBlockFromDisk(size_t cte_reader_id, size_t partition_id, Block & block);
    CTEOpStatus spillBlocks(size_t partition_id);

    void registerTask(size_t partition_id, TaskPtr && task, NotifyType type)
    {
        task->setNotifyType(type);
        this->partitions[partition_id].pipe_cv->registerTask(std::move(task));
    }

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
        std::shared_lock<std::shared_mutex> lock(this->rw_lock, std::defer_lock);
        if constexpr (need_lock)
            lock.lock();
        return this->registered_sink_num == this->expected_sink_num;
    }

private:
    void notifyImpl(bool is_eof, const String & msg)
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);

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

    size_t next_cte_reader_id = 0;

    size_t partition_num;
    std::vector<CTEPartition> partitions;

    std::shared_mutex rw_lock;
    bool is_eof = false;
    bool is_cancelled = false;

    bool get_resp = false;
    tipb::SelectResponse resp;

    const size_t expected_sink_num;
    size_t registered_sink_num = 0;

    String err_msg;
    std::shared_ptr<CTESpillContext> cte_spill_context;
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
