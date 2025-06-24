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
#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <absl/base/optimization.h>
#include <tipb/select.pb.h>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <utility>

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
    IdxWithPadding() = default;
    explicit IdxWithPadding(size_t idx_)
        : idx(idx_)
    {}

    size_t idx = 0;

    // To avoid false sharing
    char padding[ABSL_CACHELINE_SIZE]{};
};

struct CTEPartition
{
    std::unique_ptr<std::mutex> mu;
    Blocks blocks;
    std::unordered_map<size_t, IdxWithPadding> fetch_block_idxs;
    size_t memory_usages = 0; // TODO need a unified statistic
    std::unique_ptr<PipeConditionVariable> pipe_cv;
};

class CTE
{
public:
    explicit CTE(size_t partition_num_)
        : partition_num(partition_num_)
    {
        for (size_t i = 0; i < this->partition_num; i++)
        {
            this->partitions.push_back(CTEPartition());
            this->partitions.back().mu = std::make_unique<std::mutex>();
            this->partitions.back().pipe_cv = std::make_unique<PipeConditionVariable>();
        }
    }

    void init();

    size_t getCTEReaderID()
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        auto cte_reader_id = this->next_cte_reader_id;
        this->next_cte_reader_id++;
        for (auto & item : this->partitions)
            item.fetch_block_idxs.insert(std::make_pair(cte_reader_id, IdxWithPadding(0)));
        return cte_reader_id;
    }

    CTEOpStatus tryGetBlockAt(size_t cte_reader_id, size_t source_id, Block & block);

    bool pushBlock(size_t sink_id, const Block & block);
    void notifyEOF() { this->notifyImpl(true); }
    void notifyCancel() { this->notifyImpl(false); }

    void notifyError(const String & err_msg)
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        this->err_msg = err_msg;
    }

    String getError()
    {
        std::shared_lock<std::shared_mutex> lock(this->rw_lock);
        return this->err_msg;
    }

    void checkBlockAvailableAndRegisterTask(TaskPtr && task, size_t cte_reader_id, size_t source_id);

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

private:
    size_t getPartitionID(size_t id) const { return id % this->partition_num; }

    CTEOpStatus checkBlockAvailableNoLock(size_t cte_reader_id, size_t partition_id)
    {
        if unlikely (this->is_cancelled)
            return CTEOpStatus::Cancelled;

        if (this->partitions[partition_id].blocks.size()
            <= this->partitions[partition_id].fetch_block_idxs[cte_reader_id].idx)
            return this->is_eof ? CTEOpStatus::Eof : CTEOpStatus::BlockNotAvailable;

        return CTEOpStatus::Ok;
    }

    void notifyImpl(bool is_eof)
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);

        if likely (is_eof)
            this->is_eof = true;
        else
            this->is_cancelled = true;

        for (auto & partition : this->partitions)
            partition.pipe_cv->notifyAll();
    }

    size_t partition_num;
    std::vector<CTEPartition> partitions;

    std::shared_mutex rw_lock;
    size_t next_cte_reader_id = 0;
    bool is_eof = false;
    bool is_cancelled = false;

    bool get_resp = false;
    tipb::SelectResponse resp;

    String err_msg;
};
} // namespace DB
