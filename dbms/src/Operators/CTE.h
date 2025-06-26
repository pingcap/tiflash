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
    OK,
    BLOCK_NOT_AVAILABLE, // It means that we do not have specified block so far
    IO_OUT,
    IO_IN,
    END_OF_FILE,
    CANCELLED
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
    std::unordered_map<size_t, IdxWithPadding> fetch_block_idxs; // TODO id needs to be adjusted after some blocks have been spilled to the disk
    size_t memory_usage = 0; // TODO need a unified statistic
    std::unique_ptr<PipeConditionVariable> pipe_cv;

    // TODO handle this, some blocks can not be spilled when spill is in execution, they can only be stored temporary
    Blocks tmp_blocks;
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

    enum CTEStatus
    {
        NORMAL = 0,
        NEED_SPILL,
        IN_SPILLING,
    };

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

    CTEOpStatus pushBlock(size_t sink_id, const Block & block);
    void notifyEOF() { this->notifyImpl(true, ""); }
    void notifyCancel(const String & msg) { this->notifyImpl(false, msg); }

    String getError()
    {
        std::shared_lock<std::shared_mutex> lock(this->rw_lock);
        return this->err_msg;
    }

    CTEStatus getStatus();

    void checkBlockAvailableAndRegisterTask(TaskPtr && task, size_t cte_reader_id, size_t source_id);

    void registerTask(size_t partition_id, TaskPtr && task, NotifyType type);
    void notifyTaskDirectly(size_t partition_id, TaskPtr && task)
    {
        this->partitions[partition_id].pipe_cv->notifyTaskDirectly(std::move(task));
    }

    CTEOpStatus getBlockFromDisk(size_t source_id, size_t idx, Block & block);
    bool spillBlocks(size_t sink_id);

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

    CTEOpStatus checkBlockAvailableNoLock(size_t cte_reader_id, size_t partition_id);

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

    size_t memory_threshold = 0;
    
    CTEStatus cte_status = CTEStatus::NORMAL;
    
    // Protecting cte_status and tmp_blocks
    std::shared_mutex aux_rw_lock;
    
    CTESpill cte_spill;
    bool is_spill_triggered = false;

    std::shared_mutex rw_lock;
    bool is_eof = false;
    bool is_cancelled = false;

    bool get_resp = false;
    tipb::SelectResponse resp;

    String err_msg;
};
} // namespace DB
