// Copyright 2023 PingCAP, Ltd.
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

#include <Common/Logger.h>
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <Operators/Operator.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{

/// Read blocks asyncly from Storage Layer by using read thread,
/// The result can not guarantee the keep_order property
class UnorderedSourceOp : public SourceOp
{
public:
    UnorderedSourceOp(
        PipelineExecutorStatus & exec_status_,
        const DM::SegmentReadTaskPoolPtr & task_pool_,
        const DM::ColumnDefines & columns_to_read_,
        int extra_table_id_index_,
        const String & req_id)
        : SourceOp(exec_status_, req_id)
        , task_pool(task_pool_)
        , ref_no(0)
    {
        setHeader(AddExtraTableIDColumnTransformAction::buildHeader(columns_to_read_, extra_table_id_index_));
        ref_no = task_pool->increaseUnorderedInputStreamRefCount();
        LOG_DEBUG(log, "Created, pool_id={} ref_no={}", task_pool->pool_id, ref_no);
    }

    ~UnorderedSourceOp() override
    {
        task_pool->decreaseUnorderedInputStreamRefCount();
        LOG_DEBUG(log, "Destroy, pool_id={} ref_no={}", task_pool->pool_id, ref_no);
    }

    String getName() const override
    {
        return "UnorderedSourceOp";
    }

    IOProfileInfoPtr getIOProfileInfo() const override { return IOProfileInfo::createForLocal(profile_info_ptr); }

protected:
    void operatePrefixImpl() override
    {
        std::call_once(task_pool->addToSchedulerFlag(), [&]() { DM::SegmentReadTaskScheduler::instance().add(task_pool); });
    }

    OperatorStatus readImpl(Block & block) override;
    OperatorStatus awaitImpl() override;

private:
    DM::SegmentReadTaskPoolPtr task_pool;
    std::optional<Block> t_block;
    int64_t ref_no;
};
} // namespace DB
