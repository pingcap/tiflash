// Copyright 2023 PingCAP, Inc.
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
#include <DataStreams/SegmentReadTransformAction.h>
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
        const int extra_table_id_index,
        const TableID physical_table_id,
        const String & req_id)
        : SourceOp(exec_status_, req_id)
        , task_pool(task_pool_)
        , action(header, extra_table_id_index, physical_table_id)
    {
        setHeader(toEmptyBlock(columns_to_read_));
        if (extra_table_id_index != InvalidColumnID)
        {
            const auto & extra_table_id_col_define = DM::getExtraTableIDColumnDefine();
            ColumnWithTypeAndName col{extra_table_id_col_define.type->createColumn(), extra_table_id_col_define.type, extra_table_id_col_define.name, extra_table_id_col_define.id, extra_table_id_col_define.default_value};
            header.insert(extra_table_id_index, col);
        }
        auto ref_no = task_pool->increaseUnorderedInputStreamRefCount();
        LOG_DEBUG(log, "Created, pool_id={} ref_no={}", task_pool->poolId(), ref_no);
        addReadTaskPoolToScheduler();
    }

    String getName() const override
    {
        return "UnorderedSourceOp";
    }

protected:
    OperatorStatus readImpl(Block & block) override;
    OperatorStatus awaitImpl() override;

private:
    void addReadTaskPoolToScheduler()
    {
        std::call_once(task_pool->addToSchedulerFlag(), [&]() { DM::SegmentReadTaskScheduler::instance().add(task_pool); });
    }

private:
    DM::SegmentReadTaskPoolPtr task_pool;
    SegmentReadTransformAction action;
    std::optional<Block> t_block;
};
} // namespace DB
