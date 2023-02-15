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
#include <DataStreams/SegmentReadTransformAction.h>
#include <Operators/Operator.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

#include <optional>

namespace DB
{

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
        : SourceOp(exec_status_)
        , task_pool(task_pool_)
        , header(toEmptyBlock(columns_to_read_))
        , action(header, extra_table_id_index, physical_table_id)
        , log(Logger::get(req_id))
        , ref_no(0)
        , task_pool_added(false)

    {
        if (extra_table_id_index != InvalidColumnID)
        {
            const auto & extra_table_id_col_define = DM::getExtraTableIDColumnDefine();
            ColumnWithTypeAndName col{extra_table_id_col_define.type->createColumn(), extra_table_id_col_define.type, extra_table_id_col_define.name, extra_table_id_col_define.id, extra_table_id_col_define.default_value};
            header.insert(extra_table_id_index, col);
        }
        ref_no = task_pool->increaseUnorderedInputStreamRefCount();
        LOG_DEBUG(log, "Created, pool_id={} ref_no={}", task_pool->poolId(), ref_no);
        addReadTaskPoolToScheduler();
        setHeader(header);
        LOG_DEBUG(log, "prepare");
    }

    String getName() const override
    {
        return "UnorderedSourceOp";
    }

    OperatorStatus readImpl(Block & block) override
    {
        if (has_temp_block)
        {
            std::cout << "ywq test has temp block" << std::endl;
            std::swap(block, t_block);
            has_temp_block = false;
            if (action.transform(block))
            {
                std::cout << "ywq test block rows v1:" << block.rows() << std::endl;
                return OperatorStatus::HAS_OUTPUT;
            }
            else
                return OperatorStatus::FINISHED;
        }
        else
        {
            size_t i = 0;
            while (true)
            {
                std::cout << "ywq test while i=" << i << std::endl;
                Block res;
                if (!task_pool->tryPopBlock(res))
                {
                    std::cout << "ywq test waiting" << std::endl;
                    return OperatorStatus::WAITING;
                }

                if (res)
                {
                    if (action.transform(res))
                    {
                        std::swap(block, res);
                        std::cout << "ywq test block rows:" << block.rows() << std::endl;
                        return OperatorStatus::HAS_OUTPUT;
                    }
                    else
                    {
                        std::cout << "ywq test block empty" << std::endl;
                        i++;
                        continue;
                    }
                }
                else
                {
                    std::cout << "ywq test finished" << std::endl;
                    return OperatorStatus::FINISHED;
                }
            }
        }
    }

    OperatorStatus awaitImpl() override
    {
        Block res;
        if (!task_pool->tryPopBlock(res))
        {
            // std::cout << "ywq test await waiting" << std::endl;
            return OperatorStatus::WAITING;
        }
        if (res)
        {
            if (res.rows() == 0)
            {
                return OperatorStatus::WAITING;
            }
            t_block = std::move(res);
            has_temp_block = true;
            std::cout << "ywq test await done" << std::endl;
            return OperatorStatus::HAS_OUTPUT;
        }
        else
        {
            std::cout << "ywq test await finished" << std::endl;
            return OperatorStatus::FINISHED;
        }
    }

    void addReadTaskPoolToScheduler()
    {
        if (likely(task_pool_added))
        {
            return;
        }
        std::call_once(task_pool->addToSchedulerFlag(), [&]() { DM::SegmentReadTaskScheduler::instance().add(task_pool); });
        task_pool_added = true;
    }

private:
    DM::SegmentReadTaskPoolPtr task_pool;
    Block header;
    SegmentReadTransformAction action;
    Block t_block;
    bool has_temp_block = false;

    const LoggerPtr log;
    int64_t ref_no;
    bool task_pool_added;
};
} // namespace DB
