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
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <Flash/Coprocessor/RuntimeFilterMgr.h>
#include <Operators/Operator.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
struct NotifyFuture;
using NotifyFuturePtr = std::shared_ptr<NotifyFuture>;

/// Read blocks asyncly from Storage Layer by using read thread,
/// The result can not guarantee the keep_order property
class UnorderedSourceOp : public SourceOp
{
public:
    UnorderedSourceOp(
        PipelineExecutorContext & exec_context_,
        const DM::SegmentReadTaskPoolPtr & task_pool_,
        const DM::ColumnDefines & columns_to_read_,
        int extra_table_id_index_,
        const String & req_id,
        const RuntimeFilteList & runtime_filter_list_ = std::vector<RuntimeFilterPtr>{},
        int max_wait_time_ms_ = 0);

    ~UnorderedSourceOp() override
    {
        if (const auto rc_before_decr = task_pool->decreaseUnorderedInputStreamRefCount(); rc_before_decr == 1)
        {
            LOG_INFO(
                log,
                "All unordered input streams are finished, pool_id={} last_stream_ref_no={}",
                task_pool->pool_id,
                ref_no);
        }
    }

    String getName() const override { return "UnorderedSourceOp"; }

    IOProfileInfoPtr getIOProfileInfo() const override { return IOProfileInfo::createForLocal(profile_info_ptr); }

    // only for unit test
    // The logic order of unit test is error, it will build source_op firstly and register rf secondly.
    // It causes source_op could not get RF list in constructor.
    // So, for unit test, it should call this function separated.
    void setRuntimeFilterInfo(const RuntimeFilteList & runtime_filter_list_, int max_wait_time_ms_)
    {
        waiting_rf_list = runtime_filter_list_;
        max_wait_time_ms = max_wait_time_ms_;
    }

protected:
    void operatePrefixImpl() override;

    OperatorStatus readImpl(Block & block) override;

private:
    OperatorStatus doFetchBlock();

private:
    DM::SegmentReadTaskPoolPtr task_pool;
    int64_t ref_no;
    NotifyFuturePtr notify_future;

    // runtime filter
    RuntimeFilteList waiting_rf_list;
    int max_wait_time_ms;

    bool done = false;
    Block t_block;
};
} // namespace DB
