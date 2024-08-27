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

#include <Common/FailPoint.h>
#include <Interpreters/Context.h>
#include <Operators/DMSegmentThreadSourceOp.h>
#include <Storages/DeltaMerge/DMContext.h>

namespace DB
{
namespace FailPoints
{
extern const char pause_when_reading_from_dt_stream[];
} // namespace FailPoints

DMSegmentThreadSourceOp::DMSegmentThreadSourceOp(
    PipelineExecutorContext & exec_context_,
    const DM::DMContextPtr & dm_context_,
    const DM::SegmentReadTaskPoolPtr & task_pool_,
    DM::AfterSegmentRead after_segment_read_,
    const DM::ColumnDefines & columns_to_read_,
    const DM::PushDownFilterPtr & filter_,
    UInt64 start_ts_,
    size_t expected_block_size_,
    DM::ReadMode read_mode_,
    const String & req_id)
    : SourceOp(exec_context_, req_id)
    , dm_context(dm_context_)
    , task_pool(task_pool_)
    , after_segment_read(after_segment_read_)
    , columns_to_read(columns_to_read_)
    , filter(filter_)
    , start_ts(start_ts_)
    , expected_block_size(expected_block_size_)
    , read_mode(read_mode_)
{
    setHeader(toEmptyBlock(columns_to_read));
}

String DMSegmentThreadSourceOp::getName() const
{
    return NAME;
}

void DMSegmentThreadSourceOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "Finish read {} rows from storage", total_rows);
}

OperatorStatus DMSegmentThreadSourceOp::readImpl(Block & block)
{
    if unlikely (done)
    {
        block = {};
        return OperatorStatus::HAS_OUTPUT;
    }
    while (true)
    {
        while (!cur_stream)
        {
            auto task = task_pool->nextTask();
            if (!task)
            {
                done = true;
                LOG_DEBUG(log, "Read done");
                block = {};
                return OperatorStatus::HAS_OUTPUT;
            }
            cur_segment = task->segment;

            auto block_size = std::max(
                expected_block_size,
                static_cast<size_t>(dm_context->global_context.getSettingsRef().dt_segment_stable_pack_rows));
            cur_stream = task->segment->getInputStream(
                read_mode,
                *dm_context,
                columns_to_read,
                task->read_snapshot,
                task->ranges,
                filter,
                start_ts,
                block_size);
            LOG_TRACE(log, "Start to read segment, segment={}", cur_segment->simpleInfo());
        }
        FAIL_POINT_PAUSE(FailPoints::pause_when_reading_from_dt_stream);

        Block res = cur_stream->read(filter_ignored, false);
        if (res)
        {
            block = std::move(res);
            return OperatorStatus::HAS_OUTPUT;
        }
        else
        {
            after_segment_read(dm_context, cur_segment);
            LOG_TRACE(log, "Finish reading segment, segment={}", cur_segment->simpleInfo());
            cur_segment = {};
            cur_stream = {};
        }
    }
}

} // namespace DB
