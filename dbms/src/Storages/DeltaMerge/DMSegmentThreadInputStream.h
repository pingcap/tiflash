// Copyright 2022 PingCAP, Ltd.
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

#include <Common/FailPoint.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/SegmentReadTransformAction.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
namespace FailPoints
{
extern const char pause_when_reading_from_dt_stream[];
} // namespace FailPoints

namespace DM
{
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;

class DMSegmentThreadInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "DeltaMergeSegmentThread";

public:
    /// If handle_real_type_ is empty, means do not convert handle column back to real type.
    DMSegmentThreadInputStream(
        const DMContextPtr & dm_context_,
        const SegmentReadTaskPoolPtr & task_pool_,
        AfterSegmentRead after_segment_read_,
        const ColumnDefines & columns_to_read_,
        const PushDownFilterPtr & filter_,
        UInt64 max_version_,
        size_t expected_block_size_,
        ReadMode read_mode_,
        const int extra_table_id_index,
        const TableID physical_table_id,
        const String & req_id)
        : dm_context(dm_context_)
        , task_pool(task_pool_)
        , after_segment_read(after_segment_read_)
        , columns_to_read(columns_to_read_)
        , filter(filter_)
        , header(toEmptyBlock(columns_to_read))
        , max_version(max_version_)
        , expected_block_size(expected_block_size_)
        , read_mode(read_mode_)
        , action(header, extra_table_id_index, physical_table_id)
        , log(Logger::get(req_id))
    {
        if (extra_table_id_index != InvalidColumnID)
        {
            ColumnDefine extra_table_id_col_define = getExtraTableIDColumnDefine();
            ColumnWithTypeAndName col{extra_table_id_col_define.type->createColumn(), extra_table_id_col_define.type, extra_table_id_col_define.name, extra_table_id_col_define.id, extra_table_id_col_define.default_value};
            header.insert(extra_table_id_index, col);
        }
    }

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        FilterPtr filter_ignored;
        return readImpl(filter_ignored, false);
    }

    Block readImpl(FilterPtr & res_filter, bool return_filter) override
    {
        if (done)
            return {};
        while (true)
        {
            while (!cur_stream)
            {
                auto task = task_pool->nextTask();
                if (!task)
                {
                    done = true;
                    LOG_DEBUG(log, "Read done");
                    return {};
                }
                cur_segment = task->segment;

                auto block_size = std::max(expected_block_size, static_cast<size_t>(dm_context->db_context.getSettingsRef().dt_segment_stable_pack_rows));
                cur_stream = task->segment->getInputStream(read_mode, *dm_context, columns_to_read, task->read_snapshot, task->ranges, filter, max_version, block_size);
                LOG_TRACE(log, "Start to read segment, segment={}", cur_segment->simpleInfo());
            }
            FAIL_POINT_PAUSE(FailPoints::pause_when_reading_from_dt_stream);

            Block res = cur_stream->read(res_filter, return_filter);

            if (res)
            {
                if (action.transform(res))
                {
                    return res;
                }
                else
                {
                    continue;
                }
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

    void readSuffixImpl() override
    {
        LOG_DEBUG(log, "finish read {} rows from storage", action.totalRows());
    }

private:
    DMContextPtr dm_context;
    SegmentReadTaskPoolPtr task_pool;
    AfterSegmentRead after_segment_read;
    ColumnDefines columns_to_read;
    PushDownFilterPtr filter;
    Block header;
    const UInt64 max_version;
    const size_t expected_block_size;
    const ReadMode read_mode;

    bool done = false;

    BlockInputStreamPtr cur_stream;

    SegmentPtr cur_segment;
    SegmentReadTransformAction action;

    LoggerPtr log;
};

} // namespace DM
} // namespace DB
