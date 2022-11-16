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
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

#include "Common/Stopwatch.h"
#include "common/types.h"

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
        const RSOperatorPtr & filter_,
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
        , extra_table_id_index(extra_table_id_index)
        , physical_table_id(physical_table_id)
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
                Stopwatch sw;
                cur_segment = task->segment;
                if (is_raw)
                {
                    cur_stream = cur_segment->getInputStreamRaw(
                        *dm_context,
                        columns_to_read,
                        task->read_snapshot,
                        task->ranges,
                        filter,
                        do_delete_mark_filter_for_raw);
                }
                else
                {
                    cur_stream = cur_segment->getInputStream(
                        *dm_context,
                        columns_to_read,
                        task->read_snapshot,
                        task->ranges,
                        filter,
                        max_version,
                        std::max(expected_block_size, static_cast<size_t>(dm_context->db_context.getSettingsRef().dt_segment_stable_pack_rows)));
                }
                get_input_stream_ms += sw.elapsedMilliseconds();
                read_segment_count++;
                LOG_TRACE(log, "Start to read segment [{}]", cur_segment->segmentId());
            }
            FAIL_POINT_PAUSE(FailPoints::pause_when_reading_from_dt_stream);

            Block res = cur_stream->read(res_filter, return_filter);

            if (res)
            {
                if (extra_table_id_index != InvalidColumnID)
                {
                    ColumnDefine extra_table_id_col_define = getExtraTableIDColumnDefine();
                    ColumnWithTypeAndName col{{}, extra_table_id_col_define.type, extra_table_id_col_define.name, extra_table_id_col_define.id};
                    size_t row_number = res.rows();
                    auto col_data = col.type->createColumnConst(row_number, Field(physical_table_id));
                    col.column = std::move(col_data);
                    res.insert(extra_table_id_index, std::move(col));
                }
                if (!res.rows())
                    continue;
                else
                {
                    total_rows += res.rows();
                    return res;
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
        auto elapsed_ms = total_sw.elapsedMilliseconds();
        LOG_DEBUG(log, "finish read {} rows from storage, read time {} ms, read segment count {}, read time per segment {} ms, get input stream time {} ms, get input stream time per segment {} ms", total_rows, elapsed_ms, read_segment_count, elapsed_ms / read_segment_count, get_input_stream_ms, get_input_stream_ms / read_segment_count);
    }

private:
    DMContextPtr dm_context;
    SegmentReadTaskPoolPtr task_pool;
    AfterSegmentRead after_segment_read;
    ColumnDefines columns_to_read;
    RSOperatorPtr filter;
    Block header;
    const UInt64 max_version;
    const size_t expected_block_size;
    const ReadMode read_mode;
    // position of the ExtraPhysTblID column in column_names parameter in the StorageDeltaMerge::read function.
    const int extra_table_id_index;

    bool done = false;

    BlockInputStreamPtr cur_stream;

    SegmentPtr cur_segment;
    TableID physical_table_id;

    LoggerPtr log;
    size_t total_rows = 0;

    Stopwatch total_sw;
    UInt64 read_segment_count{0};
    UInt64 get_input_stream_ms{0};
};

} // namespace DM
} // namespace DB
