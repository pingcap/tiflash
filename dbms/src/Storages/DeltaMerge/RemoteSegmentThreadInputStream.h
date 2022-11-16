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

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace DB::DM
{

class RemoteSegmentThreadInpuStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "RemoteSegmentThread";

public:
    RemoteSegmentThreadInpuStream(
        const DMContextPtr & dm_context_,
        const RemoteReadTaskPtr read_tasks_,
        const ColumnDefines & columns_to_read_,
        const RSOperatorPtr & filter_,
        UInt64 max_version_,
        size_t expected_block_size_,
        ReadMode read_mode_,
        const int extra_table_id_index_,
        const TableID physical_table_id_,
        const String & req_id)
        : dm_context(dm_context_)
        , read_tasks(read_tasks_)
        , columns_to_read(columns_to_read_)
        , filter(filter_)
        , header(toEmptyBlock(columns_to_read))
        , max_version(max_version_)
        , expected_block_size(std::max(expected_block_size_, static_cast<size_t>(dm_context->db_context.getSettingsRef().dt_segment_stable_pack_rows)))
        , read_mode(read_mode_)
        , extra_table_id_index(extra_table_id_index_)
        , physical_table_id(physical_table_id_)
        , cur_segment_id(0)
        , log(Logger::get(req_id))
    {
        // TODO: abstract for this class/DMSegmentThreadInputStream/UnorderedInputStream
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
                auto task = read_tasks->nextTask();
                if (!task)
                {
                    done = true;
                    LOG_DEBUG(log, "Read from remote done");
                    return {};
                }
                cur_segment_id = task->segment->segmentId();
                // TODO:
                cur_stream = task->segment->getInputStream(
                    read_mode,
                    *dm_context,
                    columns_to_read,
                    task->segment_snap,
                    task->ranges,
                    filter,
                    max_version,
                    expected_block_size);
                LOG_TRACE(log, "Start to read segment, segment={}", cur_segment_id);
            }

            Block res = cur_stream->read(res_filter, return_filter);
            if (!res)
            {
                LOG_TRACE(log, "Finish reading segment, segment={}", cur_segment_id);
                cur_segment_id = 0;
                cur_stream = {};
                // try read from next task
                continue;
            }

            // TODO: abstract for this class/DMSegmentThreadInputStream/UnorderedInputStream
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
            {
                // try read from next task
                continue;
            }
            else
            {
                total_rows += res.rows();
                return res;
            }
        }
    }

private:
    DMContextPtr dm_context;
    RemoteReadTaskPtr read_tasks;
    ColumnDefines columns_to_read;
    RSOperatorPtr filter;
    Block header;
    const UInt64 max_version;
    const size_t expected_block_size;
    const ReadMode read_mode;
    const int extra_table_id_index;
    const TableID physical_table_id;

    size_t total_rows = 0;
    bool done = false;

    BlockInputStreamPtr cur_stream;
    UInt64 cur_segment_id;

    LoggerPtr log;
};

} // namespace DB::DM
