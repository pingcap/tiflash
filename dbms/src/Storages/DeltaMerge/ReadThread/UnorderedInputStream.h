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
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB::FailPoints
{
extern const char pause_when_reading_from_dt_stream[];
}

namespace DB::DM
{
class UnorderedInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "UnorderedInputStream";

public:
    UnorderedInputStream(
        const SegmentReadTaskPoolPtr & task_pool_,
        const ColumnDefines & columns_to_read_,
        const int extra_table_id_index,
        const TableID physical_table_id,
        const String & req_id)
        : task_pool(task_pool_)
        , header(toEmptyBlock(columns_to_read_))
        , extra_table_id_index(extra_table_id_index)
        , physical_table_id(physical_table_id)
        , log(Logger::get(NAME, req_id))
    {
        if (extra_table_id_index != InvalidColumnID)
        {
            ColumnDefine extra_table_id_col_define = getExtraTableIDColumnDefine();
            ColumnWithTypeAndName col{extra_table_id_col_define.type->createColumn(), extra_table_id_col_define.type, extra_table_id_col_define.name, extra_table_id_col_define.id, extra_table_id_col_define.default_value};
            header.insert(extra_table_id_index, col);
        }
        task_pool->increaseUnorderedInputStreamRefCount();
    }

    ~UnorderedInputStream()
    {
        task_pool->decreaseUnorderedInputStreamRefCount();
    }

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        FilterPtr filter_ignored;
        return readImpl(filter_ignored, false);
    }

    // TODO(jinhelin): res_fiter, return_filter, after_segment_read
    Block readImpl(FilterPtr & /*res_filter*/, bool /*return_filter*/) override
    {
        if (done)
        {
            return {};
        }
        while (true)
        {
            FAIL_POINT_PAUSE(FailPoints::pause_when_reading_from_dt_stream);

            Block res;
            task_pool->popBlock(res);
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
                {
                    continue;
                }
                else
                {
                    total_rows += res.rows();
                    return res;
                }
            }
            else
            {
                done = true;
                return {};
            }
        }
    }

    void readSuffixImpl() override
    {
        LOG_FMT_DEBUG(log, "finish read {} rows from storage", total_rows);
    }

private:
    SegmentReadTaskPoolPtr task_pool;
    Block header;
    // position of the ExtraPhysTblID column in column_names parameter in the StorageDeltaMerge::read function.
    const int extra_table_id_index;
    bool done = false;
    TableID physical_table_id;
    LoggerPtr log;
    size_t total_rows = 0;
};
}