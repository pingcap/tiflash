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

#include <Common/TiFlashException.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/SegmentReadTransformAction.h>
#include <Flash/Coprocessor/ChunkDecodeAndSquash.h>
#include <Flash/Disaggregated/RNPageReceiver.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Remote/RNRemoteSegmentThreadInputStream.h>

#include <magic_enum.hpp>
#include <memory>

namespace DB::DM
{
BlockInputStreams RNRemoteSegmentThreadInputStream::buildInputStreams(
    const Context & db_context,
    const RNRemoteReadTaskPtr & remote_read_tasks,
    const RNPagePreparerPtr & page_downloader,
    const DM::ColumnDefinesPtr & columns_to_read,
    UInt64 read_tso,
    size_t num_streams,
    size_t extra_table_id_index,
    DM::RSOperatorPtr rs_filter,
    std::string_view extra_info,
    std::string_view tracing_id,
    size_t expected_block_size)
{
    BlockInputStreams streams;
    streams.reserve(num_streams);
    for (size_t i = 0; i < num_streams; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<DM::RNRemoteSegmentThreadInputStream>(
            db_context,
            remote_read_tasks,
            page_downloader,
            *columns_to_read,
            rs_filter,
            read_tso,
            expected_block_size,
            DM::ReadMode::Normal,
            extra_table_id_index,
            tracing_id);
        stream->setExtraInfo(String(extra_info));
        streams.emplace_back(std::move(stream));
    }
    return streams;
}

RNRemoteSegmentThreadInputStream::RNRemoteSegmentThreadInputStream(
    const Context & db_context_,
    RNRemoteReadTaskPtr read_tasks_,
    RNPagePreparerPtr page_downloader_,
    const ColumnDefines & columns_to_read_,
    const RSOperatorPtr & filter_,
    UInt64 max_version_,
    size_t expected_block_size_,
    ReadMode read_mode_,
    int extra_table_id_index_,
    std::string_view req_id)
    : db_context(db_context_)
    , read_tasks(std::move(read_tasks_))
    , page_downloader(std::move(page_downloader_))
    , columns_to_read(columns_to_read_)
    , filter(filter_)
    , header(toEmptyBlock(columns_to_read))
    , max_version(max_version_)
    , expected_block_size(std::max(expected_block_size_, static_cast<size_t>(db_context.getSettingsRef().dt_segment_stable_pack_rows)))
    , read_mode(read_mode_)
    , extra_table_id_index(extra_table_id_index_)
    , physical_table_id(-1)
    , seconds_pop(0.0)
    , seconds_build(0.0)
    , cur_segment_id(0)
    , log(Logger::get(String(req_id)))
{
    if (extra_table_id_index != InvalidColumnID)
    {
        ColumnDefine extra_table_id_col_define = getExtraTableIDColumnDefine();
        ColumnWithTypeAndName col{extra_table_id_col_define.type->createColumn(), extra_table_id_col_define.type, extra_table_id_col_define.name, extra_table_id_col_define.id, extra_table_id_col_define.default_value};
        header.insert(extra_table_id_index, col);
    }
}

RNRemoteSegmentThreadInputStream::~RNRemoteSegmentThreadInputStream()
{
    LOG_INFO(log, "RNRemoteSegmentThreadInputStream done, time blocked in pop task: {:.3f}sec, build task: {:.3f}sec", seconds_pop, seconds_build);
    GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_pop_ready_tasks).Observe(seconds_pop);
    GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_build_stream).Observe(seconds_build);
}

Block RNRemoteSegmentThreadInputStream::readImpl(FilterPtr & res_filter, bool return_filter)
{
    if (done)
        return {};
    while (true)
    {
        while (!cur_stream)
        {
            watch.restart();
            auto task = read_tasks->nextReadyTask();
            seconds_pop += watch.elapsedSeconds();
            watch.restart();
            if (!task)
            {
                // There is no task left or error happen
                done = true;
                if (!read_tasks->getErrorMessage().empty())
                {
                    throw Exception(read_tasks->getErrorMessage(), ErrorCodes::LOGICAL_ERROR);
                }
                LOG_DEBUG(log, "Read from remote segment done");
                return {};
            }

            // Note that the segment task could come from different physical tables
            cur_segment_id = task->segment_id;
            physical_table_id = task->table_id;
            UNUSED(read_mode); // TODO: support more read mode
            cur_stream = task->getInputStream(
                columns_to_read,
                task->getReadRanges(),
                max_version,
                filter,
                expected_block_size);
            seconds_build += watch.elapsedSeconds();
            LOG_TRACE(log, "Read blocks from remote segment begin, segment={} state={}", cur_segment_id, magic_enum::enum_name(task->state));
        }

        Block res = cur_stream->read(res_filter, return_filter);
        if (!res)
        {
            LOG_TRACE(log, "Read blocks from remote segment end, segment={}", cur_segment_id);
            cur_segment_id = 0;
            cur_stream = {};
            // try read from next task
            continue;
        }

        // TODO: replace by SegmentReadTransformAction
        if (extra_table_id_index != InvalidColumnID)
        {
            assert(physical_table_id != -1);

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

} // namespace DB::DM
