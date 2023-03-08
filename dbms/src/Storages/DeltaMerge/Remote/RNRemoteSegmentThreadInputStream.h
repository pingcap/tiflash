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

#include <Core/Defines.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/SegmentReadTransformAction.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Remote/RNRemoteReadTask.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/DeltaMerge/StableValueSpace.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace DB
{
class RNPagePreparer;
using RNPagePreparerPtr = std::shared_ptr<RNPagePreparer>;
namespace DM
{

/// An input stream used for TiFlash read node to read blocks
/// from an RNRemoteReadTask.
class RNRemoteSegmentThreadInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "RemoteSegmentThread";

public:
    static BlockInputStreams buildInputStreams(
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
        size_t expected_block_size = DEFAULT_BLOCK_SIZE);

    RNRemoteSegmentThreadInputStream(
        const Context & db_context_,
        RNRemoteReadTaskPtr read_tasks_,
        RNPagePreparerPtr page_downloader_,
        const ColumnDefines & columns_to_read_,
        const RSOperatorPtr & filter_,
        UInt64 max_version_,
        size_t expected_block_size_,
        ReadMode read_mode_,
        int extra_table_id_index_,
        std::string_view req_id);

    ~RNRemoteSegmentThreadInputStream() override;

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        FilterPtr filter_ignored;
        return readImpl(filter_ignored, false);
    }

    Block readImpl(FilterPtr & res_filter, bool return_filter) override;

private:
    const Context & db_context;
    RNRemoteReadTaskPtr read_tasks;
    RNPagePreparerPtr page_downloader;
    ColumnDefines columns_to_read;
    RSOperatorPtr filter;
    Block header;
    const UInt64 max_version;
    const size_t expected_block_size;
    const ReadMode read_mode;
    const int extra_table_id_index;
    TableID physical_table_id;

    Stopwatch watch;
    double seconds_pop;
    double seconds_build;

    size_t total_rows = 0;
    bool done = false;

    BlockInputStreamPtr cur_stream;
    UInt64 cur_segment_id;

    LoggerPtr log;
};

} // namespace DM
} // namespace DB
