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
#include <Storages/DeltaMerge/Remote/DisaggSnapshot_fwd.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Transaction/Types.h>
#include <kvproto/disaggregated.pb.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <grpcpp/support/sync_stream.h>
#pragma GCC diagnostic pop


namespace DB
{
using SyncPagePacketWriter = grpc::ServerWriter<disaggregated::PagesPacket>;

class WNFetchPagesStreamWriter;
using WNFetchPagesStreamWriterPtr = std::unique_ptr<WNFetchPagesStreamWriter>;

/**
 * A writer in TiFlash write node, who sends the delta layer data to the read node in a streaming way.
 * It writes ColumnFileTiny and ColumnFileInMemory.
 * This writer is used for responding the FetchPages request.
 */
class WNFetchPagesStreamWriter
{
public:
    static WNFetchPagesStreamWriterPtr build(
        const DM::Remote::SegmentPagesFetchTask & task,
        const PageIdU64s & read_page_ids);

    void pipeTo(SyncPagePacketWriter * sync_writer);

private:
    WNFetchPagesStreamWriter(
        DM::SegmentReadTaskPtr seg_task_,
        DM::ColumnDefinesPtr column_defines_,
        std::shared_ptr<std::vector<tipb::FieldType>> result_field_types_,
        PageIdU64s read_page_ids)
        : seg_task(std::move(seg_task_))
        , column_defines(column_defines_)
        , result_field_types(std::move(result_field_types_))
        , read_page_ids(std::move(read_page_ids))
        , log(Logger::get())
    {}

    /// Returns the next packet that could write to the response sink.
    disaggregated::PagesPacket nextPacket();

private:
    const DM::DisaggTaskId task_id;
    DM::SegmentReadTaskPtr seg_task;
    DM::ColumnDefinesPtr column_defines;
    std::shared_ptr<std::vector<tipb::FieldType>> result_field_types;
    PageIdU64s read_page_ids;

    LoggerPtr log;
};

} // namespace DB
