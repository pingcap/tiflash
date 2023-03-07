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
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Transaction/Types.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/support/sync_stream.h>
#include <kvproto/disaggregated.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
class Context;
namespace DM::Remote
{
class DisaggSnapshotManager;
}

using SyncPagePacketWriter = grpc::ServerWriter<disaggregated::PagesPacket>;

class PageTunnel;
using PageTunnelPtr = std::unique_ptr<PageTunnel>;

// A tunnel for the TiFlash write node send the delta layer data to the read node.
// Including ColumnFileTiny and ColumnFileInMemory
class PageTunnel
{
public:
    static PageTunnelPtr build(
        const Context & context,
        const DM::DisaggTaskId & task_id,
        TableID table_id,
        UInt64 segment_id,
        const PageIdU64s & read_page_ids);

    void connect(SyncPagePacketWriter * sync_writer);

    // wait until all the data has been transferred.
    void waitForFinish();

    void close();

public:
    PageTunnel(DM::DisaggTaskId task_id_,
               DM::Remote::DisaggSnapshotManager * manager,
               DM::SegmentReadTaskPtr seg_task_,
               DM::ColumnDefinesPtr column_defines_,
               std::shared_ptr<std::vector<tipb::FieldType>> result_field_types_,
               PageIdU64s read_page_ids)
        : task_id(std::move(task_id_))
        , snap_manager(manager)
        , seg_task(std::move(seg_task_))
        , column_defines(column_defines_)
        , result_field_types(std::move(result_field_types_))
        , read_page_ids(std::move(read_page_ids))
        , log(Logger::get())
    {}

    // just for test
    PageTunnel(DM::SegmentReadTaskPtr seg_task_,
               DM::ColumnDefinesPtr column_defines,
               std::shared_ptr<std::vector<tipb::FieldType>> result_field_types_,
               PageIdU64s read_page_ids)
        : PageTunnel(
            DM::DisaggTaskId::unknown_disaggregated_task_id,
            /*manager*/ nullptr,
            std::move(seg_task_),
            std::move(column_defines),
            std::move(result_field_types_),
            std::move(read_page_ids))
    {}

    // just for test
    disaggregated::PagesPacket readPacket();

private:
    const DM::DisaggTaskId task_id;
    DM::Remote::DisaggSnapshotManager * const snap_manager;
    DM::SegmentReadTaskPtr seg_task;
    DM::ColumnDefinesPtr column_defines;
    std::shared_ptr<std::vector<tipb::FieldType>> result_field_types;
    PageIdU64s read_page_ids;

    LoggerPtr log;
};

} // namespace DB
