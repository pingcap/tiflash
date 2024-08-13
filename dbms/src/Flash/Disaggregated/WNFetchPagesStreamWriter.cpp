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

#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Flash/Disaggregated/WNFetchPagesStreamWriter.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/WNDisaggSnapshotManager.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Page/PageUtil.h>
#include <common/logger_useful.h>

#include <memory>

namespace DB
{
WNFetchPagesStreamWriterPtr WNFetchPagesStreamWriter::build(
    const DM::Remote::SegmentPagesFetchTask & task,
    const PageIdU64s & read_page_ids,
    UInt64 packet_limit_size)
{
    return std::unique_ptr<WNFetchPagesStreamWriter>(
        new WNFetchPagesStreamWriter(task.seg_task, task.column_defines, read_page_ids, packet_limit_size));
}

std::pair<DM::RemotePb::RemotePage, size_t> WNFetchPagesStreamWriter::getPersistedRemotePage(UInt64 page_id)
{
    auto page = seg_task->read_snapshot->delta->getPersistedFileSetSnapshot()->getDataProvider()->readTinyData(page_id);
    DM::RemotePb::RemotePage remote_page;
    remote_page.set_page_id(page_id);
    remote_page.mutable_data()->assign(page.data.begin(), page.data.end());
    const auto field_sizes = PageUtil::getFieldSizes(page.field_offsets, page.data.size());
    for (const auto field_sz : field_sizes)
    {
        remote_page.add_field_sizes(field_sz);
    }
    return {remote_page, page.data.size()};
}

void WNFetchPagesStreamWriter::pipeTo(SyncPagePacketWriter * sync_writer)
{
    disaggregated::PagesPacket packet;
    MemTrackerWrapper packet_mem_tracker_wrapper(fetch_pages_mem_tracker.get());
    UInt64 total_pages_data_size = 0;
    UInt64 packet_count = 0;
    UInt64 pending_pages_data_size = 0;
    UInt64 read_page_ns = 0;
    UInt64 send_page_ns = 0;
    for (const auto page_id : read_page_ids)
    {
        Stopwatch sw_packet;
        auto [remote_page, page_size] = getPersistedRemotePage(page_id);
        total_pages_data_size += page_size;
        pending_pages_data_size += page_size;
        packet.mutable_pages()->Add(remote_page.SerializeAsString());
        packet_mem_tracker_wrapper.alloc(page_size);
        read_page_ns += sw_packet.elapsedFromLastTime();

        if (pending_pages_data_size > packet_limit_size)
        {
            ++packet_count;
            sync_writer->Write(packet);
            send_page_ns += sw_packet.elapsedFromLastTime();
            pending_pages_data_size = 0;
            packet.clear_pages(); // Only set pages field before.
            packet_mem_tracker_wrapper.freeAll();
        }
    }

    if (packet.pages_size() > 0)
    {
        Stopwatch sw;
        ++packet_count;
        sync_writer->Write(packet);
        send_page_ns += sw.elapsedFromLastTime();
    }

    // TODO: Currently the memtable data is responded in the Establish stage, instead of in the FetchPages stage.
    //       We could improve it to respond in the FetchPages stage, so that the parallel FetchPages could start
    //       as soon as possible.

    LOG_DEBUG(
        log,
        "Send FetchPagesStream, pages={} pages_size={} blocks={} packets={} read_page_ms={} send_page_ms={}",
        read_page_ids.size(),
        total_pages_data_size,
        packet.chunks_size(),
        packet_count,
        read_page_ns / 1000000,
        send_page_ns / 1000000);
}


} // namespace DB
