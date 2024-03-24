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
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Disaggregated/WNFetchPagesStreamWriter.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Interpreters/Settings.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/Remote/WNDisaggSnapshotManager.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Page/PageUtil.h>
#include <common/logger_useful.h>
#include <kvproto/mpp.pb.h>
#include <tipb/expression.pb.h>

#include <ext/scope_guard.h>
#include <memory>
#include <tuple>

using namespace DB::DM::Remote;

namespace DB
{
std::tuple<DM::RemotePb::RemotePage, size_t> WNFetchPagesStreamWriter::getPersistedRemotePage(UInt64 page_id)
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

std::tuple<UInt64, UInt64, UInt64> WNFetchPagesStreamWriter::sendMemTableSet()
{
    const auto & memtableset_snap = seg_task->read_snapshot->delta->getMemTableSetSnapshot();
    const auto & cfs = memtableset_snap->getColumnFiles();
    disaggregated::PagesPacket packet;
    UInt64 total_chunks_size = 0;
    UInt64 pending_chunks_size = 0;
    UInt64 packet_count = 0;
    for (const auto & cf : cfs)
    {
        packet.mutable_chunks()->Add(
            Serializer::serializeCF(cf, memtableset_snap->getDataProvider(), /*need_mem_data*/ true)
                .SerializeAsString());
        auto sz = packet.chunks().rbegin()->size();
        total_chunks_size += sz;
        pending_chunks_size += sz;
        mem_tracker_wrapper.alloc(sz);
        if (pending_chunks_size > packet_limit_size)
        {
            ++packet_count;
            sync_write(packet);
            packet.clear_chunks(); // Only set chunks field before.
            mem_tracker_wrapper.free(pending_chunks_size);
            pending_chunks_size = 0;
        }
    }
    if (packet.chunks_size() > 0)
    {
        ++packet_count;
        sync_write(packet);
        mem_tracker_wrapper.free(pending_chunks_size);
    }
    return std::make_tuple(cfs.size(), total_chunks_size, packet_count);
}

std::tuple<UInt64, UInt64, UInt64> WNFetchPagesStreamWriter::sendPages()
{
    disaggregated::PagesPacket packet;
    UInt64 total_pages_size = 0;
    UInt64 pending_pages_size = 0;
    UInt64 packet_count = 0;
    for (const auto page_id : read_page_ids)
    {
        auto [remote_page, page_size] = getPersistedRemotePage(page_id);
        total_pages_size += page_size;
        pending_pages_size += page_size;
        packet.mutable_pages()->Add(remote_page.SerializeAsString());
        mem_tracker_wrapper.alloc(page_size);
        if (pending_pages_size > packet_limit_size)
        {
            ++packet_count;
            sync_write(packet);
            packet.clear_pages(); // Only set pages field before.
            mem_tracker_wrapper.free(pending_pages_size);
            pending_pages_size = 0;
        }
    }
    if (packet.pages_size() > 0)
    {
        ++packet_count;
        sync_write(packet);
        mem_tracker_wrapper.free(pending_pages_size);
    }
    return std::make_tuple(read_page_ids.size(), total_pages_size, packet_count);
}

void WNFetchPagesStreamWriter::syncWrite()
{
    Stopwatch sw;
    UInt64 mem_cf_count = 0;
    UInt64 mem_cf_size = 0;
    UInt64 mem_packet_count = 0;
    if (enable_fetch_memtableset)
    {
        std::tie(mem_cf_count, mem_cf_size, mem_packet_count) = sendMemTableSet();
    }
    auto send_mem_ns = sw.elapsedFromLastTime();

    auto [page_count, page_size, page_packet_count] = sendPages();
    auto send_pages_ns = sw.elapsedFromLastTime();

    LOG_DEBUG(
        seg_task->read_snapshot->log,
        "enable_fetch_memtableset={} mem_cf_count={} mem_cf_size={} mem_packet_count={} send_mem_ms={} page_count={} "
        "page_size={} page_packet_count={} send_pages_ms={}",
        enable_fetch_memtableset,
        mem_cf_count,
        mem_cf_size,
        mem_packet_count,
        send_mem_ns / 1000000,
        page_count,
        page_size,
        page_packet_count,
        send_pages_ns / 1000000);
}

WNFetchPagesStreamWriter::WNFetchPagesStreamWriter(
    std::function<void(const disaggregated::PagesPacket &)> && sync_write_,
    DM::SegmentReadTaskPtr seg_task_,
    PageIdU64s read_page_ids_,
    const Settings & settings_)
    : sync_write(std::move(sync_write_))
    , seg_task(std::move(seg_task_))
    , read_page_ids(std::move(read_page_ids_))
    , packet_limit_size(settings_.dt_fetch_pages_packet_limit_size)
    , enable_fetch_memtableset(settings_.dt_enable_fetch_memtableset)
    , mem_tracker_wrapper(fetch_pages_mem_tracker.get())
{}

} // namespace DB
