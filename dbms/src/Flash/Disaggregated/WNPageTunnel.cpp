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

#include <Common/Exception.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Disaggregated/WNPageTunnel.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshotManager.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>
#include <kvproto/mpp.pb.h>
#include <tipb/expression.pb.h>

#include <memory>

namespace DB
{
WNPageTunnelPtr WNPageTunnel::build(
    const Context & context,
    const DM::DisaggTaskId & task_id,
    TableID table_id,
    UInt64 segment_id,
    const PageIdU64s & read_page_ids)
{
    auto & tmt = context.getTMTContext();
    auto * snap_manager = tmt.getDisaggSnapshotManager();
    auto snap = snap_manager->getSnapshot(task_id);
    RUNTIME_CHECK_MSG(snap != nullptr, "Can not find disaggregated task, task_id={}", task_id);
    auto task = snap->popSegTask(table_id, segment_id);
    RUNTIME_CHECK(task.isValid(), task.err_msg);

    auto tunnel = std::make_unique<WNPageTunnel>(
        task_id,
        snap_manager,
        task.seg_task,
        task.column_defines,
        task.output_field_types,
        read_page_ids);
    return tunnel;
}

disaggregated::PagesPacket WNPageTunnel::readPacket()
{
    // TODO: the returned rows should respect max_rows_per_chunk

    disaggregated::PagesPacket packet;

    // read page data by page_ids
    size_t total_pages_data_size = 0;
    auto persisted_cf = seg_task->read_snapshot->delta->getPersistedFileSetSnapshot();
    for (const auto page_id : read_page_ids)
    {
        auto page = persisted_cf->getDataProvider()->readTinyData(page_id);
        DM::RemotePb::RemotePage remote_page;
        remote_page.set_page_id(page_id);
        remote_page.mutable_data()->assign(page.data.begin(), page.data.end());
        const auto field_sizes = PageUtil::getFieldSizes(page.field_offsets, page.data.size());
        for (const auto field_sz : field_sizes)
        {
            remote_page.add_field_sizes(field_sz);
        }
        total_pages_data_size += page.data.size();
        packet.mutable_pages()->Add(remote_page.SerializeAsString());
    }

    // generate an input stream of mem-table
#if 0
    // TODO: Currently the memtable data is responded in the 1st response. Serializer::serializeTo(const ColumnFileInMemory & cf_in_mem)
    if (false) 
    {
        auto chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(*result_field_types);
        auto delta_vs = seg_task->read_snapshot->delta;
        UNUSED(chunk_codec_stream, delta_vs);
        auto mem_table_stream = std::make_shared<DM::DeltaMemTableInputStream>(delta_vs, column_defines, seg_task->segment->getRowKeyRange());
        mem_table_stream->readPrefix();
        while (true)
        {
            Block block = mem_table_stream->read();
            if (!block)
                break;
            chunk_codec_stream->encode(block, 0, block.rows());
            // serialize block as chunk
            packet.add_chunks(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
        mem_table_stream->readSuffix();
    }
#endif

    LOG_DEBUG(log,
              "send packet, pages={} pages_size={} blocks={}",
              packet.pages_size(),
              total_pages_data_size,
              packet.chunks_size());
    return packet;
}

void WNPageTunnel::connect(SyncPagePacketWriter * sync_writer)
{
    // TODO: split the packet into smaller size
    sync_writer->Write(readPacket());
}

void WNPageTunnel::waitForFinish()
{
}

void WNPageTunnel::close()
{
    if (!snap_manager)
        return;

    if (auto snap = snap_manager->getSnapshot(task_id);
        snap && snap->empty())
    {
        snap_manager->unregisterSnapshot(task_id);
        LOG_DEBUG(log, "release snapshot, task_id={}", task_id);
    }
}
} // namespace DB
