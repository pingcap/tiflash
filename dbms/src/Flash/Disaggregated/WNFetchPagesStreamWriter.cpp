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
#include <Flash/Disaggregated/WNFetchPagesStreamWriter.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/Remote/WNDisaggSnapshotManager.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Page/PageUtil.h>
#include <common/logger_useful.h>
#include <kvproto/mpp.pb.h>
#include <tipb/expression.pb.h>

#include <memory>

namespace DB
{
WNFetchPagesStreamWriterPtr WNFetchPagesStreamWriter::build(
    const DM::Remote::SegmentPagesFetchTask & task,
    const PageIdU64s & read_page_ids)
{
    return std::unique_ptr<WNFetchPagesStreamWriter>(new WNFetchPagesStreamWriter(
        task.seg_task,
        task.column_defines,
        task.output_field_types,
        read_page_ids));
}

disaggregated::PagesPacket WNFetchPagesStreamWriter::nextPacket()
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

    // TODO: Currently the memtable data is responded in the Establish stage, instead of in the FetchPages stage.
    //       We could improve it to respond in the FetchPages stage, so that the parallel FetchPages could start
    //       as soon as possible.

    LOG_DEBUG(log,
              "Send FetchPagesStream, pages={} pages_size={} blocks={}",
              packet.pages_size(),
              total_pages_data_size,
              packet.chunks_size());
    return packet;
}

void WNFetchPagesStreamWriter::pipeTo(SyncPagePacketWriter * sync_writer)
{
    // Currently we only send one stream packet.
    // TODO: split the packet into smaller size
    sync_writer->Write(nextPacket());
}


} // namespace DB
