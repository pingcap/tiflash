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
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/PageDefinesBase.h>
#include <kvproto/disaggregated.pb.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <grpcpp/support/sync_stream.h>
#pragma GCC diagnostic pop


namespace DB
{

struct Settings;

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
    WNFetchPagesStreamWriter(
        std::function<void(const disaggregated::PagesPacket &)> && sync_write_,
        DM::SegmentReadTaskPtr seg_task_,
        PageIdU64s read_page_ids_,
        const Settings & settings_);

    void syncWrite();

private:
    [[nodiscard]] std::tuple<DM::RemotePb::RemotePage, size_t> getPersistedRemotePage(UInt64 page_id);
    [[nodiscard]] std::tuple<UInt64, UInt64, UInt64> sendMemTableSet();
    [[nodiscard]] std::tuple<UInt64, UInt64, UInt64> sendPages();

private:
    std::function<void(const disaggregated::PagesPacket &)> sync_write;
    DM::SegmentReadTaskPtr seg_task;
    PageIdU64s read_page_ids;
    UInt64 packet_limit_size;
    bool enable_fetch_memtableset;
    MemTrackerWrapper mem_tracker_wrapper;
};

} // namespace DB
