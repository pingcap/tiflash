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

#include <Storages/Page/V3/Universal/RaftDataReader.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>

namespace DB
{
// Proxy may try to scan data in range [start, end), and kv engine may specify an empty `end` which means infinite end.(Raft engine will always specify a non-empty `end` key)
// But we don't want to scan data unrelated to it.
// We notice that we prepend keys written by kv engine with the byte `0x02`
// so we will manually set an end value `0x03` when proxy pass an empty end key for range scan.
char RaftDataReader::raft_data_end_key[1] = {0x03};

Page RaftDataReader::read(const UniversalPageId & page_id)
{
    auto snapshot = uni_ps.getSnapshot(fmt::format("read_r_{}", page_id));
    return uni_ps.read(page_id, nullptr, snapshot, /*throw_on_not_exist*/ false);
}

void RaftDataReader::traverse(const UniversalPageId & start, const UniversalPageId & end, const std::function<void(const UniversalPageId & page_id, DB::Page page)> & acceptor)
{
    auto transformed_end = end.empty() ? UniversalPageId(raft_data_end_key, 1) : end;
    auto snapshot = uni_ps.getSnapshot(fmt::format("scan_r_{}_{}", start, transformed_end));
    const auto page_ids = uni_ps.page_directory->getAllPageIdsInRange(start, transformed_end, snapshot);
    for (const auto & page_id : page_ids)
    {
        const auto page_id_and_entry = uni_ps.page_directory->getByID(page_id, snapshot);
        acceptor(page_id, uni_ps.blob_store->read(page_id_and_entry));
    }
}

void RaftDataReader::traverseRaftLogForRegion(UInt64 region_id, const std::function<void(const UniversalPageId & page_id, DB::Page page)> & acceptor)
{
    auto start = UniversalPageIdFormat::toFullRaftLogPrefix(region_id);
    auto end = UniversalPageIdFormat::toFullRaftLogPrefix(region_id + 1);
    traverse(start, end, [&](const UniversalPageId & page_id, const DB::Page & page) {
        if (page_id.hasPrefix(start))
            acceptor(page_id, page);
    });
}

std::optional<UniversalPageId> RaftDataReader::getLowerBound(const UniversalPageId & page_id)
{
    auto snapshot = uni_ps.getSnapshot(fmt::format("lower_bound_r_{}", page_id));
    return uni_ps.page_directory->getLowerBound(page_id, snapshot);
}
} // namespace DB
