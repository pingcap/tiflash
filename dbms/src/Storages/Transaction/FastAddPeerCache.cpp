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

#include <Common/FailPoint.h>
#include <Encryption/PosixRandomAccessFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/Universal/RaftDataReader.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3GCManager.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <Storages/Transaction/CheckpointInfo.h>
#include <Storages/Transaction/FastAddPeerAsyncTasksImpl.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <fmt/core.h>

namespace DB
{
UInt64 SegmentEndKeyCache::getSegmentIdContainingKey(const DM::RowKeyValue & key)
{
    std::unique_lock lock(mu);
    if (!is_ready)
        cv.wait(lock, [&] { return is_ready; });
    auto iter = end_key_to_segment_id.upper_bound(key);
    RUNTIME_CHECK(iter != end_key_to_segment_id.end(), key.toDebugString(), end_key_to_segment_id.rbegin()->first.toDebugString());
    return iter->second;
}

void SegmentEndKeyCache::build(const std::vector<std::pair<DM::RowKeyValue, UInt64>> & end_key_and_segment_ids)
{
    {
        std::unique_lock lock(mu);
        for (const auto & [end_key, segment_id] : end_key_and_segment_ids)
        {
            end_key_to_segment_id[end_key] = segment_id;
        }
        is_ready = true;
    }
    cv.notify_all();
}

std::pair<SegmentEndKeyCachePtr, bool> ParsedCheckpointDataHolder::getSegmentEndKeyCache(const TableIdentifier & identifier)
{
    std::unique_lock lock(mu);
    auto iter = segment_end_key_cache_map.find(identifier);
    if (iter != segment_end_key_cache_map.end())
        return std::make_pair(iter->second, false);
    auto cache = std::make_shared<SegmentEndKeyCache>();
    segment_end_key_cache_map.emplace(identifier, cache);
    return std::make_pair(cache, true);
}

ParsedCheckpointDataHolderPtr buildParsedCheckpointData(Context & context, const String & manifest_key, UInt64 dir_seq)
{
    auto file_provider = context.getFileProvider();
    PageStorageConfig config;
    const auto dir_prefix = fmt::format("local_{}", dir_seq);
    auto data_holder = std::make_shared<ParsedCheckpointDataHolder>();
    auto delegator = context.getPathPool().getPSDiskDelegatorGlobalMulti(dir_prefix);
    for (const auto & path : delegator->listPaths())
    {
        data_holder->paths.push_back(path);
        auto file = Poco::File(path);
        if (file.exists())
        {
            LOG_WARNING(Logger::get("createTempPageStorage"), "Path {} already exists, removing it", path);
            file.remove(true);
        }
    }
    auto local_ps = UniversalPageStorage::create( //
        dir_prefix,
        delegator,
        config,
        file_provider);
    local_ps->restore();
    data_holder->temp_ps = local_ps;
    auto * log = &Poco::Logger::get("FastAddPeer");
    LOG_DEBUG(log, "Begin to create temp ps from {}", manifest_key);

    RandomAccessFilePtr manifest_file = S3::S3RandomAccessFile::create(manifest_key);
    auto reader = PS::V3::CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    auto im = PS::V3::CheckpointProto::StringsInternMap{};
    auto prefix = reader->readPrefix();
    UniversalWriteBatch wb;
    wb.disableRemoteLock();
    // insert delete records at last
    PS::V3::PageEntriesEdit<UniversalPageId>::EditRecords ref_records;
    PS::V3::PageEntriesEdit<UniversalPageId>::EditRecords delete_records;
    while (true)
    {
        auto edits = reader->readEdits(im);
        if (!edits.has_value())
            break;
        auto records = edits->getRecords();
        for (auto & record : records)
        {
            if (record.type == PS::V3::EditRecordType::VAR_ENTRY)
            {
                wb.putRemotePage(record.page_id, record.entry.tag, record.entry.checkpoint_info.data_location, std::move(record.entry.field_offsets));
            }
            else if (record.type == PS::V3::EditRecordType::VAR_REF)
            {
                ref_records.emplace_back(record);
            }
            else if (record.type == PS::V3::EditRecordType::VAR_DELETE)
            {
                delete_records.emplace_back(record);
            }
            else if (record.type == PS::V3::EditRecordType::VAR_EXTERNAL)
            {
                RUNTIME_CHECK(record.entry.checkpoint_info.has_value());
                wb.putRemoteExternal(record.page_id, record.entry.checkpoint_info.data_location);
            }
            else
            {
                RUNTIME_CHECK(false);
            }
        }
    }

    for (const auto & record : ref_records)
    {
        RUNTIME_CHECK(record.type == PS::V3::EditRecordType::VAR_REF);
        wb.putRefPage(record.page_id, record.ori_page_id);
    }
    for (const auto & record : delete_records)
    {
        RUNTIME_CHECK(record.type == PS::V3::EditRecordType::VAR_DELETE);
        wb.delPage(record.page_id);
    }
    local_ps->write(std::move(wb));
    return data_holder;
}
} // namespace DB