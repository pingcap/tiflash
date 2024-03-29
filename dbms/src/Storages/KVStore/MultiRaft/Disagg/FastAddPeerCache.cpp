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

#include <Common/FailPoint.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/CheckpointInfo.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeerCache.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Utils/AsyncTasks.h>
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
#include <fmt/core.h>

namespace DB
{
[[nodiscard]] std::unique_lock<std::mutex> EndToSegmentId::lock()
{
    return std::unique_lock(mu);
}

bool EndToSegmentId::isReady(std::unique_lock<std::mutex> & lock) const
{
    UNUSED(lock);
    return is_ready;
}

UInt64 EndToSegmentId::getSegmentIdContainingKey(std::unique_lock<std::mutex> & lock, const DM::RowKeyValue & key)
{
    UNUSED(lock);
    RUNTIME_CHECK(is_ready);
    auto iter = std::upper_bound(
        end_to_segment_id.begin(),
        end_to_segment_id.end(),
        key,
        [](const DM::RowKeyValue & key1, const std::pair<DM::RowKeyValue, UInt64> & element2) {
            return key1.toRowKeyValueRef() < element2.first.toRowKeyValueRef();
        });
    RUNTIME_CHECK(
        iter != end_to_segment_id.end(),
        key.toDebugString(),
        end_to_segment_id.rbegin()->first.toDebugString());
    return iter->second;
}

void EndToSegmentId::build(
    std::unique_lock<std::mutex> & lock,
    std::vector<std::pair<DM::RowKeyValue, UInt64>> && end_key_and_segment_ids)
{
    UNUSED(lock);
    end_to_segment_id = std::move(end_key_and_segment_ids);
    is_ready = true;
}

ParsedCheckpointDataHolder::ParsedCheckpointDataHolder(
    Context & context,
    const PageStorageConfig & config,
    UInt64 dir_seq)
{
    const auto dir_prefix = fmt::format("local_{}", dir_seq);
    auto delegator = context.getPathPool().getPSDiskDelegatorGlobalMulti(dir_prefix);
    for (const auto & path : delegator->listPaths())
    {
        paths.push_back(path);
        auto file = Poco::File(path);
        if (file.exists())
        {
            LOG_WARNING(Logger::get(), "Path {} already exists, removing it", path);
            file.remove(true);
        }
    }
    auto local_ps = UniversalPageStorage::create( //
        dir_prefix,
        delegator,
        config,
        context.getFileProvider());
    local_ps->restore();
    temp_ps = local_ps;
}

UniversalPageStoragePtr ParsedCheckpointDataHolder::getUniversalPageStorage()
{
    return temp_ps;
}

EndToSegmentIdPtr ParsedCheckpointDataHolder::getEndToSegmentIdCache(const KeyspaceTableID & ks_tb_id)
{
    std::unique_lock lock(mu);
    auto iter = end_to_segment_ids.find(ks_tb_id);
    if (iter != end_to_segment_ids.end())
        return iter->second;
    else
    {
        auto cache = std::make_shared<EndToSegmentId>();
        end_to_segment_ids.emplace(ks_tb_id, cache);
        return cache;
    }
}

ParsedCheckpointDataHolderPtr buildParsedCheckpointData(Context & context, const String & manifest_key, UInt64 dir_seq)
{
    PageStorageConfig config;
    auto data_holder = std::make_shared<ParsedCheckpointDataHolder>(context, config, dir_seq);
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
        // Note the `data_file_id` in temp ps is all lock key, we need transform them to data key before write to local ps.
        for (auto & record : records)
        {
            {
                // Filter remote peer's local page storage pages.
                if (UniversalPageIdFormat::getUniversalPageIdType(record.page_id) == StorageType::LocalKV)
                {
                    continue;
                }
            }
            if (record.type == PS::V3::EditRecordType::VAR_ENTRY)
            {
                if (!record.entry.checkpoint_info.data_location.isValid())
                {
                    // Eventually fallback to regular snapshot
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "buildParsedCheckpointData: can't put remote page with empty data_location, page_id={}",
                        record.page_id);
                }
                wb.putRemotePage(
                    record.page_id,
                    record.entry.tag,
                    record.entry.size,
                    record.entry.checkpoint_info.data_location,
                    std::move(record.entry.field_offsets));
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
                if (!record.entry.checkpoint_info.data_location.isValid())
                {
                    // Eventually fallback to regular snapshot
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "buildParsedCheckpointData: can't put external page with empty data_location, page_id={}",
                        record.page_id);
                }
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
    data_holder->getUniversalPageStorage()->write(std::move(wb));
    return data_holder;
}
} // namespace DB