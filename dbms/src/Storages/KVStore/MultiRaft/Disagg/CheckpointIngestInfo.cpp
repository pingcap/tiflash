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

#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/CheckpointIngestInfo.h>
#include <Storages/KVStore/MultiRaft/RegionPersister.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/StorageDeltaMerge.h>

namespace DB
{

static constexpr uint8_t FAP_INGEST_INFO_PERSIST_FMT_VER = 1;

CheckpointIngestInfoPtr CheckpointIngestInfo::restore(
    TMTContext & tmt,
    const TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id,
    UInt64 peer_id,
    bool allow_segment_noexist)
{
    StoreID remote_store_id;
    RegionPtr region;
    DM::Segments restored_segments;

    auto uni_ps = tmt.getContext().getWriteNodePageStorage();
    auto snapshot = uni_ps->getSnapshot(fmt::format("read_fap_i_{}", region_id));
    auto page_id
        = UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestInfo, region_id);
    Page page = uni_ps->read(page_id, nullptr, snapshot, /*throw_on_not_exist*/ false);
    if (!page.isValid())
    {
        // The restore failed, we can safely return null here to make FAP fallback.
        return nullptr;
    }
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    RUNTIME_CHECK_MSG(readBinary2<UInt8>(buf) == FAP_INGEST_INFO_PERSIST_FMT_VER, "wrong fap ingest info format");

    std::vector<UInt64> restored_segments_id;
    {
        auto count = readBinary2<UInt64>(buf);
        for (size_t i = 0; i < count; ++i)
        {
            auto segment_id = readBinary2<UInt64>(buf);
            restored_segments_id.push_back(segment_id);
        }
        remote_store_id = readBinary2<UInt64>(buf);
    }
    region = Region::deserialize(buf, proxy_helper);

    auto & storages = tmt.getStorages();
    auto keyspace_id = region->getKeyspaceID();
    auto table_id = region->getMappedTableID();
    auto storage = storages.get(keyspace_id, table_id);

    auto log = DB::Logger::get("CheckpointIngestInfo");
    if (storage && storage->engineType() == TiDB::StorageEngine::DT)
    {
        auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
        auto dm_context = dm_storage->getStore()->newDMContext(tmt.getContext(), tmt.getContext().getSettingsRef());

        for (auto segment_id : restored_segments_id)
        {
            try
            {
                restored_segments.emplace_back(DM::Segment::restoreSegment(log, *dm_context, segment_id));
            }
            catch (const Exception & e)
            {
                if (!allow_segment_noexist)
                {
                    e.rethrow();
                }
                else
                {
                    LOG_INFO(
                        log,
                        "Can't restore segment when clean FAP data, segment_id={} region_id={}",
                        segment_id,
                        region_id);
                    continue;
                }
            }
        }

        LOG_INFO(
            log,
            "CheckpointIngestInfo restore success with {} segments, region_id={} table_id={} keyspace_id={} region={}",
            restored_segments.size(),
            region_id,
            table_id,
            keyspace_id,
            region->getDebugString());
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unsupported storage engine");
    }

    return std::make_shared<CheckpointIngestInfo>(
        tmt,
        region_id,
        peer_id,
        remote_store_id,
        region,
        std::move(restored_segments),
        /*begin_time_*/ 0);
}

void CheckpointIngestInfo::persistToLocal() const
{
    if (region->isPendingRemove())
    {
        // A pending remove region should not be selected as candidate.
        LOG_ERROR(log, "candidate region {} is pending remove", region->toString(false));
        return;
    }
    auto uni_ps = tmt.getContext().getWriteNodePageStorage();
    UniversalWriteBatch wb;
    MemoryWriteBuffer wb_buffer;
    // Write:
    // - The region, which is actually data and meta in KVStore.
    // - The segment ids point to segments which are already persisted but not ingested.
    static_assert(sizeof(FAP_INGEST_INFO_PERSIST_FMT_VER) == 1);
    auto data_size = writeBinary2(FAP_INGEST_INFO_PERSIST_FMT_VER, wb_buffer);
    {
        size_t segment_data_size = 0;
        segment_data_size += writeBinary2(restored_segments.size(), wb_buffer);
        for (const auto & restored_segment : restored_segments)
        {
            LOG_DEBUG(
                log,
                "Persist segment to local, segment_id={} region_id={}",
                restored_segment->segmentId(),
                region_id);
            data_size += writeBinary2(restored_segment->segmentId(), wb_buffer);
        }
        segment_data_size += writeBinary2<UInt64>(remote_store_id, wb_buffer);
        data_size += segment_data_size;
        RUNTIME_CHECK_MSG(
            wb_buffer.count() == data_size,
            "buffer {} != data_size {}, segment_data_size={}",
            wb_buffer.count(),
            data_size,
            segment_data_size);
    }
    {
        // Although the region is the first peer of this region in this store, we can't write it to formal KVStore for now.
        // Otherwise it could be uploaded and then overwritten.
        auto region_size = RegionPersister::computeRegionWriteBuffer(*region, wb_buffer);
        data_size += region_size;
        RUNTIME_CHECK_MSG(
            wb_buffer.count() == data_size,
            "buffer {} != data_size {}, region_size={}",
            wb_buffer.count(),
            data_size,
            region_size);
    }
    auto page_id
        = UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestInfo, region_id);
    auto read_buf = wb_buffer.tryGetReadBuffer();
    wb.putPage(UniversalPageId(page_id.data(), page_id.size()), 0, read_buf, data_size);
    uni_ps->write(std::move(wb), DB::PS::V3::PageType::Local, nullptr);
    LOG_INFO(
        log,
        "Successfully persist CheckpointIngestInfo, region_id={} peer_id={} remote_store_id={} region={} segments={}",
        region_id,
        peer_id,
        remote_store_id,
        region->getDebugString(),
        restored_segments.size());
}

static void removeFromLocal(TMTContext & tmt, UInt64 region_id)
{
    auto log = DB::Logger::get();
    LOG_INFO(log, "Erase CheckpointIngestInfo from disk, region_id={}", region_id);
    auto uni_ps = tmt.getContext().getWriteNodePageStorage();
    UniversalWriteBatch del_batch;
    del_batch.delPage(
        UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestInfo, region_id));
    uni_ps->write(std::move(del_batch), PageType::Local);
}

void CheckpointIngestInfo::deleteWrittenData(TMTContext & tmt, RegionPtr region, const DM::Segments & segments)
{
    UNUSED(tmt, region, segments);
    // auto & storages = tmt.getStorages();
    // auto keyspace_id = region->getKeyspaceID();
    // auto table_id = region->getMappedTableID();
    // auto storage = storages.get(keyspace_id, table_id);

    // auto log = DB::Logger::get("CheckpointIngestInfo");
    // if (storage && storage->engineType() == TiDB::StorageEngine::DT)
    // {
    //     auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    //     auto dm_context = dm_storage->getStore()->newDMContext(tmt.getContext(), tmt.getContext().getSettingsRef());
    //     for (auto segment_to_drop : segments)
    //     {
    //         DM::WriteBatches wbs(*dm_context->storage_pool, dm_context->getWriteLimiter());
    //         // No need to call `abandon`, since the segment is not ingested or in use.
    //         LOG_DEBUG(log, "Delete segment from local, segment_id={}, page_id={}, region_id={}",
    //             segment_to_drop->segmentId(),
    //             UniversalPageIdFormat::toFullPageId(UniversalPageIdFormat::toFullPrefix(keyspace_id, StorageType::Meta, table_id), segment_to_drop->segmentId()),
    //             region->id());
    //         segment_to_drop->drop(tmt.getContext().getFileProvider(), wbs);
    //     }
    // }
}

bool CheckpointIngestInfo::cleanOnSuccess(TMTContext & tmt, UInt64 region_id, bool pre_check)
{
    if (!pre_check)
    {
        removeFromLocal(tmt, region_id);
        return true;
    }

    auto uni_ps = tmt.getContext().getWriteNodePageStorage();
    auto snapshot = uni_ps->getSnapshot(fmt::format("read_fap_i_{}", region_id));
    auto page_id
        = UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestInfo, region_id);
    // For most cases, ingest infos are deleted in `removeFromLocal`.
    Page page = uni_ps->read(page_id, nullptr, snapshot, /*throw_on_not_exist*/ false);
    if (unlikely(page.isValid()))
    {
        removeFromLocal(tmt, region_id);
        return true;
    }
    return false;
}

bool CheckpointIngestInfo::forciblyClean(
    TMTContext & tmt,
    const TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id)
{
    UNUSED(proxy_helper);
    CheckpointIngestInfo::cleanOnSuccess(tmt, region_id, true);
    return false;
}

} // namespace DB
