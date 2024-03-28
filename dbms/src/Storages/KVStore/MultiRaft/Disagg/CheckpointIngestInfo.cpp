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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/CheckpointIngestInfo.h>
#include <Storages/KVStore/MultiRaft/RegionPersister.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/Universal/RaftDataReader.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/StorageDeltaMerge.h>

namespace DB
{
CheckpointIngestInfoPtr CheckpointIngestInfo::restore(
    TMTContext & tmt,
    const TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id,
    UInt64 peer_id)
{
    RegionPtr region;
    DM::Segments restored_segments;

    auto log = DB::Logger::get("CheckpointIngestInfo");
    auto uni_ps = tmt.getContext().getWriteNodePageStorage();
    RUNTIME_CHECK(uni_ps != nullptr);
    auto snapshot = uni_ps->getSnapshot(fmt::format("read_fap_i_{}", region_id));
    RUNTIME_CHECK(snapshot != nullptr);
    auto page_id
        = UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestInfo, region_id);
    Page page = uni_ps->read(page_id, nullptr, snapshot, /*throw_on_not_exist*/ false);
    if (!page.isValid())
    {
        // The restore failed, we can safely return null here to make FAP fallback in ApplyFapSnapshotImpl.
        return nullptr;
    }

    FastAddPeerProto::CheckpointIngestInfoPersisted ingest_info_persisted;
    if (!ingest_info_persisted.ParseFromArray(page.data.data(), page.data.size()))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Can't parse CheckpointIngestInfo, region_id={} peer_id={} store_id={}",
            region_id,
            peer_id,
            tmt.getKVStore()->getStoreID(std::memory_order_relaxed));
    }

    {
        ReadBufferFromMemory buf(
            ingest_info_persisted.region_info().data(),
            ingest_info_persisted.region_info().size());
        region = Region::deserialize(buf, proxy_helper);
    }

    StoreID remote_store_id = ingest_info_persisted.remote_store_id();

    auto & storages = tmt.getStorages();
    auto keyspace_id = region->getKeyspaceID();
    auto table_id = region->getMappedTableID();
    auto storage = storages.get(keyspace_id, table_id);
    if (storage && storage->engineType() == TiDB::StorageEngine::DT)
    {
        auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
        auto dm_context = dm_storage->getStore()->newDMContext(tmt.getContext(), tmt.getContext().getSettingsRef());
        for (const auto & seg_persisted : ingest_info_persisted.segments())
        {
            ReadBufferFromString buf(seg_persisted.segment_meta());
            DM::Segment::SegmentMetaInfo segment_info;
            readSegmentMetaInfo(buf, segment_info);

            ReadBufferFromString buf_delta(seg_persisted.delta_meta());
            auto delta
                = DM::DeltaValueSpace::restore(*dm_context, segment_info.range, buf_delta, segment_info.delta_id);
            ReadBufferFromString buf_stable(seg_persisted.stable_meta());
            auto stable = DM::StableValueSpace::restore(*dm_context, buf_stable, segment_info.stable_id);

            LOG_DEBUG(
                log,
                "Restore segments for checkpoint, remote_segment_id={} range={} remote_store_id={} region_id={}",
                segment_info.segment_id,
                segment_info.range.toDebugString(),
                remote_store_id,
                region_id);
            restored_segments.push_back(std::make_shared<DM::Segment>(
                log,
                segment_info.epoch,
                segment_info.range,
                segment_info.segment_id,
                segment_info.next_segment_id,
                delta,
                stable));
        }
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

FastAddPeerProto::CheckpointIngestInfoPersisted CheckpointIngestInfo::serializeMeta() const
{
    // Write:
    // - The region, which is actually data and meta in KVStore.
    // - The segment ids point to segments which are already persisted but not ingested.
    FastAddPeerProto::CheckpointIngestInfoPersisted ingest_info_persisted;
    {
        for (const auto & restored_segment : restored_segments)
        {
            auto * segment_info = ingest_info_persisted.add_segments();
            restored_segment->serializeToFAPTempSegment(segment_info);
        }
    }
    {
        // Although the region is the first peer of this region in this store, we can't write it to formal KVStore for now.
        // Otherwise it could be uploaded and then overwritten.
        WriteBufferFromOwnString wb;
        RegionPersister::computeRegionWriteBuffer(*region, wb);
        ingest_info_persisted.set_region_info(wb.releaseStr());
    }
    ingest_info_persisted.set_remote_store_id(remote_store_id);
    return ingest_info_persisted;
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

    auto ingest_info_persisted = serializeMeta();
    auto s = ingest_info_persisted.SerializeAsString();
    auto data_size = s.size();
    auto read_buf = std::make_shared<ReadBufferFromOwnString>(s);
    auto page_id
        = UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestInfo, region_id);
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
    auto uni_ps = tmt.getContext().getWriteNodePageStorage();
    UniversalWriteBatch del_batch;
    del_batch.delPage(
        UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestInfo, region_id));
    uni_ps->write(std::move(del_batch), PageType::Local);
}

void CheckpointIngestInfo::deleteWrittenData(TMTContext & tmt, RegionPtr region, const DM::Segments & segments)
{
    auto region_id = region->id();
    auto & storages = tmt.getStorages();
    auto keyspace_id = region->getKeyspaceID();
    auto table_id = region->getMappedTableID();
    auto storage = storages.get(keyspace_id, table_id);

    auto log = DB::Logger::get("CheckpointIngestInfo");
    if (storage && storage->engineType() == TiDB::StorageEngine::DT)
    {
        auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
        auto dm_context = dm_storage->getStore()->newDMContext(tmt.getContext(), tmt.getContext().getSettingsRef());
        for (const auto & segment_to_drop : segments)
        {
            DM::WriteBatches wbs(*dm_context->storage_pool, dm_context->getWriteLimiter());
            // No need to call `abandon`, since the segment is not ingested or in use.
            LOG_DEBUG(
                log,
                "Delete segment from local, segment_id={}, page_id={}, region_id={}",
                segment_to_drop->segmentId(),
                UniversalPageIdFormat::toFullPageId(
                    UniversalPageIdFormat::toFullPrefix(keyspace_id, StorageType::Meta, table_id),
                    segment_to_drop->segmentId()),
                region->id());
            segment_to_drop->dropAsFAPTemp(tmt.getContext().getFileProvider(), wbs);
        }
    }
    else
    {
        LOG_INFO(
            log,
            "No found storage in clean stale FAP data region_id={} keyspace_id={} table_id={}",
            region_id,
            keyspace_id,
            table_id);
    }

    UniversalWriteBatch wb;
    auto wn_ps = tmt.getContext().getWriteNodePageStorage();
    RaftDataReader raft_data_reader(*wn_ps);
    raft_data_reader.traverseRemoteRaftLogForRegion(
        region_id,
        [&](const UniversalPageId & page_id, PageSize size, const PS::V3::CheckpointLocation &) {
            LOG_DEBUG(
                log,
                "Delete raft log size {}, region_id={} index={}",
                size,
                region_id,
                UniversalPageIdFormat::getU64ID(page_id));
            wb.delPage(page_id);
        });
    wn_ps->write(std::move(wb));

    LOG_INFO(
        log,
        "Finish clean stale FAP data region_id={} keyspace_id={} table_id={}",
        region_id,
        keyspace_id,
        table_id);
}

bool CheckpointIngestInfo::cleanOnSuccess(TMTContext & tmt, UInt64 region_id)
{
    auto log = DB::Logger::get();
    LOG_INFO(log, "Erase CheckpointIngestInfo from disk on success, region_id={}", region_id);
    removeFromLocal(tmt, region_id);
    return true;
}

bool CheckpointIngestInfo::forciblyClean(
    TMTContext & tmt,
    const TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id,
    bool in_memory,
    CleanReason reason)
{
    auto log = DB::Logger::get();
    // For most cases, ingest infos are deleted in `removeFromLocal`.
    auto checkpoint_ptr = CheckpointIngestInfo::restore(tmt, proxy_helper, region_id, 0);
    LOG_INFO(
        log,
        "Erase CheckpointIngestInfo from disk by force, region_id={} exist={} in_memory={} reason={}",
        region_id,
        checkpoint_ptr != nullptr,
        in_memory,
        magic_enum::enum_name(reason));
    if (unlikely(checkpoint_ptr))
    {
        // First delete the page, it may cause dangling data.
        // However, never point to incomplete data then.
        removeFromLocal(tmt, region_id);
        CheckpointIngestInfo::deleteWrittenData(
            tmt,
            checkpoint_ptr->getRegion(),
            checkpoint_ptr->getRestoredSegments());
        return true;
    }
    return false;
}

} // namespace DB
