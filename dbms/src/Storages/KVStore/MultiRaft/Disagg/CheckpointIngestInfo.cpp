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
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/CheckpointIngestInfo.h>
#include <Storages/KVStore/MultiRaft/Disagg/fast_add_peer.pb.h>
#include <Storages/KVStore/MultiRaft/RegionPersister.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/Page/PageStorage.h>
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
    GET_METRIC(tiflash_fap_task_result, type_restore).Increment();
    RegionPtr region;
    DM::Segments restored_segments;

    auto log = DB::Logger::get("CheckpointIngestInfo");
    auto uni_ps = tmt.getContext().getWriteNodePageStorage();
    auto snapshot = uni_ps->getSnapshot(fmt::format("read_fap_i_{}", region_id));
    auto page_id
        = UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestInfo, region_id);
    Page page = uni_ps->read(page_id, nullptr, snapshot, /*throw_on_not_exist*/ false);
    if (!page.isValid())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Failed to restore CheckpointIngestInfo, region_id={} peer_id={} store_id={}",
            region_id,
            peer_id,
            tmt.getKVStore()->getStoreID(std::memory_order_relaxed));
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
                "Restore segments for checkpoint, remote_segment_id={} range={} remote_store_id={}",
                segment_info.segment_id,
                segment_info.range.toDebugString(),
                remote_store_id);
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

    auto s = ingest_info_persisted.SerializeAsString();
    auto data_size = s.size();
    auto read_buf = std::make_shared<ReadBufferFromOwnString>(s);
    auto page_id
        = UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestInfo, region_id);
    wb.putPage(UniversalPageId(page_id.data(), page_id.size()), 0, read_buf, data_size);
    uni_ps->write(std::move(wb), DB::PS::V3::PageType::Local, nullptr);
    LOG_INFO(
        log,
        "Successfully persist CheckpointIngestInfo, region_id={} peer_id={} remote_store_id={} region={}",
        region_id,
        peer_id,
        remote_store_id,
        region->getDebugString());
}

void CheckpointIngestInfo::removeFromLocal(TMTContext & tmt, UInt64 region_id)
{
    auto log = DB::Logger::get();
    LOG_INFO(log, "Erase CheckpointIngestInfo from disk, region_id={}", region_id);
    auto uni_ps = tmt.getContext().getWriteNodePageStorage();
    UniversalWriteBatch del_batch;
    del_batch.delPage(
        UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestInfo, region_id));
    uni_ps->write(std::move(del_batch), PageType::Local);
}

// Like removeFromLocal, but is static and with check.
bool CheckpointIngestInfo::forciblyClean(TMTContext & tmt, UInt64 region_id, bool pre_check)
{
    if (!pre_check)
    {
        CheckpointIngestInfo::removeFromLocal(tmt, region_id);
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
        CheckpointIngestInfo::removeFromLocal(tmt, region_id);
        return true;
    }
    return false;
}

} // namespace DB
