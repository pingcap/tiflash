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
DM::Segments CheckpointIngestInfo::getRestoredSegments() const
{
    if (unlikely(!in_memory))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CheckpointIngestInfo is not inited");
    }
    return restored_segments;
}

UInt64 CheckpointIngestInfo::getRemoteStoreId() const
{
    if (unlikely(!in_memory))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CheckpointIngestInfo is not inited");
    }
    return remote_store_id;
}

void CheckpointIngestInfo::markDelete()
{
    clean_when_destruct = true;
}

RegionPtr CheckpointIngestInfo::getRegion() const
{
    if (unlikely(!in_memory))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CheckpointIngestInfo is not inited");
    }
    return region;
}

bool CheckpointIngestInfo::forciblyClean(TMTContext & tmt, UInt64 region_id)
{
    auto log = DB::Logger::get("CheckpointIngestInfo");
    auto uni_ps = tmt.getContext().getWriteNodePageStorage();
    auto snapshot = uni_ps->getSnapshot(fmt::format("read_l_{}", region_id));
    UniversalWriteBatch del_batch;
    bool has_data = false;
    {
        auto page_id
            = UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestRegion, region_id);
        Page page = uni_ps->read(page_id, nullptr, snapshot, /*throw_on_not_exist*/ false);
        has_data = true;
        if (page.isValid())
        {
            del_batch.delPage(UniversalPageIdFormat::toLocalKVPrefix(
                UniversalPageIdFormat::LocalKVKeyType::FAPIngestRegion,
                region_id));
        }
    }
    {
        auto page_id = UniversalPageIdFormat::toLocalKVPrefix(
            UniversalPageIdFormat::LocalKVKeyType::FAPIngestSegments,
            region_id);
        Page page = uni_ps->read(page_id, nullptr, snapshot, /*throw_on_not_exist*/ false);
        has_data = true;
        del_batch.delPage(UniversalPageIdFormat::toLocalKVPrefix(
            UniversalPageIdFormat::LocalKVKeyType::FAPIngestSegments,
            region_id));
        uni_ps->write(std::move(del_batch), PageType::Local);
    }
    if (has_data)
        uni_ps->write(std::move(del_batch), DB::PS::V3::PageType::Local, nullptr);
    return has_data;
}

bool CheckpointIngestInfo::loadFromPS(const TiFlashRaftProxyHelper * proxy_helper)
{
    RUNTIME_CHECK_MSG(!in_memory && restored_segments.empty(), "CheckpointIngestInfo is already inited");
    auto log = DB::Logger::get("CheckpointIngestInfo");
    auto uni_ps = tmt.getContext().getWriteNodePageStorage();
    auto snapshot = uni_ps->getSnapshot(fmt::format("read_l_{}", region_id));

    {
        auto page_id
            = UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestRegion, region_id);
        Page page = uni_ps->read(page_id, nullptr, snapshot, /*throw_on_not_exist*/ false);
        if (!page.isValid())
            return false;
        ReadBufferFromMemory buf(page.data.begin(), page.data.size());
        region = Region::deserialize(buf, proxy_helper);
    }

    {
        auto & storages = tmt.getStorages();
        auto keyspace_id = region->getKeyspaceID();
        auto table_id = region->getMappedTableID();
        auto storage = storages.get(keyspace_id, table_id);
        if (storage && storage->engineType() == TiDB::StorageEngine::DT)
        {
            auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
            auto dm_context = dm_storage->getStore()->newDMContext(tmt.getContext(), tmt.getContext().getSettingsRef());
            auto page_id = UniversalPageIdFormat::toLocalKVPrefix(
                UniversalPageIdFormat::LocalKVKeyType::FAPIngestSegments,
                region_id);
            auto page = uni_ps->read(page_id, nullptr, snapshot, /*throw_on_not_exist*/ false);
            if (!page.isValid())
                return false;
            ReadBufferFromMemory buf(page.data.begin(), page.data.size());
            auto count = readBinary2<UInt64>(buf);
            for (size_t i = 0; i < count; i++)
            {
                auto segment_id = readBinary2<UInt64>(buf);
                restored_segments.emplace_back(DM::Segment::restoreSegment(log, *dm_context, segment_id));
            }
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "unsupported storage engine");
        }

        LOG_INFO(
            log,
            "CheckpointIngestInfo restore success, region_id={} table_id={} keyspace_id={} region={}",
            region_id,
            table_id,
            keyspace_id,
            region->getDebugString());
    }

    in_memory = true;
    return true;
}

void CheckpointIngestInfo::persistToPS()
{
    auto * log = &Poco::Logger::get("CheckpointIngestInfo");
    auto uni_ps = tmt.getContext().getWriteNodePageStorage();
    UniversalWriteBatch wb;
    // Write:
    // - The region, which is actually data and meta in KVStore.
    // - The segment ids point to segments which are already persisted but not ingested.
    {
        // The region should be persisted in local, although it's the first peer of this region in this store.
        // Otherwise it could be uploaded and then overwritten.
        RegionPersister::RegionCacheWriteElement region_buffer;
        RegionPersister::computeRegionWriteBuffer(*region, region_buffer);
        auto & [region_id, buffer, region_size, applied_index] = region_buffer;
        if (region->isPendingRemove())
        {
            // A pending remove region should not be selected as candidate.
            LOG_ERROR(log, "candidate region {} is pending remove", region->toString(false));
            return;
        }
        auto read_buf = buffer.tryGetReadBuffer();
        RUNTIME_CHECK_MSG(read_buf != nullptr, "failed to gen buffer for region {}", region->toString(true));
        RUNTIME_CHECK_MSG(buffer.count() == region_size, "buffer {} != region_size {}", buffer.count(), region_size);
        auto page_id
            = UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestRegion, region_id);
        wb.putPage(UniversalPageId(page_id.data(), page_id.size()), 0, read_buf, region_size);
    }
    {
        size_t data_size = 0;
        uint64_t count = restored_segments.size();
        MemoryWriteBuffer buffer;
        data_size += writeBinary2(count, buffer);
        for (auto it = restored_segments.begin(); it != restored_segments.end(); it++)
        {
            data_size += writeBinary2((*it)->segmentId(), buffer);
        }
        auto page_id = UniversalPageIdFormat::toLocalKVPrefix(
            UniversalPageIdFormat::LocalKVKeyType::FAPIngestSegments,
            region_id);
        auto read_buf = buffer.tryGetReadBuffer();
        RUNTIME_CHECK_MSG(buffer.count() == data_size, "buffer {} != data_size {}", buffer.count(), data_size);
        wb.putPage(UniversalPageId(page_id.data(), page_id.size()), 0, read_buf, data_size);
    }
    uni_ps->write(std::move(wb), DB::PS::V3::PageType::Local, nullptr);
}

void CheckpointIngestInfo::removeFromPS()
{
    auto uni_ps = tmt.getContext().getWriteNodePageStorage();
    UniversalWriteBatch del_batch;
    del_batch.delPage(
        UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestRegion, region_id));
    del_batch.delPage(
        UniversalPageIdFormat::toLocalKVPrefix(UniversalPageIdFormat::LocalKVKeyType::FAPIngestSegments, region_id));
    uni_ps->write(std::move(del_batch), PageType::Local);
}

CheckpointIngestInfo::~CheckpointIngestInfo()
{
    if (clean_when_destruct)
    {
        removeFromPS();
    }
}

} // namespace DB