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
#include <IO/MemoryReadWriteBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/ConfigSettings.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/V1/PageStorage.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/PathPool.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionManager.h>
#include <Storages/Transaction/RegionPersister.h>

#include <memory>

namespace CurrentMetrics
{
extern const Metric RegionPersisterRunMode;
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char force_enable_region_persister_compatible_mode[];
extern const char force_disable_region_persister_compatible_mode[];
} // namespace FailPoints

void RegionPersister::drop(RegionID region_id, const RegionTaskLock &)
{
    UniversalWriteBatch wb;
    wb.delPage(KVStoreReader::toFullPageId(region_id));
    global_uni_page_storage->write(std::move(wb), global_context.getWriteLimiter());
}

void RegionPersister::computeRegionWriteBuffer(const Region & region, RegionCacheWriteElement & region_write_buffer)
{
    auto & [region_id, buffer, region_size, applied_index] = region_write_buffer;

    region_id = region.id();
    std::tie(region_size, applied_index) = region.serialize(buffer);
    if (unlikely(region_size > static_cast<size_t>(std::numeric_limits<UInt32>::max())))
    {
        LOG_WARNING(
            Logger::get(),
            "Persisting big region: {} with data info: {}, serialized size {}",
            region.toString(),
            region.dataInfo(),
            region_size);
    }
}

void RegionPersister::persist(const Region & region, const RegionTaskLock & lock)
{
    doPersist(region, &lock);
}

void RegionPersister::persist(const Region & region)
{
    doPersist(region, nullptr);
}

void RegionPersister::doPersist(const Region & region, const RegionTaskLock * lock)
{
    // Support only one thread persist.
    RegionCacheWriteElement region_buffer;
    computeRegionWriteBuffer(region, region_buffer);

    if (lock)
        doPersist(region_buffer, *lock, region);
    else
        doPersist(region_buffer, region_manager.genRegionTaskLock(region.id()), region);
}

void RegionPersister::doPersist(RegionCacheWriteElement & region_write_buffer, const RegionTaskLock &, const Region & region)
{
    auto & [region_id, buffer, region_size, applied_index] = region_write_buffer;

    std::lock_guard lock(mutex);

    if (page_reader->isAppliedIndexExceed(region_id, applied_index))
        return;

    if (region.isPendingRemove())
    {
        LOG_DEBUG(log, "no need to persist {} because of pending remove", region.toString(false));
        return;
    }

    auto read_buf = buffer.tryGetReadBuffer();
    UniversalWriteBatch wb;
    wb.putPage(KVStoreReader::toFullPageId(region_id), applied_index, read_buf, region_size);
    global_uni_page_storage->write(std::move(wb), global_context.getWriteLimiter());
}

RegionPersister::RegionPersister(Context & global_context_, const RegionManager & region_manager_)
    : global_context(global_context_)
    , global_uni_page_storage(global_context_.getWriteNodePageStorage())
    , region_manager(region_manager_)
    , log(Logger::get())
{}

PageStorageConfig RegionPersister::getPageStorageSettings() const
{
    // TODO: not implement it because it is just for test
    return PageStorageConfig{};
}

PS::V1::PageStorage::Config getV1PSConfig(const PageStorageConfig & config)
{
    PS::V1::PageStorage::Config c;
    c.sync_on_write = config.sync_on_write;
    c.file_roll_size = config.file_roll_size;
    c.file_max_size = config.file_max_size;
    c.file_small_size = config.file_max_size;

    c.merge_hint_low_used_rate = config.gc_max_valid_rate;
    c.merge_hint_low_used_file_total_size = config.gc_min_bytes;
    c.merge_hint_low_used_file_num = config.gc_min_files;
    c.gc_compact_legacy_min_num = config.gc_min_legacy_num;

    c.version_set_config.compact_hint_delta_deletions = config.version_set_config.compact_hint_delta_deletions;
    c.version_set_config.compact_hint_delta_entries = config.version_set_config.compact_hint_delta_entries;
    return c;
}

void RegionPersister::forceTransformKVStoreV2toV3()
{
}

RegionMap RegionPersister::restore(PathPool & path_pool, const TiFlashRaftProxyHelper * proxy_helper, PageStorageConfig /*config*/)
{
    {
        auto delegator = path_pool.getPSDiskDelegatorRaft();
        auto provider = global_context.getFileProvider();
        const auto global_run_mode = global_context.getPageStorageRunMode();
        auto global_uni_storage = global_context.getWriteNodePageStorage();
        page_reader = std::make_shared<KVStoreReader>(*global_uni_storage);

        auto run_mode = global_run_mode;
        CurrentMetrics::set(CurrentMetrics::RegionPersisterRunMode, static_cast<UInt8>(run_mode));
        LOG_INFO(log, "RegionPersister running. Current Run Mode is {}", static_cast<UInt8>(run_mode));
    }

    RegionMap regions;
    auto acceptor = [&](DB::PageId page_id, const Page & page) {
        ReadBufferFromMemory buf(page.data.begin(), page.data.size());
        auto region = Region::deserialize(buf, proxy_helper);
        if (page_id != region->id())
            throw Exception(fmt::format("region id and page id not match page_id={} region_id={}!", page_id, region->id()), ErrorCodes::LOGICAL_ERROR);

        regions.emplace(region->id(), region);
    };
    page_reader->traverse(acceptor);

    return regions;
}

bool RegionPersister::gc()
{
    // TODO: implement it
    return false;
}

FileUsageStatistics RegionPersister::getFileUsageStatistics() const
{
    return FileUsageStatistics{};
}

} // namespace DB
