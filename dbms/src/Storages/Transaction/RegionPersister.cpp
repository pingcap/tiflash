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
#include <Storages/Page/V1/PageStorage.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/PathPool.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionManager.h>
#include <Storages/Transaction/RegionPersister.h>

#include <memory>

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
    if (page_writer)
    {
        DB::WriteBatch wb_v2{ns_id};
        wb_v2.delPage(region_id);
        page_writer->write(std::move(wb_v2), global_context.getWriteLimiter());
    }
    else
    {
        PS::V1::WriteBatch wb_v1;
        wb_v1.delPage(region_id);
        stable_page_storage->write(std::move(wb_v1));
    }
}

void RegionPersister::computeRegionWriteBuffer(const Region & region, RegionCacheWriteElement & region_write_buffer)
{
    auto & [region_id, buffer, region_size, applied_index] = region_write_buffer;

    region_id = region.id();
    std::tie(region_size, applied_index) = region.serialize(buffer);
    if (unlikely(region_size > static_cast<size_t>(std::numeric_limits<UInt32>::max())))
    {
        LOG_FMT_WARNING(
            &Poco::Logger::get("RegionPersister"),
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

    if (page_reader)
    {
        auto entry = page_reader->getPageEntry(region_id);
        if (entry.isValid() && entry.tag > applied_index)
            return;
    }
    else
    {
        auto entry = stable_page_storage->getEntry(region_id, nullptr);
        if (entry.isValid() && entry.tag > applied_index)
            return;
    }

    if (region.isPendingRemove())
    {
        LOG_FMT_DEBUG(log, "no need to persist {} because of pending remove", region.toString(false));
        return;
    }

    auto read_buf = buffer.tryGetReadBuffer();
    if (page_writer)
    {
        DB::WriteBatch wb{ns_id};
        wb.putPage(region_id, applied_index, read_buf, region_size);
        page_writer->write(std::move(wb), global_context.getWriteLimiter());
    }
    else
    {
        PS::V1::WriteBatch wb;
        wb.putPage(region_id, applied_index, read_buf, region_size);
        stable_page_storage->write(std::move(wb));
    }
}

RegionPersister::RegionPersister(Context & global_context_, const RegionManager & region_manager_)
    : global_context(global_context_)
    , region_manager(region_manager_)
    , log(&Poco::Logger::get("RegionPersister"))
{}

PS::V1::PageStorage::Config getV1PSConfig(const PS::V2::PageStorage::Config & config)
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

RegionMap RegionPersister::restore(const TiFlashRaftProxyHelper * proxy_helper, PageStorage::Config config)
{
    {
        auto & path_pool = global_context.getPathPool();
        auto delegator = path_pool.getPSDiskDelegatorRaft();
        auto provider = global_context.getFileProvider();
        // If the GlobalStoragePool is initialized, then use v3 format
        const auto & run_mode = global_context.getPageStorageRunMode();
        // todo
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            // If there is no PageFile with basic version binary format, use version 2 of PageStorage.
            auto detect_binary_version = DB::PS::V2::PageStorage::getMaxDataVersion(provider, delegator);
            bool use_v1_format = path_pool.isRaftCompatibleModeEnabled() && (detect_binary_version == PageFormat::V1);

            fiu_do_on(FailPoints::force_enable_region_persister_compatible_mode, { use_v1_format = true; });
            fiu_do_on(FailPoints::force_disable_region_persister_compatible_mode, { use_v1_format = false; });

            if (!use_v1_format)
            {
                mergeConfigFromSettings(global_context.getSettingsRef(), config);
                config.num_write_slots = 4; // extend write slots to 4 at least

                LOG_FMT_INFO(log, "RegionPersister running in v2 mode");
                auto page_storage_v2 = std::make_shared<PS::V2::PageStorage>(
                    "RegionPersister",
                    delegator,
                    config,
                    provider);
                page_storage_v2->restore();
                page_writer = std::make_shared<PageWriter>(run_mode, page_storage_v2, /*storage_v3_*/ nullptr);
                page_reader = std::make_shared<PageReader>(run_mode, ns_id, page_storage_v2, /*storage_v3_*/ nullptr, /*readlimiter*/ global_context.getReadLimiter());
            }
            else
            {
                LOG_FMT_INFO(log, "RegionPersister running in v1 mode");
                auto c = getV1PSConfig(config);
                stable_page_storage = std::make_unique<PS::V1::PageStorage>(
                    "RegionPersister",
                    delegator->defaultPath(),
                    c,
                    provider);
            }
            break;
        }
        case PageStorageRunMode::ONLY_V3:
        {
            mergeConfigFromSettings(global_context.getSettingsRef(), config);

            LOG_FMT_INFO(log, "RegionPersister running in v3 mode");
            auto page_storage_v3 = std::make_shared<PS::V3::PageStorageImpl>( //
                "RegionPersister",
                delegator,
                config,
                provider);
            page_storage_v3->restore();
            page_writer = std::make_shared<PageWriter>(run_mode, /*storage_v2_*/ nullptr, page_storage_v3);
            page_reader = std::make_shared<PageReader>(run_mode, ns_id, /*storage_v2_*/ nullptr, page_storage_v3, global_context.getReadLimiter());
            break;
        }
        case PageStorageRunMode::MIX_MODE:
        {
            LOG_FMT_INFO(log, "RegionPersister running in mix mode");
            auto page_storage_v2 = std::make_shared<PS::V2::PageStorage>(
                "RegionPersister",
                delegator,
                config,
                provider);
            auto page_storage_v3 = std::make_shared<PS::V3::PageStorageImpl>( //
                "RegionPersister",
                delegator,
                config,
                provider);

            page_storage_v2->restore();
            page_storage_v3->restore();
            page_writer = std::make_shared<PageWriter>(run_mode, page_storage_v2, page_storage_v3);
            page_reader = std::make_shared<PageReader>(run_mode, ns_id, page_storage_v2, page_storage_v3, global_context.getReadLimiter());
            break;
        }
        }
    }

    RegionMap regions;
    if (page_reader)
    {
        auto acceptor = [&](const DB::Page & page) {
            ReadBufferFromMemory buf(page.data.begin(), page.data.size());
            auto region = Region::deserialize(buf, proxy_helper);
            if (page.page_id != region->id())
                throw Exception("region id and page id not match!", ErrorCodes::LOGICAL_ERROR);
            regions.emplace(page.page_id, region);
        };
        page_reader->traverse(acceptor);
    }
    else
    {
        auto acceptor = [&](const PS::V1::Page & page) {
            ReadBufferFromMemory buf(page.data.begin(), page.data.size());
            auto region = Region::deserialize(buf, proxy_helper);
            if (page.page_id != region->id())
                throw Exception("region id and page id not match!", ErrorCodes::LOGICAL_ERROR);
            regions.emplace(page.page_id, region);
        };
        stable_page_storage->traverse(acceptor, nullptr);
    }

    return regions;
}

bool RegionPersister::gc()
{
    if (page_writer)
    {
        PageStorage::Config config = getConfigFromSettings(global_context.getSettingsRef());
        page_writer->reloadSettings(config);
        return page_writer->gc(false, nullptr, nullptr);
    }
    else
        return stable_page_storage->gc();
}

} // namespace DB
