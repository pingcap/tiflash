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
            Logger::get("RegionPersister"),
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
    , log(Logger::get("RegionPersister"))
{}

PageStorage::Config RegionPersister::getPageStorageSettings() const
{
    if (!page_writer)
    {
        throw Exception("Not support for PS v1", ErrorCodes::LOGICAL_ERROR);
    }

    return page_writer->getSettings();
}

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

void RegionPersister::forceTransformKVStoreV2toV3()
{
    assert(page_reader != nullptr);
    assert(page_writer != nullptr);

    WriteBatch write_batch_del_v2{KVSTORE_NAMESPACE_ID};
    auto meta_transform_acceptor = [&](const DB::Page & page) {
        WriteBatch write_batch_transform{KVSTORE_NAMESPACE_ID};
        // Check pages have not contain field offset
        // Also get the tag of page_id
        const auto & page_transform_entry = page_reader->getPageEntry(page.page_id);
        if (!page_transform_entry.field_offsets.empty())
        {
            throw Exception(fmt::format("Can't transfrom kvstore from V2 to V3, [page_id={}] {}",
                                        page.page_id,
                                        page_transform_entry.toDebugString()),
                            ErrorCodes::LOGICAL_ERROR);
        }

        write_batch_transform.putPage(page.page_id, //
                                      page_transform_entry.tag,
                                      std::make_shared<ReadBufferFromMemory>(page.data.begin(),
                                                                             page.data.size()),
                                      page.data.size());

        // Will rewrite into V3 one by one.
        // The region data is big. It is not a good idea to combine pages.
        page_writer->write(std::move(write_batch_transform), nullptr);

        // Record del page_id
        write_batch_del_v2.delPage(page.page_id);
    };

    page_reader->traverse(meta_transform_acceptor, /*only_v2*/ true, /*only_v3*/ false);

    // DEL must call after rewrite.
    page_writer->writeIntoV2(std::move(write_batch_del_v2), nullptr);
}

RegionMap RegionPersister::restore(const TiFlashRaftProxyHelper * proxy_helper, PageStorage::Config config)
{
    {
        auto & path_pool = global_context.getPathPool();
        auto delegator = path_pool.getPSDiskDelegatorRaft();
        auto provider = global_context.getFileProvider();
        auto run_mode = global_context.getPageStorageRunMode();

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

                auto page_storage_v2 = std::make_shared<PS::V2::PageStorage>(
                    "RegionPersister",
                    delegator,
                    config,
                    provider,
                    global_context.getPSBackgroundPool());
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

            auto page_storage_v3 = std::make_shared<PS::V3::PageStorageImpl>( //
                "RegionPersister",
                path_pool.getPSDiskDelegatorGlobalMulti("kvstore"),
                config,
                provider);
            page_storage_v3->restore();
            page_writer = std::make_shared<PageWriter>(run_mode, /*storage_v2_*/ nullptr, page_storage_v3);
            page_reader = std::make_shared<PageReader>(run_mode, ns_id, /*storage_v2_*/ nullptr, page_storage_v3, global_context.getReadLimiter());
            break;
        }
        case PageStorageRunMode::MIX_MODE:
        {
            auto page_storage_v2 = std::make_shared<PS::V2::PageStorage>(
                "RegionPersister",
                delegator,
                config,
                provider,
                global_context.getPSBackgroundPool());
            // V3 should not used getPSDiskDelegatorRaft
            // Because V2 will delete all invalid(unrecognized) file when it restore
            auto page_storage_v3 = std::make_shared<PS::V3::PageStorageImpl>( //
                "RegionPersister",
                path_pool.getPSDiskDelegatorGlobalMulti("kvstore"),
                config,
                provider);

            page_storage_v2->restore();
            page_storage_v3->restore();

            if (const auto & kvstore_remain_pages = page_storage_v2->getNumberOfPages(); kvstore_remain_pages != 0)
            {
                page_writer = std::make_shared<PageWriter>(run_mode, page_storage_v2, page_storage_v3);
                page_reader = std::make_shared<PageReader>(run_mode, ns_id, page_storage_v2, page_storage_v3, global_context.getReadLimiter());

                LOG_FMT_INFO(log, "Current kvstore transform to V3 begin [pages_before_transform={}]", kvstore_remain_pages);
                forceTransformKVStoreV2toV3();
                const auto & kvstore_remain_pages_after_transform = page_storage_v2->getNumberOfPages();
                LOG_FMT_INFO(log, "Current kvstore transfrom to V3 finished. [ns_id={}] [done={}] [pages_before_transform={}] [pages_after_transform={}]", //
                             ns_id,
                             kvstore_remain_pages_after_transform == 0,
                             kvstore_remain_pages,
                             kvstore_remain_pages_after_transform);

                if (kvstore_remain_pages_after_transform != 0)
                {
                    throw Exception("KVStore transform failed. Still have some data exist in V2", ErrorCodes::LOGICAL_ERROR);
                }
            }
            else // no need do transform
            {
                LOG_FMT_INFO(log, "Current kvstore translate already done before restored.");
            }

            // change run_mode to ONLY_V3
            page_storage_v2 = nullptr;

            // Must use PageStorageRunMode::ONLY_V3 here.
            page_writer = std::make_shared<PageWriter>(PageStorageRunMode::ONLY_V3, /*storage_v2_*/ nullptr, page_storage_v3);
            page_reader = std::make_shared<PageReader>(PageStorageRunMode::ONLY_V3, ns_id, /*storage_v2_*/ nullptr, page_storage_v3, global_context.getReadLimiter());

            run_mode = PageStorageRunMode::ONLY_V3;
            break;
        }
        }

        CurrentMetrics::set(CurrentMetrics::RegionPersisterRunMode, static_cast<UInt8>(run_mode));
        LOG_FMT_INFO(log, "RegionPersister running. Current Run Mode is {}", static_cast<UInt8>(run_mode));
    }

    RegionMap regions;
    if (page_reader)
    {
        auto acceptor = [&](const DB::Page & page) {
            // We will traverse the pages in V3 before traverse the pages in V2 When we used MIX MODE
            // If we found the page_id has been restored, just skip it.
            if (const auto it = regions.find(page.page_id); it != regions.end())
            {
                LOG_FMT_INFO(log, "Already exist [page_id={}], skip it.", page.page_id);
                return;
            }

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

FileUsageStatistics RegionPersister::getFileUsageStatistics() const
{
    return page_reader->getFileUsageStatistics();
}

} // namespace DB
