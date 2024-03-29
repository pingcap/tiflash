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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <IO/Buffer/MemoryReadWriteBuffer.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/KVStore/MultiRaft/RegionManager.h>
#include <Storages/KVStore/MultiRaft/RegionPersister.h>
#include <Storages/KVStore/Region.h>
#include <Storages/Page/ConfigSettings.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/WriteBatchImpl.h>
#include <Storages/Page/WriteBatchWrapperImpl.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>
#include <fiu.h>

#include <chrono>
#include <magic_enum.hpp>
#include <memory>
#include <thread>

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
extern const char pause_when_persist_region[];
extern const char random_region_persister_latency_failpoint[];
} // namespace FailPoints

void RegionPersister::drop(RegionID region_id, const RegionTaskLock &)
{
    DB::WriteBatchWrapper wb{run_mode, getWriteBatchPrefix()};
    wb.delPage(region_id);
    page_writer->write(std::move(wb), global_context.getWriteLimiter());
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
            "Persisting big region={} with data info: {}, serialized_size={}",
            region.toString(true),
            region.dataInfo(),
            region_size);
    }
}

size_t RegionPersister::computeRegionWriteBuffer(const Region & region, WriteBuffer & buffer)
{
    auto region_size = 0;
    std::tie(region_size, std::ignore) = region.serialize(buffer);
    return region_size;
}

void RegionPersister::persist(const Region & region, const RegionTaskLock & lock)
{
    // Support only one thread persist.
    RegionCacheWriteElement region_buffer;
    computeRegionWriteBuffer(region, region_buffer);

    doPersist(region_buffer, lock, region);
}

void RegionPersister::doPersist(
    RegionCacheWriteElement & region_write_buffer,
    const RegionTaskLock & region_task_lock,
    const Region & region)
{
    auto & [region_id, buffer, region_size, applied_index] = region_write_buffer;

    auto entry = page_reader->getPageEntry(region_id);
    if (entry.isValid() && entry.tag > applied_index)
        return;

    if (region.isPendingRemove())
    {
        LOG_DEBUG(log, "no need to persist {} because of pending remove", region.toString(false));
        return;
    }

    auto read_buf = buffer.tryGetReadBuffer();
    RUNTIME_CHECK_MSG(read_buf != nullptr, "failed to gen buffer for {}", region.toString(true));
    DB::WriteBatchWrapper wb{run_mode, getWriteBatchPrefix()};
    wb.putPage(region_id, applied_index, read_buf, region_size);
    page_writer->write(std::move(wb), global_context.getWriteLimiter());

#ifdef FIU_ENABLE
    fiu_do_on(FailPoints::pause_when_persist_region, {
        if (auto v = FailPointHelper::getFailPointVal(FailPoints::pause_when_persist_region); v)
        {
            // Only pause for the given region_id
            auto pause_region_id = std::any_cast<RegionID>(v.value());
            if (region_id == pause_region_id)
            {
                SYNC_FOR("before_RegionPersister::persist_write_done");
            }
        }
        else
        {
            // Pause for all persisting requests
            SYNC_FOR("before_RegionPersister::persist_write_done");
        }
    });
    fiu_do_on(FailPoints::random_region_persister_latency_failpoint, {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1ms);
    });
#endif

    region.updateLastCompactLogApplied(region_task_lock);
}

RegionPersister::RegionPersister(Context & global_context_)
    : global_context(global_context_)
    , run_mode(global_context.getPageStorageRunMode())
    , log(Logger::get())
{}

PageStorageConfig RegionPersister::getPageStorageSettings() const
{
    return page_writer->getSettings();
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
            throw Exception(
                fmt::format(
                    "Can't transform kvstore from V2 to V3, [page_id={}] {}",
                    page.page_id,
                    page_transform_entry.toDebugString()),
                ErrorCodes::LOGICAL_ERROR);
        }

        write_batch_transform.putPage(
            page.page_id, //
            page_transform_entry.tag,
            std::make_shared<ReadBufferFromMemory>(page.data.begin(), page.data.size()),
            page.data.size());

        // Will rewrite into V3 one by one.
        // The region data is big. It is not a good idea to combine pages.
        page_writer->writeIntoV3(std::move(write_batch_transform), nullptr);

        // Record del page_id
        write_batch_del_v2.delPage(page.page_id);
    };

    page_reader->traverse(meta_transform_acceptor, /*only_v2*/ true, /*only_v3*/ false);

    // DEL must call after rewrite.
    page_writer->writeIntoV2(std::move(write_batch_del_v2), nullptr);
}

RegionMap RegionPersister::restore(
    PathPool & path_pool,
    const TiFlashRaftProxyHelper * proxy_helper,
    PageStorageConfig config)
{
    {
        auto delegator = path_pool.getPSDiskDelegatorRaft();
        auto provider = global_context.getFileProvider();

        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            // If there is no PageFile with basic version binary format, use version 2 of PageStorage.
            auto detect_binary_version = DB::PS::V2::PageStorage::getMaxDataVersion(provider, delegator);
            if (detect_binary_version == PageFormat::V1)
            {
                LOG_WARNING(log, "Detect V1 format data, and we will read it using V2 format code.");
            }
            auto page_storage_v2 = std::make_shared<PS::V2::PageStorage>(
                "RegionPersister",
                delegator,
                config,
                provider,
                global_context.getPSBackgroundPool());
            page_storage_v2->restore();
            page_writer = std::make_shared<PageWriter>(
                run_mode,
                StorageType::KVStore,
                page_storage_v2,
                /*storage_v3_*/ nullptr,
                /*uni_ps_*/ nullptr);
            page_reader = std::make_shared<PageReader>(
                run_mode,
                NullspaceID,
                StorageType::KVStore,
                ns_id,
                page_storage_v2,
                /*storage_v3_*/ nullptr,
                /*uni_ps_*/ nullptr,
                /*readlimiter*/ global_context.getReadLimiter());
            break;
        }
        case PageStorageRunMode::ONLY_V3:
        {
            mergeConfigFromSettings(global_context.getSettingsRef(), config);

            auto page_storage_v3 = std::make_shared<PS::V3::PageStorageImpl>( //
                "RegionPersister",
                path_pool.getPSDiskDelegatorGlobalMulti(PathPool::kvstore_path_prefix),
                config,
                provider);
            page_storage_v3->restore();
            page_writer = std::make_shared<PageWriter>(
                run_mode,
                StorageType::KVStore,
                /*storage_v2_*/ nullptr,
                page_storage_v3,
                /*uni_ps_*/ nullptr);
            page_reader = std::make_shared<PageReader>(
                run_mode,
                NullspaceID,
                StorageType::KVStore,
                ns_id,
                /*storage_v2_*/ nullptr,
                page_storage_v3,
                /*uni_ps_*/ nullptr,
                global_context.getReadLimiter());
            break;
        }
        case PageStorageRunMode::MIX_MODE:
        {
            // The ps v2 instance will be destroyed soon after transform its data to v3,
            // so we can safely use some aggressive gc config for it.
            auto page_storage_v2 = std::make_shared<PS::V2::PageStorage>(
                "RegionPersister",
                delegator,
                DB::PageStorageConfig::getEasyGCConfig(),
                provider,
                global_context.getPSBackgroundPool());
            // V3 should not used getPSDiskDelegatorRaft
            // Because V2 will delete all invalid(unrecognized) file when it restore
            auto page_storage_v3 = std::make_shared<PS::V3::PageStorageImpl>( //
                "RegionPersister",
                path_pool.getPSDiskDelegatorGlobalMulti(PathPool::kvstore_path_prefix),
                config,
                provider);

            page_storage_v2->restore();
            page_storage_v3->restore();

            if (const auto & kvstore_remain_pages = page_storage_v2->getNumberOfPages(); kvstore_remain_pages != 0)
            {
                page_writer = std::make_shared<PageWriter>(
                    run_mode,
                    StorageType::KVStore,
                    page_storage_v2,
                    page_storage_v3,
                    /*uni_ps_*/ nullptr);
                page_reader = std::make_shared<PageReader>(
                    run_mode,
                    NullspaceID,
                    StorageType::KVStore,
                    ns_id,
                    page_storage_v2,
                    page_storage_v3,
                    /*uni_ps_*/ nullptr,
                    global_context.getReadLimiter());

                LOG_INFO(
                    log,
                    "Current kvstore transform to V3 begin [pages_before_transform={}]",
                    kvstore_remain_pages);
                forceTransformKVStoreV2toV3();
                const auto & kvstore_remain_pages_after_transform = page_storage_v2->getNumberOfPages();
                LOG_INFO(
                    log,
                    "Current kvstore transform to V3 finished. [ns_id={}] [done={}] [pages_before_transform={}] "
                    "[pages_after_transform={}]", //
                    ns_id,
                    kvstore_remain_pages_after_transform == 0,
                    kvstore_remain_pages,
                    kvstore_remain_pages_after_transform);

                if (kvstore_remain_pages_after_transform != 0)
                {
                    throw Exception(
                        "KVStore transform failed. Still have some data exist in V2",
                        ErrorCodes::LOGICAL_ERROR);
                }
            }
            else // no need do transform
            {
                LOG_INFO(log, "Current kvstore transform already done before restored.");
            }
            // running gc on v2 to decrease its disk space usage
            page_storage_v2->gcImpl(/*not_skip=*/true, nullptr, nullptr);

            // change run_mode to ONLY_V3
            page_storage_v2 = nullptr;

            // Must use PageStorageRunMode::ONLY_V3 here.
            page_writer = std::make_shared<PageWriter>(
                PageStorageRunMode::ONLY_V3,
                StorageType::KVStore,
                /*storage_v2_*/ nullptr,
                page_storage_v3,
                /*uni_ps_*/ nullptr);
            page_reader = std::make_shared<PageReader>(
                PageStorageRunMode::ONLY_V3,
                NullspaceID,
                StorageType::KVStore,
                ns_id,
                /*storage_v2_*/ nullptr,
                page_storage_v3,
                /*uni_ps_*/ nullptr,
                global_context.getReadLimiter());

            run_mode = PageStorageRunMode::ONLY_V3;
            break;
        }
        case PageStorageRunMode::UNI_PS:
        {
            auto uni_ps = global_context.getWriteNodePageStorage();
            page_writer = std::make_shared<PageWriter>(
                run_mode,
                StorageType::KVStore,
                /*storage_v2_*/ nullptr,
                /*storage_v3_*/ nullptr,
                uni_ps);
            page_reader = std::make_shared<PageReader>(
                run_mode,
                NullspaceID,
                StorageType::KVStore,
                ns_id,
                /*storage_v2_*/ nullptr,
                /*storage_v3_*/ nullptr,
                uni_ps,
                global_context.getReadLimiter());
            break;
        }
        }

        CurrentMetrics::set(CurrentMetrics::RegionPersisterRunMode, static_cast<UInt8>(run_mode));
        LOG_INFO(log, "RegionPersister running. Current Run Mode is {}", magic_enum::enum_name(run_mode));
    }

    RegionMap regions;
    auto acceptor = [&](const DB::Page & page) {
        // We will traverse the pages in V3 before traverse the pages in V2 When we used MIX MODE
        // If we found the page_id has been restored, just skip it.
        if (const auto it = regions.find(page.page_id); it != regions.end())
        {
            LOG_INFO(log, "Already exist [page_id={}], skip it.", page.page_id);
            return;
        }

        ReadBufferFromMemory buf(page.data.begin(), page.data.size());
        auto region = Region::deserialize(buf, proxy_helper);
        RUNTIME_CHECK_MSG(
            page.page_id == region->id(),
            "region_id and page_id not match! region_id={} page_id={}",
            region->id(),
            page.page_id);

        regions.emplace(page.page_id, region);
    };
    page_reader->traverse(acceptor);

    return regions;
}

bool RegionPersister::gc()
{
    PageStorageConfig config = getConfigFromSettings(global_context.getSettingsRef());
    page_writer->reloadSettings(config);
    return page_writer->gc(false, nullptr, nullptr);
}

FileUsageStatistics RegionPersister::getFileUsageStatistics() const
{
    return page_reader->getFileUsageStatistics();
}

} // namespace DB
