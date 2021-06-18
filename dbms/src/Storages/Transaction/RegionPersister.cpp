#include <Common/FailPoint.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Interpreters/Context.h>
#include <Storages/Page/ConfigSettings.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/stable/PageStorage.h>
#include <Storages/PathPool.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionManager.h>
#include <Storages/Transaction/RegionPersister.h>

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
    if (page_storage)
    {
        WriteBatch wb;
        wb.delPage(region_id);
        page_storage->write(std::move(wb));
    }
    else
    {
        DB::stable::WriteBatch wb;
        wb.delPage(region_id);
        stable_page_storage->write(std::move(wb));
    }
}

void RegionPersister::computeRegionWriteBuffer(const Region & region, RegionCacheWriteElement & region_write_buffer)
{
    auto & [region_id, buffer, region_size, applied_index] = region_write_buffer;

    region_id = region.id();
    std::tie(region_size, applied_index) = region.serialize(buffer);
    if (unlikely(region_size > static_cast<size_t>(std::numeric_limits<UInt32>::max())))
    {
        LOG_WARNING(&Logger::get("RegionPersister"),
            "Persisting big region: " << region.toString() << " with data info: " << region.dataInfo() << ", serialized size "
                                      << region_size);
    }
}

void RegionPersister::persist(const Region & region, const RegionTaskLock & lock) { doPersist(region, &lock); }

void RegionPersister::persist(const Region & region) { doPersist(region, nullptr); }

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

    std::lock_guard<std::mutex> lock(mutex);

    if (page_storage)
    {
        auto entry = page_storage->getEntry(region_id);
        if (entry.isValid() && entry.tag > applied_index)
            return;
    }
    else
    {
        auto entry = stable_page_storage->getEntry(region_id);
        if (entry.isValid() && entry.tag > applied_index)
            return;
    }

    if (region.isPendingRemove())
    {
        LOG_DEBUG(log, "no need to persist " << region.toString(false) << " because of pending remove");
        return;
    }

    auto read_buf = buffer.tryGetReadBuffer();
    if (page_storage)
    {
        WriteBatch wb;
        wb.putPage(region_id, applied_index, read_buf, region_size);
        page_storage->write(std::move(wb));
    }
    else
    {
        DB::stable::WriteBatch wb;
        wb.putPage(region_id, applied_index, read_buf, region_size);
        stable_page_storage->write(std::move(wb));
    }
}

RegionPersister::RegionPersister(Context & global_context_, const RegionManager & region_manager_)
    : global_context(global_context_), region_manager(region_manager_), log(&Logger::get("RegionPersister"))
{}

namespace
{
DB::stable::PageStorage::Config getStablePSConfig(const PageStorage::Config & config)
{
    DB::stable::PageStorage::Config c;
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
} // namespace

RegionMap RegionPersister::restore(const TiFlashRaftProxyHelper * proxy_helper, PageStorage::Config config)
{
    {
        auto & path_pool = global_context.getPathPool();
        auto delegator = path_pool.getPSDiskDelegatorRaft();
        // If there is no PageFile with basic version binary format, use the latest version of PageStorage.
        auto detect_binary_version = PageStorage::getMaxDataVersion(global_context.getFileProvider(), delegator);
        bool run_in_compatible_mode = path_pool.isRaftCompatibleModeEnabled() && (detect_binary_version == PageFormat::V1);

        fiu_do_on(FailPoints::force_enable_region_persister_compatible_mode, { run_in_compatible_mode = true; });
        fiu_do_on(FailPoints::force_disable_region_persister_compatible_mode, { run_in_compatible_mode = false; });

        if (!run_in_compatible_mode)
        {
            mergeConfigFromSettings(global_context.getSettingsRef(), config);
            config.num_write_slots = 4; // extend write slots to 4 at least

            LOG_INFO(log, "RegionPersister running in normal mode");
            page_storage = std::make_unique<DB::PageStorage>( //
                "RegionPersister",
                std::move(delegator),
                config,
                global_context.getFileProvider(),
                global_context.getTiFlashMetrics());
            page_storage->restore();
        }
        else
        {
            LOG_INFO(log, "RegionPersister running in compatible mode");
            auto c = getStablePSConfig(config);
            stable_page_storage = std::make_unique<DB::stable::PageStorage>( //
                "RegionPersister", delegator->defaultPath(), c, global_context.getFileProvider());
        }
    }

    RegionMap regions;
    if (page_storage)
    {
        auto acceptor = [&](const Page & page) {
            ReadBufferFromMemory buf(page.data.begin(), page.data.size());
            auto region = Region::deserialize(buf, proxy_helper);
            if (page.page_id != region->id())
                throw Exception("region id and page id not match!", ErrorCodes::LOGICAL_ERROR);
            regions.emplace(page.page_id, region);
        };
        page_storage->traverse(acceptor);
    }
    else
    {
        auto acceptor = [&](const DB::stable::Page & page) {
            ReadBufferFromMemory buf(page.data.begin(), page.data.size());
            auto region = Region::deserialize(buf, proxy_helper);
            if (page.page_id != region->id())
                throw Exception("region id and page id not match!", ErrorCodes::LOGICAL_ERROR);
            regions.emplace(page.page_id, region);
        };
        stable_page_storage->traverse(acceptor);
    }

    return regions;
}

bool RegionPersister::gc()
{
    if (page_storage)
        return page_storage->gc();
    else
        return stable_page_storage->gc();
}

} // namespace DB
