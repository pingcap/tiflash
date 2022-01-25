#include <Interpreters/Context.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/Transaction/BackgroundService.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TiFlashContext.h>

namespace DB
{
BackgroundService::BackgroundService(TiFlashContext & tmt_)
    : tmt(tmt_)
    , background_pool(tmt.getContext().getBackgroundPool())
    , log(&Poco::Logger::get("BackgroundService"))
{
    if (!tmt.isInitialized())
        throw Exception("TiFlashContext is not initialized", ErrorCodes::LOGICAL_ERROR);

    single_thread_task_handle = background_pool.addTask(
        [this] {
            tmt.getKVStore()->gcRegionPersistedCache();
            return false;
        },
        false);

    if (!tmt.isBgFlushDisabled())
    {
        table_flush_handle = background_pool.addTask([this] {
            RegionTable & region_table = tmt.getRegionTable();

            return region_table.tryFlushRegions();
        });

        region_handle = background_pool.addTask([this] {
            bool ok = false;

            {
                RegionPtr region = nullptr;
                {
                    std::lock_guard<std::mutex> lock(region_mutex);
                    if (!regions_to_flush.empty())
                    {
                        auto it = regions_to_flush.begin();
                        region = it->second;
                        regions_to_flush.erase(it);
                        ok = true;
                    }
                }
                if (region)
                    tmt.getRegionTable().tryFlushRegion(region, true);
            }
            return ok;
        });

        {
            std::vector<RegionPtr> regions;
            tmt.getKVStore()->traverseRegions([&regions](RegionID, const RegionPtr & region) {
                if (region->dataSize())
                    regions.emplace_back(region);
            });
        }
    }
    else
    {
        LOG_INFO(log, "Configuration raft.disable_bg_flush is set to true, background flush tasks are disabled.");
        auto & global_settings = tmt.getContext().getSettingsRef();
        storage_gc_handle = background_pool.addTask(
            [this] { return tmt.getGCManager().work(); },
            false,
            /*interval_ms=*/global_settings.dt_bg_gc_check_interval * 1000);
        LOG_INFO(log, "Start background storage gc worker with interval " << global_settings.dt_bg_gc_check_interval << " seconds.");
    }
}

void BackgroundService::addRegionToFlush(const DB::RegionPtr & region)
{
    if (tmt.isBgFlushDisabled())
        throw Exception("Try to addRegionToFlush while background flush is disabled.", ErrorCodes::LOGICAL_ERROR);

    {
        std::lock_guard<std::mutex> lock(region_mutex);
        regions_to_flush.emplace(region->id(), region);
    }
    region_handle->wake();
}

BackgroundService::~BackgroundService()
{
    if (single_thread_task_handle)
    {
        background_pool.removeTask(single_thread_task_handle);
        single_thread_task_handle = nullptr;
    }
    if (table_flush_handle)
    {
        background_pool.removeTask(table_flush_handle);
        table_flush_handle = nullptr;
    }

    if (region_handle)
    {
        background_pool.removeTask(region_handle);
        region_handle = nullptr;
    }
    if (storage_gc_handle)
    {
        background_pool.removeTask(storage_gc_handle);
        storage_gc_handle = nullptr;
    }
}

} // namespace DB
