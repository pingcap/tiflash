#include <Interpreters/Context.h>
#include <Storages/Transaction/BackgroundService.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionDataMover.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

BackgroundService::BackgroundService(TMTContext & tmt_)
    : tmt(tmt_), background_pool(tmt.getContext().getBackgroundPool()), log(&Logger::get("BackgroundService"))
{
    if (!tmt.isInitialized())
        throw Exception("TMTContext is not initialized", ErrorCodes::LOGICAL_ERROR);

    single_thread_task_handle = background_pool.addTask(
        [this] {
            {
                RegionTable & region_table = tmt.getRegionTable();
                region_table.checkTableOptimize();
            }
            tmt.getKVStore()->gcRegionCache();
            return false;
        },
        false);

    table_flush_handle = background_pool.addTask([this] {
        RegionTable & region_table = tmt.getRegionTable();

        // if all regions of table is removed, try to optimize data.
        if (auto table_id = region_table.popOneTableToOptimize(); table_id != InvalidTableID)
        {
            LOG_INFO(log, "try to final optimize table " << table_id);
            tryOptimizeStorageFinal(tmt.getContext(), table_id);
            LOG_INFO(log, "finish final optimize table " << table_id);
        }
        return region_table.tryFlushRegions();
    });

    region_handle = background_pool.addTask([this] {
        bool ok = false;
        {
            RegionPtr region = nullptr;
            {
                std::lock_guard<std::mutex> lock(region_mutex);
                if (!regions_to_decode.empty())
                {
                    auto it = regions_to_decode.begin();
                    region = it->second;
                    regions_to_decode.erase(it);
                    ok = true;
                }
            }
            if (region)
                region->tryPreDecodeTiKVValue(tmt);
        }

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

        for (const auto & region : regions)
            addRegionToDecode(region);
    }
}

void BackgroundService::addRegionToDecode(const RegionPtr & region)
{
    {
        std::lock_guard<std::mutex> lock(region_mutex);
        regions_to_decode.emplace(region->id(), region);
    }
    region_handle->wake();
}

void BackgroundService::addRegionToFlush(const DB::RegionPtr & region)
{
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
}

} // namespace DB
