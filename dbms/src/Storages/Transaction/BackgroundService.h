#pragma once

#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Storages/Transaction/RegionDataRead.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>

#include <boost/noncopyable.hpp>
#include <memory>
#include <queue>

namespace DB
{

class TMTContext;
class Region;
using RegionPtr = std::shared_ptr<Region>;
using Regions = std::vector<RegionPtr>;
using RegionMap = std::unordered_map<RegionID, RegionPtr>;

class BackgroundService : boost::noncopyable
{
public:
    BackgroundService(TMTContext &);

    ~BackgroundService();

    void addRegionToFlush(const RegionPtr & region);

private:
    TMTContext & tmt;
    BackgroundProcessingPool & background_pool;

    Logger * log;

    std::mutex region_mutex;
    RegionMap regions_to_flush;

    BackgroundProcessingPool::TaskHandle single_thread_task_handle;
    BackgroundProcessingPool::TaskHandle table_flush_handle;
    BackgroundProcessingPool::TaskHandle region_handle;
    BackgroundProcessingPool::TaskHandle storage_gc_handle;
};

} // namespace DB
