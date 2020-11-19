#pragma once

#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>

namespace DB
{

class Context;
class PageStorage;
namespace stable
{
class PageStorage;
}

class Region;
using RegionPtr = std::shared_ptr<Region>;
using RegionMap = std::unordered_map<RegionID, RegionPtr>;

class RegionTaskLock;
class RegionManager;

struct TiFlashRaftProxyHelper;

class RegionPersister final : private boost::noncopyable
{
public:
    RegionPersister(Context & global_context_, const RegionManager & region_manager_);

    void drop(RegionID region_id, const RegionTaskLock &);
    void persist(const Region & region);
    void persist(const Region & region, const RegionTaskLock & lock);
    RegionMap restore(const TiFlashRaftProxyHelper * proxy_helper = nullptr, DB::PageStorage::Config config = DB::PageStorage::Config{});
    bool gc();

    using RegionCacheWriteElement = std::tuple<RegionID, MemoryWriteBuffer, size_t, UInt64>;
    static void computeRegionWriteBuffer(const Region & region, RegionCacheWriteElement & region_write_buffer);

private:
    void doPersist(RegionCacheWriteElement & region_write_buffer, const RegionTaskLock & lock, const Region & region);
    void doPersist(const Region & region, const RegionTaskLock * lock);

private:
    Context & global_context;
    std::shared_ptr<DB::PageStorage> page_storage;
    std::shared_ptr<DB::stable::PageStorage> stable_page_storage;

    const RegionManager & region_manager;
    std::mutex mutex;
    Logger * log;
};
} // namespace DB
