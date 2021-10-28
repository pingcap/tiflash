#pragma once

#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/Page/V1/PageStorage.h>
#include <Storages/Page/V1/WriteBatch.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V2/WriteBatch.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>

namespace DB
{
class Context;

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
    RegionMap restore(const TiFlashRaftProxyHelper * proxy_helper = nullptr, PS::V2::PageStorage::Config config = PS::V2::PageStorage::Config{});
    bool gc();

    using RegionCacheWriteElement = std::tuple<RegionID, MemoryWriteBuffer, size_t, UInt64>;
    static void computeRegionWriteBuffer(const Region & region, RegionCacheWriteElement & region_write_buffer);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    void doPersist(RegionCacheWriteElement & region_write_buffer, const RegionTaskLock & lock, const Region & region);
    void doPersist(const Region & region, const RegionTaskLock * lock);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    Context & global_context;
    std::shared_ptr<PS::V2::PageStorage> page_storage;
    std::shared_ptr<PS::V1::PageStorage> stable_page_storage;

    const RegionManager & region_manager;
    std::mutex mutex;
    Poco::Logger * log;
};
} // namespace DB
