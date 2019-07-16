#pragma once

#include <common/logger_useful.h>

#include <Storages/Page/PageStorage.h>
#include <Storages/Transaction/RegionClientCreate.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;
using RegionMap = std::unordered_map<RegionID, RegionPtr>;

class RegionPersistLock;

class RegionPersister final : private boost::noncopyable
{
public:
    RegionPersister(const std::string & storage_path, const PageStorage::Config & config = {})
        : page_storage(storage_path, config), log(&Logger::get("RegionPersister"))
    {}

    void drop(RegionID region_id);
    void persist(const Region & region);
    void persist(const Region & region, const RegionPersistLock & lock);
    void restore(RegionMap & regions, RegionClientCreateFunc * func = nullptr);
    bool gc();

    using RegionWriteBuffer = std::tuple<RegionID, MemoryWriteBuffer, size_t, UInt64>;
    static void computeRegionWriteBuffer(const Region & region, RegionWriteBuffer & region_write_buffer);

private:
    void doPersist(RegionWriteBuffer & region_write_buffer, const RegionPersistLock & lock);
    void doPersist(const Region & region, const RegionPersistLock * lock);

private:
    PageStorage page_storage;

    std::mutex mutex;
    Logger * log;
};
} // namespace DB
