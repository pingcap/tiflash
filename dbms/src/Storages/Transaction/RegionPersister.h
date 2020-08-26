#pragma once

#include <Storages/Page/stable/PageStorage.h>
#include <Storages/Transaction/IndexReaderCreate.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;
using RegionMap = std::unordered_map<RegionID, RegionPtr>;

class RegionTaskLock;
class RegionManager;

class RegionPersister final : private boost::noncopyable
{
public:
    RegionPersister(
        const std::string & storage_path, const RegionManager & region_manager_, const FileProviderPtr & file_provider, const stable::PageStorage::Config & config = {})
        : page_storage("RegionPersister", storage_path, config, file_provider), region_manager(region_manager_), log(&Logger::get("RegionPersister"))
    {}

    void drop(RegionID region_id, const RegionTaskLock &);
    void persist(const Region & region);
    void persist(const Region & region, const RegionTaskLock & lock);
    RegionMap restore(IndexReaderCreateFunc * func = nullptr);
    bool gc();

    using RegionCacheWriteElement = std::tuple<RegionID, MemoryWriteBuffer, size_t, UInt64>;
    static void computeRegionWriteBuffer(const Region & region, RegionCacheWriteElement & region_write_buffer);

private:
    void doPersist(RegionCacheWriteElement & region_write_buffer, const RegionTaskLock & lock, const Region & region);
    void doPersist(const Region & region, const RegionTaskLock * lock);

private:
    DB::stable::PageStorage page_storage;
    const RegionManager & region_manager;
    std::mutex mutex;
    Logger * log;
};
} // namespace DB
