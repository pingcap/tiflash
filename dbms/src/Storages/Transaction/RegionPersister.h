#pragma once

#include <functional>
#include <map>

#include <common/logger_useful.h>

#include <Storages/Page/PageStorage.h>
#include <Storages/Transaction/Region.h>

namespace DB
{

using RegionMap = std::unordered_map<RegionID, RegionPtr>;
using RegionIndexMap = std::unordered_map<RegionID, UInt64>;

class RegionPersister final : private boost::noncopyable
{
public:
    RegionPersister(const std::string & storage_path, const PageStorage::Config & config = {}) : page_storage(storage_path, config), log(&Logger::get("RegionPersister")) {}

    void drop(RegionID region_id);
    void persist(const RegionPtr & region, enginepb::CommandResponse * response = nullptr);
    void restore(RegionMap & regions, Region::RegionClientCreateFunc * func = nullptr);
    bool gc();
private:
    void doPersist(const RegionPtr & region, enginepb::CommandResponse * response = nullptr);

private:
    PageStorage page_storage;

    std::mutex mutex;
    Logger * log;
};
} // namespace DB