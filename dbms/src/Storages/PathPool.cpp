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
#include <Common/Logger.h>
#include <Common/escapeForFileName.h>
#include <Core/Types.h>
#include <IO/FileProvider/FileProvider.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/PathPool.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <common/likely.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

#include <mutex>
#include <set>
#include <unordered_map>
#include <utility>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

inline String removeTrailingSlash(String s)
{
    if (s.back() == '/')
        s.erase(s.begin() + s.size() - 1);
    return s;
}

inline String getNormalizedPath(const String & s)
{
    return removeTrailingSlash(Poco::Path{s}.toString());
}

const String PathPool::log_path_prefix = "log";
const String PathPool::data_path_prefix = "data";
const String PathPool::meta_path_prefix = "meta";
const String PathPool::kvstore_path_prefix = "kvstore";
const String PathPool::write_uni_path_prefix = "write";
const String PathPool::read_node_cache_path_prefix = "read_cache";

// Constructor to be used during initialization
PathPool::PathPool(
    const Strings & main_data_paths_,
    const Strings & latest_data_paths_,
    const Strings & kvstore_paths_, //
    PathCapacityMetricsPtr global_capacity_,
    FileProviderPtr file_provider_)
    : main_data_paths(main_data_paths_)
    , latest_data_paths(latest_data_paths_)
    , kvstore_paths(kvstore_paths_)
    , global_capacity(global_capacity_)
    , file_provider(file_provider_)
    , log(Logger::get())
{
    if (kvstore_paths.empty())
    {
        // Set default path generated from latest_data_paths
        for (const auto & s : latest_data_paths)
        {
            // Get a normalized path without trailing '/'
            auto p = getNormalizedPath(s + "/" + PathPool::kvstore_path_prefix);
            kvstore_paths.emplace_back(std::move(p));
        }
    }
    for (const auto & s : latest_data_paths)
    {
        // Get a normalized path without trailing '/'
        auto p = getNormalizedPath(s + "/page");
        global_page_paths.emplace_back(std::move(p));
    }
}

StoragePathPool PathPool::withTable(const String & database_, const String & table_, bool path_need_database_name_)
    const
{
    return StoragePathPool(
        main_data_paths,
        latest_data_paths,
        database_,
        table_,
        path_need_database_name_,
        global_capacity,
        file_provider);
}

Strings PathPool::listPaths() const
{
    std::set<String> path_set;
    for (const auto & p : main_data_paths)
        path_set.insert(getNormalizedPath(p + "/data"));
    for (const auto & p : latest_data_paths)
        path_set.insert(getNormalizedPath(p + "/data"));
    Strings paths;
    for (const auto & p : path_set)
        paths.emplace_back(p);
    return paths;
}

PSDiskDelegatorPtr PathPool::getPSDiskDelegatorRaft()
{
    return std::make_shared<PSDiskDelegatorRaft>(*this);
}

PSDiskDelegatorPtr PathPool::getPSDiskDelegatorGlobalMulti(const String & prefix) const
{
    return std::make_shared<PSDiskDelegatorGlobalMulti>(*this, prefix);
}

PSDiskDelegatorPtr PathPool::getPSDiskDelegatorGlobalSingle(const String & prefix) const
{
    return std::make_shared<PSDiskDelegatorGlobalSingle>(*this, prefix);
}

PSDiskDelegatorPtr PathPool::getPSDiskDelegatorFixedDirectory(const String & dir) const
{
    return std::make_shared<PSDiskDelegatorFixedDirectory>(*this, dir);
}

//==========================================================================================
// StoragePathPool
//==========================================================================================

StoragePathPool::StoragePathPool( //
    const Strings & main_data_paths,
    const Strings & latest_data_paths, //
    String database_,
    String table_,
    bool path_need_database_name_, //
    PathCapacityMetricsPtr global_capacity_,
    FileProviderPtr file_provider_)
    : database(std::move(database_))
    , table(std::move(table_))
    , keyspace_id(SchemaNameMapper::getMappedNameKeyspaceID(table_))
    , path_need_database_name(path_need_database_name_)
    , shutdown_called(false)
    , global_capacity(std::move(global_capacity_))
    , file_provider(std::move(file_provider_))
    , log(Logger::get())
{
    RUNTIME_CHECK_MSG(
        !database.empty() && !table.empty(),
        "Can NOT create StoragePathPool [database={}] [table={}]",
        database,
        table);

    for (const auto & p : main_data_paths)
    {
        MainPathInfo info;
        info.path = getStorePath(p + "/data", database, table);
        main_path_infos.emplace_back(info);
    }
    for (const auto & p : latest_data_paths)
    {
        LatestPathInfo info;
        info.path = getStorePath(p + "/data", database, table);
        latest_path_infos.emplace_back(info);
    }
}

StoragePathPool::StoragePathPool(StoragePathPool && rhs) noexcept
    : main_path_infos(std::move(rhs.main_path_infos))
    , latest_path_infos(std::move(rhs.latest_path_infos))
    , database(std::move(rhs.database))
    , table(std::move(rhs.table))
    , keyspace_id(rhs.keyspace_id)
    , dt_file_path_map(std::move(rhs.dt_file_path_map))
    , path_need_database_name(rhs.path_need_database_name)
    , shutdown_called(rhs.shutdown_called.load())
    , global_capacity(std::move(rhs.global_capacity))
    , file_provider(std::move(rhs.file_provider))
    , log(std::move(rhs.log))
{}

StoragePathPool & StoragePathPool::operator=(StoragePathPool && rhs)
{
    if (this != &rhs)
    {
        main_path_infos.swap(rhs.main_path_infos);
        latest_path_infos.swap(rhs.latest_path_infos);
        dt_file_path_map.swap(rhs.dt_file_path_map);
        database.swap(rhs.database);
        table.swap(rhs.table);
        keyspace_id = rhs.keyspace_id;
        path_need_database_name = rhs.path_need_database_name;
        shutdown_called = rhs.shutdown_called.load();
        global_capacity.swap(rhs.global_capacity);
        file_provider.swap(rhs.file_provider);
        log.swap(rhs.log);
    }
    return *this;
}

bool StoragePathPool::createPSV2DeleteMarkFile()
{
    try
    {
        return Poco::File(getPSV2DeleteMarkFilePath()).createFile();
    }
    catch (...)
    {
        tryLogCurrentException(log);
        return false;
    }
}

bool StoragePathPool::isPSV2Deleted() const
{
    return Poco::File(getPSV2DeleteMarkFilePath()).exists();
}

void StoragePathPool::clearPSV2ObsoleteData()
{
    std::lock_guard lock{mutex};
    auto drop_instance_data = [&](const String & prefix) {
        for (auto & path_info : latest_path_infos)
        {
            try
            {
                auto path = fmt::format("{}/{}", path_info.path, prefix);
                if (Poco::File dir(path); dir.exists())
                {
                    // This function is used to clean obsolete data in ps v2 instance at restart,
                    // so no need to update global_capacity here.
                    LOG_INFO(log, "Begin to drop obsolete data[dir={}]", path);
                    file_provider->deleteDirectory(dir.path(), false, true);
                }
            }
            catch (...)
            {
                tryLogCurrentException(log);
            }
        }
    };
    drop_instance_data("meta");
    drop_instance_data("log");
    drop_instance_data("data");
}

void StoragePathPool::rename(const String & new_database, const String & new_table)
{
    RUNTIME_CHECK(!new_database.empty() && !new_table.empty(), database, table, new_database, new_table);
    RUNTIME_CHECK(!path_need_database_name, database, table, new_database, new_table);

    // The directories for storing table data is not changed, just rename related names.
    std::lock_guard lock{mutex};
    database = new_database;
    table = new_table;
}

void StoragePathPool::drop(bool recursive, bool must_success)
{
    std::lock_guard lock{mutex};
    for (auto & path_info : main_path_infos)
    {
        try
        {
            if (Poco::File dir(path_info.path); dir.exists())
            {
                LOG_INFO(log, "Begin to drop [dir={}] from main_path_infos", path_info.path);
                file_provider->deleteDirectory(dir.path(), false, recursive);

                // update global used size
                size_t total_bytes = 0;
                for (const auto & [file_id, file_size] : path_info.file_size_map)
                {
                    (void)file_id;
                    total_bytes += file_size;
                }
                global_capacity->freeUsedSize(path_info.path, total_bytes);
                // clear in case delegator->removeDTFile is called after `drop`
                dt_file_path_map.clear();
            }
        }
        catch (Poco::DirectoryNotEmptyException & e)
        {
            if (must_success)
                throw;
            else
            {
                // just ignore and keep that directory if it is not empty
                LOG_WARNING(log, "Can not remove directory: {}, it is not empty", path_info.path);
            }
        }
    }
    for (auto & path_info : latest_path_infos)
    {
        try
        {
            if (Poco::File dir(path_info.path); dir.exists())
            {
                LOG_INFO(log, "Begin to drop [dir={}] from latest_path_infos", path_info.path);
                file_provider->deleteDirectory(dir.path(), false, recursive);

                // When PageStorage is dropped, it will update the size in global_capacity.
                // Don't need to update global_capacity here.
            }
        }
        catch (Poco::DirectoryNotEmptyException & e)
        {
            if (must_success)
                throw;
            else
            {
                // just ignore and keep that directory if it is not empty
                LOG_WARNING(log, "Can not remove directory: {}, it is not empty", path_info.path);
            }
        }
    }
    for (const auto & [local_file_id, size_entry] : remote_dt_file_size_map)
    {
        UNUSED(local_file_id);
        global_capacity->freeRemoteUsedSize(keyspace_id, size_entry.second);
    }
    remote_dt_file_size_map.clear();
}

//==========================================================================================
// private methods
//==========================================================================================

String StoragePathPool::getStorePath(
    const String & extra_path_root,
    const String & database_name,
    const String & table_name) const
{
    if (likely(!path_need_database_name))
        return getNormalizedPath(fmt::format("{}/{}", extra_path_root, escapeForFileName(table_name)));
    else
        return getNormalizedPath(
            fmt::format("{}/{}/{}", extra_path_root, escapeForFileName(database_name), escapeForFileName(table_name)));
}

void StoragePathPool::renamePath(const String & old_path, const String & new_path)
{
    LOG_INFO(log, "Renaming {} to {}", old_path, new_path);
    if (auto file = Poco::File{old_path}; file.exists())
        file.renameTo(new_path);
    else
        LOG_WARNING(log, "Path \"{}\" is missed.", old_path);
}

String StoragePathPool::getPSV2DeleteMarkFilePath() const
{
    return fmt::format("{}/{}", latest_path_infos[0].path, "PS_V2_DELETED");
}

//==========================================================================================
// Generic functions
//==========================================================================================

template <typename T>
String genericChoosePath(
    const std::vector<T> & paths,
    const PathCapacityMetricsPtr & global_capacity,
    std::function<String(const std::vector<T> & paths, size_t idx)> path_generator,
    std::function<String(const T & path_info)> path_getter,
    const LoggerPtr & log,
    const String & log_msg)
{
    if (paths.size() == 1)
        return path_generator(paths, 0);

    UInt64 total_available_size = 0;
    std::vector<FsStats> stats;
    for (size_t i = 0; i < paths.size(); ++i)
    {
        stats.emplace_back(std::get<0>(global_capacity->getFsStatsOfPath(path_getter(paths[i]))));
        total_available_size += stats.back().avail_size;
    }

    // We should choose path even if there is no available space.
    // If the actual disk space is running out, let the later `write` to throw exception.
    // If available space is limited by the quota, then write down a GC-ed file can make
    // some files be deleted later.
    if (total_available_size == 0)
        LOG_WARNING(log, "No available space for all disks, choose randomly.");
    std::vector<double> ratio;
    for (auto & stat : stats)
    {
        if (likely(total_available_size != 0))
            ratio.push_back(1.0 * stat.avail_size / total_available_size);
        else
        {
            // No available space for all disks, choose randomly
            ratio.push_back(1.0 / paths.size());
        }
    }

    double rand_number = static_cast<double>(rand()) / RAND_MAX; // NOLINT(cert-msc50-cpp)
    double ratio_sum = 0.0;
    for (size_t i = 0; i < ratio.size(); i++)
    {
        ratio_sum += ratio[i];
        if ((rand_number < ratio_sum) || (i == ratio.size() - 1))
        {
            LOG_INFO(log, "Choose path [index={}] {}", i, log_msg);
            return path_generator(paths, i);
        }
    }
    throw Exception("Should not reach here", ErrorCodes::LOGICAL_ERROR);
}

//==========================================================================================
// Stable data
//==========================================================================================

Strings StableDiskDelegator::listPaths() const
{
    std::vector<String> paths;
    for (auto & main_path_info : pool.main_path_infos)
    {
        paths.push_back(fmt::format("{}/{}", main_path_info.path, StoragePathPool::STABLE_FOLDER_NAME));
    }
    return paths;
}

String StableDiskDelegator::choosePath() const
{
    std::function<String(const StoragePathPool::MainPathInfos & paths, size_t idx)> path_generator
        = [](const StoragePathPool::MainPathInfos & paths, size_t idx) -> String {
        return fmt::format("{}/{}", paths[idx].path, StoragePathPool::STABLE_FOLDER_NAME);
    };

    std::function<String(const StoragePathPool::MainPathInfo & info)> path_getter
        = [](const StoragePathPool::MainPathInfo & info) -> String {
        return info.path;
    };

    const String log_msg = fmt::format("[type=stable] [database={}] [table={}]", pool.database, pool.table);
    return genericChoosePath(
        pool.main_path_infos,
        pool.global_capacity,
        path_generator,
        path_getter,
        pool.log,
        log_msg);
}

String StableDiskDelegator::getDTFilePath(UInt64 file_id, bool throw_on_not_exist) const
{
    std::lock_guard lock{pool.mutex};
    auto iter = pool.dt_file_path_map.find(file_id);
    if (likely(iter != pool.dt_file_path_map.end()))
        return fmt::format("{}/{}", pool.main_path_infos[iter->second].path, StoragePathPool::STABLE_FOLDER_NAME);
    if (likely(throw_on_not_exist))
        throw Exception(fmt::format("Can not find path for DMFile, file_id={}", file_id));
    return "";
}

void StableDiskDelegator::addDTFile(UInt64 file_id, size_t file_size, std::string_view path)
{
    // remove '/stable' added in listPathsForStable/getDTFilePath
    path.remove_suffix(1 + strlen(StoragePathPool::STABLE_FOLDER_NAME));
    std::lock_guard lock{pool.mutex};
    if (auto iter = pool.dt_file_path_map.find(file_id); unlikely(iter != pool.dt_file_path_map.end()))
    {
        const auto & path_info = pool.main_path_infos[iter->second];
        throw DB::TiFlashException(
            fmt::format(
                "Try to add a DTFile with duplicated id, file_id={} path={} existed_path={}",
                file_id,
                path,
                path_info.path),
            Errors::DeltaTree::Internal);
    }

    UInt32 index = UINT32_MAX;
    for (size_t i = 0; i < pool.main_path_infos.size(); i++)
    {
        if (pool.main_path_infos[i].path == path)
        {
            index = i;
            break;
        }
    }
    RUNTIME_CHECK_MSG(
        index != UINT32_MAX,
        "Try to add a DTFile to an unrecognized path. file_id={} path={}",
        file_id,
        path);
    pool.dt_file_path_map.emplace(file_id, index);
    pool.main_path_infos[index].file_size_map.emplace(file_id, file_size);

#ifndef NDEBUG
    try
    {
        auto dmf_path = fmt::format("{}/stable/dmf_{}", path, file_id);
        Poco::File dmf_file = {dmf_path};
        if (dmf_file.isFile())
        {
            LOG_DEBUG(
                pool.log,
                "added new dtfile. [id={}] [path={}] [real_size={}] [reported_size={}]",
                file_id,
                path,
                dmf_file.getSize(),
                file_size);
        }
        else
        {
            size_t size_sum = 0;
            auto get_folder_size = [](const Poco::File & target, size_t & counter) -> void {
                auto get_folder_size_impl
                    = [](const Poco::File & inner_target, size_t & inner_counter, auto & self) -> void {
                    std::vector<Poco::File> files;
                    inner_target.list(files);
                    for (auto & i : files)
                    {
                        if (i.isFile())
                        {
                            inner_counter += i.getSize();
                        }
                        else
                        {
                            self(i, inner_counter, self);
                        }
                    }
                };
                get_folder_size_impl(target, counter, get_folder_size_impl);
            };
            get_folder_size(dmf_file, size_sum);
            LOG_DEBUG(
                pool.log,
                "added new dtfile. [id={}] [path={}] [real_size={}] [reported_size={}]",
                file_id,
                path,
                size_sum,
                file_size);
        }
    }
    catch (const Poco::Exception & exp)
    {
        LOG_WARNING(
            pool.log,
            "failed to get real size info for dtfile. [id={}] [path={}] [err={}]",
            file_id,
            path,
            exp.displayText());
    }
#endif

    // update global used size
    pool.global_capacity->addUsedSize(path, file_size);
}

bool StableDiskDelegator::updateDTFileSize(UInt64 file_id, size_t file_size)
{
    std::lock_guard lock{pool.mutex};
    auto iter = pool.dt_file_path_map.find(file_id);
    if (iter == pool.dt_file_path_map.end())
    {
        return false;
    }
    auto index = iter->second;
    auto it = pool.main_path_infos[index].file_size_map.find(file_id);
    if (it == pool.main_path_infos[index].file_size_map.end())
    {
        return false;
    }
    const auto origin_file_size = it->second;
    it->second = file_size;
    // update global used size
    pool.global_capacity->addUsedSize(pool.main_path_infos[index].path, file_size - origin_file_size);
    return true;
}

void StableDiskDelegator::removeDTFile(UInt64 file_id, bool throw_on_not_exist)
{
    std::lock_guard lock{pool.mutex};
    auto iter = pool.dt_file_path_map.find(file_id);
    if (iter == pool.dt_file_path_map.end())
    {
        if (throw_on_not_exist)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find DMFile when removing, file_id={}", file_id);
        else
            return;
    }
    UInt32 index = iter->second;
    const auto file_size = pool.main_path_infos[index].file_size_map.at(file_id);
    pool.dt_file_path_map.erase(file_id);
    pool.main_path_infos[index].file_size_map.erase(file_id);
    // update global used size
    pool.global_capacity->freeUsedSize(pool.main_path_infos[index].path, file_size);
}

void StableDiskDelegator::addRemoteDTFileIfNotExists(UInt64 local_external_id, size_t file_size)
{
    std::lock_guard lock{pool.mutex};
    auto [iter, inserted] = pool.remote_dt_file_size_map.emplace(local_external_id, std::make_pair(true, file_size));
    if (!inserted)
    {
        RUNTIME_CHECK(iter->second.second == file_size, iter->second.second, file_size);
        return;
    }
    // update global used size
    pool.global_capacity->addRemoteUsedSize(pool.keyspace_id, file_size);
}

void StableDiskDelegator::addRemoteDTFileWithGCDisabled(UInt64 local_external_id, size_t file_size)
{
    std::lock_guard lock{pool.mutex};
    auto [_, inserted] = pool.remote_dt_file_size_map.emplace(local_external_id, std::make_pair(false, file_size));
    RUNTIME_CHECK(inserted);
    // update global used size
    pool.global_capacity->addRemoteUsedSize(pool.keyspace_id, file_size);
}

void StableDiskDelegator::enableGCForRemoteDTFile(UInt64 local_page_id)
{
    std::lock_guard lock{pool.mutex};
    auto iter = pool.remote_dt_file_size_map.find(local_page_id);
    // local_page_id may be a ref id, so it may not in remote_dt_file_size_map
    if (iter != pool.remote_dt_file_size_map.end())
    {
        iter->second.first = true;
    }
}

void StableDiskDelegator::removeRemoteDTFile(UInt64 local_external_id, bool throw_on_not_exist)
{
    std::lock_guard lock{pool.mutex};
    auto iter = pool.remote_dt_file_size_map.find(local_external_id);
    if (iter == pool.remote_dt_file_size_map.end())
    {
        if (throw_on_not_exist)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot find remote DMFile when removing, file_id={}",
                local_external_id);
        else
            return;
    }
    // update global used size
    pool.global_capacity->freeRemoteUsedSize(pool.keyspace_id, iter->second.second);
    pool.remote_dt_file_size_map.erase(iter);
}

std::set<UInt64> StableDiskDelegator::getAllRemoteDTFilesForGC()
{
    std::lock_guard lock{pool.mutex};
    std::set<UInt64> all_local_external_ids;
    for (auto & [local_external_id, pair] : pool.remote_dt_file_size_map)
    {
        if (pair.first)
        {
            all_local_external_ids.insert(local_external_id);
        }
    }
    return all_local_external_ids;
}

//==========================================================================================
// Delta data
//==========================================================================================


bool PSDiskDelegatorMulti::fileExist(const PageFileIdAndLevel & /*id_lvl*/) const
{
    throw Exception("Not Implemented", ErrorCodes::NOT_IMPLEMENTED);
}

size_t PSDiskDelegatorMulti::numPaths() const
{
    return pool.latest_path_infos.size();
}

String PSDiskDelegatorMulti::defaultPath() const
{
    return fmt::format("{}/{}", pool.latest_path_infos[default_path_index].path, path_prefix);
}

Strings PSDiskDelegatorMulti::listPaths() const
{
    // The delta data could be stored in all direcotries.
    std::vector<String> paths;
    for (auto & latest_path_info : pool.latest_path_infos)
    {
        paths.push_back(fmt::format("{}/{}", latest_path_info.path, path_prefix));
    }
    return paths;
}

String PSDiskDelegatorMulti::choosePath(const PageFileIdAndLevel & id_lvl)
{
    std::function<String(const StoragePathPool::LatestPathInfos & paths, size_t idx)> path_generator
        = [this](const StoragePathPool::LatestPathInfos & paths, size_t idx) -> String {
        return fmt::format("{}/{}", paths[idx].path, this->path_prefix);
    };

    std::function<String(const StoragePathPool::LatestPathInfo & info)> path_getter
        = [](const StoragePathPool::LatestPathInfo & info) -> String {
        return info.path;
    };

    {
        /// If id exists in page_path_map, just return the same path
        auto index_opt = page_path_map.getIndex(id_lvl);
        if (index_opt.has_value())
            return path_generator(pool.latest_path_infos, *index_opt);
    }

    const String log_msg = fmt::format("[type=ps_multi] [database={}] [table={}]", pool.database, pool.table);
    return genericChoosePath(
        pool.latest_path_infos,
        pool.global_capacity,
        path_generator,
        path_getter,
        pool.log,
        log_msg);
}

size_t PSDiskDelegatorMulti::addPageFileUsedSize(
    const PageFileIdAndLevel & id_lvl,
    size_t size_to_add,
    const String & pf_parent_path,
    bool need_insert_location)
{
    // Get a normalized path without `path_prefix` and trailing '/'
    String upper_path = removeTrailingSlash(Poco::Path(pf_parent_path).parent().toString());
    UInt32 index = UINT32_MAX;
    for (size_t i = 0; i < pool.latest_path_infos.size(); i++)
    {
        if (pool.latest_path_infos[i].path == upper_path)
        {
            index = i;
            break;
        }
    }

    RUNTIME_CHECK_MSG(index != UINT32_MAX, "Unrecognized path {}", upper_path);

    if (need_insert_location)
        page_path_map.setIndex(id_lvl, index);

    // update global used size
    pool.global_capacity->addUsedSize(upper_path, size_to_add);
    return index;
}

void PSDiskDelegatorMulti::freePageFileUsedSize(
    const PageFileIdAndLevel & /*id_lvl*/,
    size_t /*size_to_free*/,
    const String & /*pf_parent_path*/)
{
    throw Exception("Not Implemented", ErrorCodes::NOT_IMPLEMENTED);
}

String PSDiskDelegatorMulti::getPageFilePath(const PageFileIdAndLevel & id_lvl) const
{
    auto index_opt = page_path_map.getIndex(id_lvl);
    RUNTIME_CHECK_MSG(index_opt.has_value(), "Can not find path for PageFile, file_id={}", id_lvl);
    return fmt::format("{}/{}", pool.latest_path_infos[*index_opt].path, path_prefix);
}

void PSDiskDelegatorMulti::removePageFile(
    const PageFileIdAndLevel & id_lvl,
    size_t file_size,
    bool meta_left,
    bool remove_from_default_path)
{
    if (remove_from_default_path)
    {
        pool.global_capacity->freeUsedSize(pool.latest_path_infos[default_path_index].path, file_size);
    }
    else
    {
        auto index_opt = page_path_map.getIndex(id_lvl);
        if (unlikely(!index_opt.has_value()))
            return;
        if (!meta_left)
            page_path_map.eraseIfExist(id_lvl);

        pool.global_capacity->freeUsedSize(pool.latest_path_infos[*index_opt].path, file_size);
    }
}

//==========================================================================================
// Normal data
//==========================================================================================

bool PSDiskDelegatorSingle::fileExist(const PageFileIdAndLevel & /*id_lvl*/) const
{
    throw Exception("Not Implemented", ErrorCodes::NOT_IMPLEMENTED);
}

size_t PSDiskDelegatorSingle::numPaths() const
{
    return 1;
}

String PSDiskDelegatorSingle::defaultPath() const
{
    return fmt::format("{}/{}", pool.latest_path_infos[0].path, path_prefix);
}

Strings PSDiskDelegatorSingle::listPaths() const
{
    // only stored in the first path.
    std::vector<String> paths;
    paths.push_back(fmt::format("{}/{}", pool.latest_path_infos[0].path, path_prefix));
    return paths;
}

String PSDiskDelegatorSingle::choosePath(const PageFileIdAndLevel & /*id_lvl*/)
{
    return fmt::format("{}/{}", pool.latest_path_infos[0].path, path_prefix);
}

size_t PSDiskDelegatorSingle::addPageFileUsedSize(
    const PageFileIdAndLevel & /*id_lvl*/,
    size_t size_to_add,
    const String & pf_parent_path,
    bool /*need_insert_location*/)
{
    // In this case, inserting to page_path_map seems useless.
    // Simply add used size for global capacity is OK.
    pool.global_capacity->addUsedSize(pf_parent_path, size_to_add);
    return 0;
}


void PSDiskDelegatorSingle::freePageFileUsedSize(
    const PageFileIdAndLevel & id_lvl,
    size_t size_to_free,
    const String & /*pf_parent_path*/)
{
    removePageFile(id_lvl, size_to_free, false, false);
}

String PSDiskDelegatorSingle::getPageFilePath(const PageFileIdAndLevel & /*id_lvl*/) const
{
    return fmt::format("{}/{}", pool.latest_path_infos[0].path, path_prefix);
}

void PSDiskDelegatorSingle::removePageFile(
    const PageFileIdAndLevel & /*id_lvl*/,
    size_t file_size,
    bool /*meta_left*/,
    bool /*remove_from_default_path*/)
{
    pool.global_capacity->freeUsedSize(pool.latest_path_infos[0].path, file_size);
}

//==========================================================================================
// Raft Region data
//==========================================================================================
PSDiskDelegatorRaft::PSDiskDelegatorRaft(PathPool & pool_)
    : pool(pool_)
{
    for (const auto & s : pool.kvstore_paths)
    {
        RaftPathInfo info;
        // Get a normalized path without trailing '/'
        info.path = getNormalizedPath(s);
        raft_path_infos.emplace_back(info);
    }
}

bool PSDiskDelegatorRaft::fileExist(const PageFileIdAndLevel & id_lvl) const
{
    return page_path_map.exist(id_lvl);
}

size_t PSDiskDelegatorRaft::numPaths() const
{
    return raft_path_infos.size();
}

String PSDiskDelegatorRaft::defaultPath() const
{
    return raft_path_infos[default_path_index].path;
}

Strings PSDiskDelegatorRaft::listPaths() const
{
    return pool.kvstore_paths;
}

String PSDiskDelegatorRaft::choosePath(const PageFileIdAndLevel & id_lvl)
{
    std::function<String(const RaftPathInfos & paths, size_t idx)> path_generator
        = [](const RaftPathInfos & paths, size_t idx) -> String {
        return paths[idx].path;
    };

    std::function<String(const RaftPathInfo & info)> path_getter = [](const RaftPathInfo & info) -> String {
        return info.path;
    };

    {
        /// If id exists in page_path_map, just return the same path
        auto index_opt = page_path_map.getIndex(id_lvl);
        if (index_opt.has_value())
            return path_generator(raft_path_infos, *index_opt);
    }

    // Else choose path randomly
    const String log_msg = "[type=ps_raft]";
    return genericChoosePath(raft_path_infos, pool.global_capacity, path_generator, path_getter, pool.log, log_msg);
}

size_t PSDiskDelegatorRaft::addPageFileUsedSize(
    const PageFileIdAndLevel & id_lvl,
    size_t size_to_add,
    const String & pf_parent_path,
    bool need_insert_location)
{
    // Get a normalized path without trailing '/'
    String upper_path = getNormalizedPath(pf_parent_path);
    UInt32 index = UINT32_MAX;
    for (size_t i = 0; i < raft_path_infos.size(); i++)
    {
        if (raft_path_infos[i].path == upper_path)
        {
            index = i;
            break;
        }
    }
    RUNTIME_CHECK_MSG(index != UINT32_MAX, "Unrecognized path {}", upper_path);

    if (need_insert_location)
        page_path_map.setIndex(id_lvl, index);

    // update global used size
    pool.global_capacity->addUsedSize(upper_path, size_to_add);
    return index;
}

void PSDiskDelegatorRaft::freePageFileUsedSize(
    const PageFileIdAndLevel & id_lvl,
    size_t size_to_free,
    const String & pf_parent_path)
{
    String upper_path = getNormalizedPath(pf_parent_path);
    UInt32 index = UINT32_MAX;
    for (size_t i = 0; i < raft_path_infos.size(); i++)
    {
        if (raft_path_infos[i].path == upper_path)
        {
            index = i;
            break;
        }
    }

    RUNTIME_CHECK_MSG(index != UINT32_MAX, "Unrecognized path {}", upper_path);
    RUNTIME_CHECK_MSG(
        page_path_map.exist(id_lvl),
        "Can not find path for PageFile, file_id={} path={}",
        id_lvl,
        pf_parent_path);

    // update global used size
    pool.global_capacity->freeUsedSize(upper_path, size_to_free);
}

String PSDiskDelegatorRaft::getPageFilePath(const PageFileIdAndLevel & id_lvl) const
{
    auto index_opt = page_path_map.getIndex(id_lvl);
    RUNTIME_CHECK_MSG(index_opt.has_value(), "Can not find path for PageFile, file_id={}", id_lvl);
    return raft_path_infos[*index_opt].path;
}

void PSDiskDelegatorRaft::removePageFile(
    const PageFileIdAndLevel & id_lvl,
    size_t file_size,
    bool meta_left,
    bool remove_from_default_path)
{
    if (remove_from_default_path)
    {
        pool.global_capacity->freeUsedSize(raft_path_infos[default_path_index].path, file_size);
    }
    else
    {
        auto index_opt = page_path_map.getIndex(id_lvl);
        if (unlikely(!index_opt.has_value()))
            return;
        if (!meta_left)
            page_path_map.eraseIfExist(id_lvl);

        pool.global_capacity->freeUsedSize(raft_path_infos[*index_opt].path, file_size);
    }
}

//==========================================================================================
// Global page data
//==========================================================================================

bool PSDiskDelegatorGlobalMulti::fileExist(const PageFileIdAndLevel & id_lvl) const
{
    return page_path_map.exist(id_lvl);
}

size_t PSDiskDelegatorGlobalMulti::numPaths() const
{
    return pool.listGlobalPagePaths().size();
}

String PSDiskDelegatorGlobalMulti::defaultPath() const
{
    return fmt::format("{}/{}", pool.listGlobalPagePaths()[default_path_index], path_prefix);
}

Strings PSDiskDelegatorGlobalMulti::listPaths() const
{
    // The delta data could be stored in all direcotries.
    std::vector<String> paths;
    for (const auto & path : pool.listGlobalPagePaths())
    {
        paths.push_back(fmt::format("{}/{}", path, path_prefix));
    }
    return paths;
}

String PSDiskDelegatorGlobalMulti::choosePath(const PageFileIdAndLevel & id_lvl)
{
    std::function<String(const Strings & paths, size_t idx)> path_generator
        = [this](const Strings & paths, size_t idx) -> String {
        return fmt::format("{}/{}", paths[idx], this->path_prefix);
    };

    std::function<String(const String & info)> path_getter = [](const String & path_) -> String {
        return path_;
    };

    {
        /// If id exists in page_path_map, just return the same path
        auto index_opt = page_path_map.getIndex(id_lvl);
        if (index_opt.has_value())
            return path_generator(pool.listGlobalPagePaths(), *index_opt);
    }

    const String log_msg = "[type=global_ps_multi]";
    return genericChoosePath(
        pool.listGlobalPagePaths(),
        pool.global_capacity,
        path_generator,
        path_getter,
        pool.log,
        log_msg);
}

size_t PSDiskDelegatorGlobalMulti::addPageFileUsedSize(
    const PageFileIdAndLevel & id_lvl,
    size_t size_to_add,
    const String & pf_parent_path,
    bool need_insert_location)
{
    // Get a normalized path without `path_prefix` and trailing '/'
    String upper_path = removeTrailingSlash(Poco::Path(pf_parent_path).parent().toString());
    UInt32 index = UINT32_MAX;

    const auto & global_paths = pool.listGlobalPagePaths();
    for (size_t i = 0; i < global_paths.size(); i++)
    {
        if (global_paths[i] == upper_path)
        {
            index = i;
            break;
        }
    }

    RUNTIME_CHECK_MSG(index != UINT32_MAX, "Unrecognized path {}", upper_path);

    if (need_insert_location)
    {
        page_path_map.setIndex(id_lvl, index);
    }

    // update global used size
    pool.global_capacity->addUsedSize(upper_path, size_to_add);
    return index;
}

void PSDiskDelegatorGlobalMulti::freePageFileUsedSize(
    const PageFileIdAndLevel & id_lvl,
    size_t size_to_free,
    const String & pf_parent_path)
{
    // Get a normalized path without `path_prefix` and trailing '/'
    String upper_path = removeTrailingSlash(Poco::Path(pf_parent_path).parent().toString());
    UInt32 index = UINT32_MAX;

    const auto & global_paths = pool.listGlobalPagePaths();
    for (size_t i = 0; i < global_paths.size(); i++)
    {
        if (global_paths[i] == upper_path)
        {
            index = i;
            break;
        }
    }

    RUNTIME_CHECK_MSG(index != UINT32_MAX, "Unrecognized path {}", upper_path);
    RUNTIME_CHECK_MSG(
        page_path_map.exist(id_lvl),
        "Can not find path for PageFile, file_id={} path={}",
        id_lvl,
        pf_parent_path);

    // update global used size
    pool.global_capacity->freeUsedSize(upper_path, size_to_free);
}

String PSDiskDelegatorGlobalMulti::getPageFilePath(const PageFileIdAndLevel & id_lvl) const
{
    auto index = page_path_map.getIndex(id_lvl);
    RUNTIME_CHECK_MSG(index.has_value(), "Can not find path for PageFile file_id={}", id_lvl);
    return fmt::format("{}/{}", pool.listGlobalPagePaths()[*index], path_prefix);
}

void PSDiskDelegatorGlobalMulti::removePageFile(
    const PageFileIdAndLevel & id_lvl,
    size_t file_size,
    bool meta_left,
    bool remove_from_default_path)
{
    if (remove_from_default_path)
    {
        pool.global_capacity->freeUsedSize(pool.listGlobalPagePaths()[default_path_index], file_size);
    }
    else
    {
        auto index_opt = page_path_map.getIndex(id_lvl);
        if (unlikely(!index_opt.has_value()))
            return;
        if (!meta_left)
            page_path_map.eraseIfExist(id_lvl);

        pool.global_capacity->freeUsedSize(pool.listGlobalPagePaths()[*index_opt], file_size);
    }
}

bool PSDiskDelegatorGlobalSingle::fileExist(const PageFileIdAndLevel & id_lvl) const
{
    return page_path_map.exist(id_lvl);
}

size_t PSDiskDelegatorGlobalSingle::numPaths() const
{
    return 1;
}

String PSDiskDelegatorGlobalSingle::defaultPath() const
{
    return fmt::format("{}/{}", pool.listGlobalPagePaths()[0], path_prefix);
}

Strings PSDiskDelegatorGlobalSingle::listPaths() const
{
    // only stored in the first path.
    std::vector<String> paths;
    paths.push_back(fmt::format("{}/{}", pool.listGlobalPagePaths()[0], path_prefix));
    return paths;
}

String PSDiskDelegatorGlobalSingle::choosePath(const PageFileIdAndLevel & /*id_lvl*/)
{
    return fmt::format("{}/{}", pool.listGlobalPagePaths()[0], path_prefix);
}

size_t PSDiskDelegatorGlobalSingle::addPageFileUsedSize(
    const PageFileIdAndLevel & id_lvl,
    size_t size_to_add,
    const String & pf_parent_path,
    bool need_insert_location)
{
    // We need a map for id_lvl -> path_index for function `fileExist`
    if (need_insert_location)
    {
        page_path_map.setIndex(id_lvl, 0);
    }
    pool.global_capacity->addUsedSize(pf_parent_path, size_to_add);
    return 0;
}

void PSDiskDelegatorGlobalSingle::freePageFileUsedSize(
    const PageFileIdAndLevel & /*id_lvl*/,
    size_t size_to_free,
    const String & pf_parent_path)
{
    pool.global_capacity->freeUsedSize(pf_parent_path, size_to_free);
}

String PSDiskDelegatorGlobalSingle::getPageFilePath(const PageFileIdAndLevel & /*id_lvl*/) const
{
    return fmt::format("{}/{}", pool.listGlobalPagePaths()[0], path_prefix);
}

void PSDiskDelegatorGlobalSingle::removePageFile(
    const PageFileIdAndLevel & id_lvl,
    size_t file_size,
    bool /*meta_left*/,
    bool /*remove_from_default_path*/)
{
    pool.global_capacity->freeUsedSize(pool.listGlobalPagePaths()[0], file_size);

    page_path_map.eraseIfExist(id_lvl);
}

//==========================================================================================
// Choose PS file path in a fixed directory.
//==========================================================================================
PSDiskDelegatorFixedDirectory::PSDiskDelegatorFixedDirectory(const PathPool & pool_, const String & path_)
    : path(path_)
    , pool(pool_)
{}

bool PSDiskDelegatorFixedDirectory::fileExist(const PageFileIdAndLevel & id_lvl) const
{
    return page_path_map.exist(id_lvl);
}

size_t PSDiskDelegatorFixedDirectory::numPaths() const
{
    return 1;
}

String PSDiskDelegatorFixedDirectory::defaultPath() const
{
    return path;
}

Strings PSDiskDelegatorFixedDirectory::listPaths() const
{
    return {path};
}

String PSDiskDelegatorFixedDirectory::choosePath(const PageFileIdAndLevel &)
{
    return path;
}

size_t PSDiskDelegatorFixedDirectory::addPageFileUsedSize(
    const PageFileIdAndLevel & id_lvl,
    size_t size_to_add,
    const String & pf_parent_path,
    bool need_insert_location)
{
    // We need a map for id_lvl -> path_index for function `fileExist`
    if (need_insert_location)
        page_path_map.setIndex(id_lvl, 0);

    pool.global_capacity->addUsedSize(pf_parent_path, size_to_add);
    return 0;
}

void PSDiskDelegatorFixedDirectory::freePageFileUsedSize(
    const PageFileIdAndLevel &,
    size_t size_to_free,
    const String & pf_parent_path)
{
    pool.global_capacity->freeUsedSize(pf_parent_path, size_to_free);
}

String PSDiskDelegatorFixedDirectory::getPageFilePath(const PageFileIdAndLevel &) const
{
    return path;
}

void PSDiskDelegatorFixedDirectory::removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size, bool, bool)
{
    pool.global_capacity->freeUsedSize(path, file_size);
    page_path_map.eraseIfExist(id_lvl);
}

} // namespace DB
