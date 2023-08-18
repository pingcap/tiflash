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
#include <Encryption/FileProvider.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/PathPool.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <common/likely.h>
#include <fmt/core.h>

#include <random>
#include <set>
#include <unordered_map>

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

// Constructor to be used during initialization
PathPool::PathPool(
    const Strings & main_data_paths_,
    const Strings & latest_data_paths_,
    const Strings & kvstore_paths_, //
    PathCapacityMetricsPtr global_capacity_,
    FileProviderPtr file_provider_,
    bool enable_raft_compatible_mode_)
    : main_data_paths(main_data_paths_)
    , latest_data_paths(latest_data_paths_)
    , kvstore_paths(kvstore_paths_)
    , enable_raft_compatible_mode(enable_raft_compatible_mode_)
    , global_capacity(global_capacity_)
    , file_provider(file_provider_)
    , log(Logger::get("PathPool"))
{
    if (kvstore_paths.empty())
    {
        // Set default path generated from latest_data_paths
        for (const auto & s : latest_data_paths)
        {
            // Get a normalized path without trailing '/'
            auto p = getNormalizedPath(s + "/kvstore");
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

StoragePathPool PathPool::withTable(const String & database_, const String & table_, bool path_need_database_name_) const
{
    return StoragePathPool(main_data_paths, latest_data_paths, database_, table_, path_need_database_name_, global_capacity, file_provider);
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
    , path_need_database_name(path_need_database_name_)
    , global_capacity(std::move(global_capacity_))
    , file_provider(std::move(file_provider_))
    , log(Logger::get("StoragePathPool"))
{
    if (unlikely(database.empty() || table.empty()))
        throw Exception(fmt::format("Can NOT create StoragePathPool [database={}] [table={}]", database, table), ErrorCodes::LOGICAL_ERROR);

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

StoragePathPool::StoragePathPool(const StoragePathPool & rhs)
    : main_path_infos(rhs.main_path_infos)
    , latest_path_infos(rhs.latest_path_infos)
    , dt_file_path_map(rhs.dt_file_path_map)
    , database(rhs.database)
    , table(rhs.table)
    , path_need_database_name(rhs.path_need_database_name)
    , global_capacity(rhs.global_capacity)
    , file_provider(rhs.file_provider)
    , log(rhs.log)
{}

StoragePathPool & StoragePathPool::operator=(const StoragePathPool & rhs)
{
    if (this != &rhs)
    {
        main_path_infos = rhs.main_path_infos;
        latest_path_infos = rhs.latest_path_infos;
        dt_file_path_map = rhs.dt_file_path_map;
        database = rhs.database;
        table = rhs.table;
        path_need_database_name = rhs.path_need_database_name;
        global_capacity = rhs.global_capacity;
        file_provider = rhs.file_provider;
        log = rhs.log;
    }
    return *this;
}

void StoragePathPool::rename(const String & new_database, const String & new_table, bool clean_rename)
{
    if (unlikely(new_database.empty() || new_table.empty()))
        throw Exception(fmt::format("Can not rename for PathPool to {}.{}", new_database, new_table));

    if (likely(clean_rename))
    {
        // caller ensure that no path need to be renamed.
        if (unlikely(path_need_database_name))
            throw Exception("Can not do clean rename with path_need_database_name is true!");

        std::lock_guard lock{mutex};
        database = new_database;
        table = new_table;
    }
    else
    {
        if (unlikely(file_provider->isEncryptionEnabled()))
            throw Exception("Encryption is only supported when using clean_rename");

        // Note: changing these path is not atomic, we may lost data if process is crash here.
        std::lock_guard lock{mutex};
        // Get root path without database and table
        for (auto & info : main_path_infos)
        {
            Poco::Path p(info.path);
            p = p.parent().parent();
            if (path_need_database_name)
                p = p.parent();
            auto new_path = getStorePath(p.toString() + "/data", new_database, new_table);
            renamePath(info.path, new_path);
            info.path = new_path;
        }
        for (auto & info : latest_path_infos)
        {
            Poco::Path p(info.path);
            p = p.parent().parent();
            if (path_need_database_name)
                p = p.parent();
            auto new_path = getStorePath(p.toString() + "/data", new_database, new_table);
            renamePath(info.path, new_path);
            info.path = new_path;
        }

        database = new_database;
        table = new_table;
    }
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
                LOG_FMT_INFO(log, "Begin to drop [dir={}] from main_path_infos", path_info.path);
                file_provider->deleteDirectory(dir.path(), false, recursive);

                // update global used size
                size_t total_bytes = 0;
                for (const auto & [file_id, file_size] : path_info.file_size_map)
                {
                    (void)file_id;
                    total_bytes += file_size;
                }
                global_capacity->freeUsedSize(path_info.path, total_bytes);
            }
        }
        catch (Poco::DirectoryNotEmptyException & e)
        {
            if (must_success)
                throw;
            else
            {
                // just ignore and keep that directory if it is not empty
                LOG_FMT_WARNING(log, "Can not remove directory: {}, it is not empty", path_info.path);
            }
        }
    }
    for (auto & path_info : latest_path_infos)
    {
        try
        {
            if (Poco::File dir(path_info.path); dir.exists())
            {
                LOG_FMT_INFO(log, "Begin to drop [dir={}] from latest_path_infos", path_info.path);
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
                LOG_FMT_WARNING(log, "Can not remove directory: {}, it is not empty", path_info.path);
            }
        }
    }
}

//==========================================================================================
// private methods
//==========================================================================================

String StoragePathPool::getStorePath(const String & extra_path_root, const String & database_name, const String & table_name) const
{
    if (likely(!path_need_database_name))
        return getNormalizedPath(fmt::format("{}/{}", extra_path_root, escapeForFileName(table_name)));
    else
        return getNormalizedPath(fmt::format("{}/{}/{}", extra_path_root, escapeForFileName(database_name), escapeForFileName(table_name)));
}

void StoragePathPool::renamePath(const String & old_path, const String & new_path)
{
    LOG_FMT_INFO(log, "Renaming {} to {}", old_path, new_path);
    if (auto file = Poco::File{old_path}; file.exists())
        file.renameTo(new_path);
    else
        LOG_FMT_WARNING(log, "Path \"{}\" is missed.", old_path);
}

//==========================================================================================
// Generic functions
//==========================================================================================

template <typename T>
String genericChoosePath(const std::vector<T> & paths, //
                         const PathCapacityMetricsPtr & global_capacity, //
                         std::function<String(const std::vector<T> & paths, size_t idx)> path_generator, //
                         std::function<String(const T & path_info)> path_getter, //
                         LoggerPtr log, //
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
        LOG_FMT_WARNING(log, "No available space for all disks, choose randomly.");
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
            LOG_FMT_INFO(log, "Choose path [index={}] {}", i, log_msg);
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

    std::function<String(const StoragePathPool::MainPathInfo & info)> path_getter = [](const StoragePathPool::MainPathInfo & info) -> String {
        return info.path;
    };

    const String log_msg = fmt::format("[type=stable] [database={}] [table={}]", pool.database, pool.table);
    return genericChoosePath(pool.main_path_infos, pool.global_capacity, path_generator, path_getter, pool.log, log_msg);
}

String StableDiskDelegator::getDTFilePath(UInt64 file_id, bool throw_on_not_exist) const
{
    std::lock_guard lock{pool.mutex};
    auto iter = pool.dt_file_path_map.find(file_id);
    if (likely(iter != pool.dt_file_path_map.end()))
        return fmt::format("{}/{}", pool.main_path_infos[iter->second].path, StoragePathPool::STABLE_FOLDER_NAME);
    if (likely(throw_on_not_exist))
        throw Exception(fmt::format("Can not find path for DMFile [id={}]", file_id));
    return "";
}

void StableDiskDelegator::addDTFile(UInt64 file_id, size_t file_size, std::string_view path)
{
    path.remove_suffix(1 + strlen(StoragePathPool::STABLE_FOLDER_NAME)); // remove '/stable' added in listPathsForStable/getDTFilePath
    std::lock_guard lock{pool.mutex};
    if (auto iter = pool.dt_file_path_map.find(file_id); unlikely(iter != pool.dt_file_path_map.end()))
    {
        const auto & path_info = pool.main_path_infos[iter->second];
        throw DB::TiFlashException(
            fmt::format("Try to add a DTFile with duplicated id. [id={}] [path={}] [existed_path={}]", file_id, path, path_info.path),
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
    if (unlikely(index == UINT32_MAX))
        throw DB::TiFlashException(
            fmt::format("Try to add a DTFile to an unrecognized path. [id={}] [path={}]", file_id, path),
            Errors::DeltaTree::Internal);
    pool.dt_file_path_map.emplace(file_id, index);
    pool.main_path_infos[index].file_size_map.emplace(file_id, file_size);

#ifndef NDEBUG
    try
    {
        auto dmf_path = fmt::format("{}/stable/dmf_{}", path, file_id);
        Poco::File dmf_file = {dmf_path};
        if (dmf_file.isFile())
        {
            LOG_FMT_DEBUG(
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
                auto get_folder_size_impl = [](const Poco::File & inner_target, size_t & inner_counter, auto & self) -> void {
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
            LOG_FMT_DEBUG(
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
        LOG_FMT_WARNING(pool.log, "failed to get real size info for dtfile. [id={}] [path={}] [err={}]", file_id, path, exp.displayText());
    }
#endif

    // update global used size
    pool.global_capacity->addUsedSize(path, file_size);
}

void StableDiskDelegator::removeDTFile(UInt64 file_id)
{
    std::lock_guard lock{pool.mutex};
    auto iter = pool.dt_file_path_map.find(file_id);
    if (unlikely(iter == pool.dt_file_path_map.end()))
        throw Exception(fmt::format("Cannot find DMFile for id {}", file_id));
    UInt32 index = iter->second;
    const auto file_size = pool.main_path_infos[index].file_size_map.at(file_id);
    pool.dt_file_path_map.erase(file_id);
    pool.main_path_infos[index].file_size_map.erase(file_id);
    // update global used size
    pool.global_capacity->freeUsedSize(pool.main_path_infos[index].path, file_size);
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
    std::function<String(const StoragePathPool::LatestPathInfos & paths, size_t idx)> path_generator =
        [this](const StoragePathPool::LatestPathInfos & paths, size_t idx) -> String {
        return fmt::format("{}/{}", paths[idx].path, this->path_prefix);
    };

    std::function<String(const StoragePathPool::LatestPathInfo & info)> path_getter = [](const StoragePathPool::LatestPathInfo & info) -> String {
        return info.path;
    };

    {
        std::lock_guard lock{pool.mutex};
        /// If id exists in page_path_map, just return the same path
        if (auto iter = page_path_map.find(id_lvl); iter != page_path_map.end())
            return path_generator(pool.latest_path_infos, iter->second);
    }

    const String log_msg = fmt::format("[type=ps_multi] [database={}] [table={}]", pool.database, pool.table);
    return genericChoosePath(pool.latest_path_infos, pool.global_capacity, path_generator, path_getter, pool.log, log_msg);
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
    if (unlikely(index == UINT32_MAX))
        throw Exception(fmt::format("Unrecognized path {}", upper_path));

    {
        std::lock_guard lock{pool.mutex};
        if (need_insert_location)
            page_path_map[id_lvl] = index;
    }

    // update global used size
    pool.global_capacity->addUsedSize(upper_path, size_to_add);
    return index;
}

size_t PSDiskDelegatorMulti::freePageFileUsedSize(
    const PageFileIdAndLevel & /*id_lvl*/,
    size_t /*size_to_free*/,
    const String & /*pf_parent_path*/)
{
    throw Exception("Not Implemented", ErrorCodes::NOT_IMPLEMENTED);
}

String PSDiskDelegatorMulti::getPageFilePath(const PageFileIdAndLevel & id_lvl) const
{
    std::lock_guard lock{pool.mutex};
    auto iter = page_path_map.find(id_lvl);
    if (likely(iter != page_path_map.end()))
        return fmt::format("{}/{}", pool.latest_path_infos[iter->second].path, path_prefix);
    throw Exception(fmt::format("Can not find path for PageFile [id={}_{}]", id_lvl.first, id_lvl.second));
}

void PSDiskDelegatorMulti::removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size, bool meta_left, bool remove_from_default_path)
{
    std::lock_guard lock{pool.mutex};
    if (remove_from_default_path)
    {
        pool.global_capacity->freeUsedSize(pool.latest_path_infos[default_path_index].path, file_size);
    }
    else
    {
        auto iter = page_path_map.find(id_lvl);
        if (unlikely(iter == page_path_map.end()))
            return;
        auto index = iter->second;
        if (!meta_left)
            page_path_map.erase(iter);

        pool.global_capacity->freeUsedSize(pool.latest_path_infos[index].path, file_size);
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


size_t PSDiskDelegatorSingle::freePageFileUsedSize(
    const PageFileIdAndLevel & /*id_lvl*/,
    size_t /*size_to_free*/,
    const String & /*pf_parent_path*/)
{
    throw Exception("Not Implemented", ErrorCodes::NOT_IMPLEMENTED);
}

String PSDiskDelegatorSingle::getPageFilePath(const PageFileIdAndLevel & /*id_lvl*/) const
{
    return fmt::format("{}/{}", pool.latest_path_infos[0].path, path_prefix);
}

void PSDiskDelegatorSingle::removePageFile(const PageFileIdAndLevel & /*id_lvl*/, size_t file_size, bool /*meta_left*/, bool /*remove_from_default_path*/)
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
    return page_path_map.find(id_lvl) != page_path_map.end();
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
        std::lock_guard lock{mutex};
        /// If id exists in page_path_map, just return the same path
        if (auto iter = page_path_map.find(id_lvl); iter != page_path_map.end())
            return path_generator(raft_path_infos, iter->second);
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
    if (unlikely(index == UINT32_MAX))
        throw Exception(fmt::format("Unrecognized path {}", upper_path));

    {
        std::lock_guard lock{mutex};
        if (need_insert_location)
            page_path_map[id_lvl] = index;
    }

    // update global used size
    pool.global_capacity->addUsedSize(upper_path, size_to_add);
    return index;
}

size_t PSDiskDelegatorRaft::freePageFileUsedSize(
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

    if (unlikely(index == UINT32_MAX))
    {
        throw Exception(fmt::format("Unrecognized path {}", upper_path));
    }

    if (page_path_map.find(id_lvl) == page_path_map.end())
    {
        throw Exception(fmt::format("Can not find path for PageFile [id={}_{}, path={}]", id_lvl.first, id_lvl.second, pf_parent_path));
    }

    // update global used size
    pool.global_capacity->freeUsedSize(upper_path, size_to_free);
    return index;
}

String PSDiskDelegatorRaft::getPageFilePath(const PageFileIdAndLevel & id_lvl) const
{
    std::lock_guard lock{mutex};
    auto iter = page_path_map.find(id_lvl);
    if (likely(iter != page_path_map.end()))
        return raft_path_infos[iter->second].path;
    throw Exception(fmt::format("Can not find path for PageFile [id={}_{}]", id_lvl.first, id_lvl.second));
}

void PSDiskDelegatorRaft::removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size, bool meta_left, bool remove_from_default_path)
{
    std::lock_guard lock{mutex};
    if (remove_from_default_path)
    {
        pool.global_capacity->freeUsedSize(raft_path_infos[default_path_index].path, file_size);
    }
    else
    {
        auto iter = page_path_map.find(id_lvl);
        if (unlikely(iter == page_path_map.end()))
            return;
        auto index = iter->second;
        if (!meta_left)
            page_path_map.erase(iter);
        pool.global_capacity->freeUsedSize(raft_path_infos[index].path, file_size);
    }
}

//==========================================================================================
// Global page data
//==========================================================================================

bool PSDiskDelegatorGlobalMulti::fileExist(const PageFileIdAndLevel & id_lvl) const
{
    return page_path_map.find(id_lvl) != page_path_map.end();
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
    std::function<String(const Strings & paths, size_t idx)> path_generator =
        [this](const Strings & paths, size_t idx) -> String {
        return fmt::format("{}/{}", paths[idx], this->path_prefix);
    };

    std::function<String(const String & info)> path_getter = [](const String & path_) -> String {
        return path_;
    };

    {
        std::lock_guard lock{mutex};
        /// If id exists in page_path_map, just return the same path
        if (auto iter = page_path_map.find(id_lvl); iter != page_path_map.end())
            return path_generator(pool.listGlobalPagePaths(), iter->second);
    }

    const String log_msg = "[type=global_ps_multi]";
    return genericChoosePath(pool.listGlobalPagePaths(), pool.global_capacity, path_generator, path_getter, pool.log, log_msg);
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

    if (unlikely(index == UINT32_MAX))
        throw Exception(fmt::format("Unrecognized path {}", upper_path));

    {
        std::lock_guard lock{mutex};
        if (need_insert_location)
            page_path_map[id_lvl] = index;
    }

    // update global used size
    pool.global_capacity->addUsedSize(upper_path, size_to_add);
    return index;
}

size_t PSDiskDelegatorGlobalMulti::freePageFileUsedSize(
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

    if (unlikely(index == UINT32_MAX))
    {
        throw Exception(fmt::format("Unrecognized path {}", upper_path));
    }

    if (page_path_map.find(id_lvl) == page_path_map.end())
    {
        throw Exception(fmt::format("Can not find path for PageFile [id={}_{}, path={}]", id_lvl.first, id_lvl.second, pf_parent_path));
    }

    // update global used size
    pool.global_capacity->freeUsedSize(upper_path, size_to_free);
    return index;
}

String PSDiskDelegatorGlobalMulti::getPageFilePath(const PageFileIdAndLevel & id_lvl) const
{
    std::lock_guard lock{mutex};
    auto iter = page_path_map.find(id_lvl);
    if (likely(iter != page_path_map.end()))
        return fmt::format("{}/{}", pool.listGlobalPagePaths()[iter->second], path_prefix);
    throw Exception(fmt::format("Can not find path for PageFile [id={}_{}]", id_lvl.first, id_lvl.second));
}

void PSDiskDelegatorGlobalMulti::removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size, bool meta_left, bool remove_from_default_path)
{
    std::lock_guard lock{mutex};
    if (remove_from_default_path)
    {
        pool.global_capacity->freeUsedSize(pool.listGlobalPagePaths()[default_path_index], file_size);
    }
    else
    {
        auto iter = page_path_map.find(id_lvl);
        if (unlikely(iter == page_path_map.end()))
            return;
        auto index = iter->second;
        if (!meta_left)
            page_path_map.erase(iter);

        pool.global_capacity->freeUsedSize(pool.listGlobalPagePaths()[index], file_size);
    }
}

bool PSDiskDelegatorGlobalSingle::fileExist(const PageFileIdAndLevel & id_lvl) const
{
    return page_path_map.find(id_lvl) != page_path_map.end();
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
        std::lock_guard lock{mutex};
        page_path_map[id_lvl] = 0;
    }
    pool.global_capacity->addUsedSize(pf_parent_path, size_to_add);
    return 0;
}

size_t PSDiskDelegatorGlobalSingle::freePageFileUsedSize(
    const PageFileIdAndLevel & /*id_lvl*/,
    size_t size_to_free,
    const String & pf_parent_path)
{
    pool.global_capacity->freeUsedSize(pf_parent_path, size_to_free);
    return 0;
}

String PSDiskDelegatorGlobalSingle::getPageFilePath(const PageFileIdAndLevel & /*id_lvl*/) const
{
    return fmt::format("{}/{}", pool.listGlobalPagePaths()[0], path_prefix);
}

void PSDiskDelegatorGlobalSingle::removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size, bool /*meta_left*/, bool /*remove_from_default_path*/)
{
    pool.global_capacity->freeUsedSize(pool.listGlobalPagePaths()[0], file_size);

    std::lock_guard lock{mutex};
    auto iter = page_path_map.find(id_lvl);
    if (unlikely(iter != page_path_map.end()))
        page_path_map.erase(iter);
}

} // namespace DB
