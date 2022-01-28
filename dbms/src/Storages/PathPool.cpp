#include <Common/Exception.h>
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
#include <common/logger_useful.h>

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
    , log(&Poco::Logger::get("PathPool"))
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
    , log(&Poco::Logger::get("StoragePathPool"))
{
    if (unlikely(database.empty() || table.empty()))
        throw Exception("Can NOT create StoragePathPool [database=" + database + "] [table=" + table + "]", ErrorCodes::LOGICAL_ERROR);

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
        throw Exception("Can not rename for PathPool to " + new_database + "." + new_table);

    if (likely(clean_rename))
    {
        // caller ensure that no path need to be renamed.
        if (unlikely(path_need_database_name))
            throw Exception("Can not do clean rename with path_need_database_name is true!");

        std::lock_guard<std::mutex> lock{mutex};
        database = new_database;
        table = new_table;
    }
    else
    {
        if (unlikely(file_provider->isEncryptionEnabled()))
            throw Exception("Encryption is only supported when using clean_rename");

        // Note: changing these path is not atomic, we may lost data if process is crash here.
        std::lock_guard<std::mutex> lock{mutex};
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
    std::lock_guard<std::mutex> lock{mutex};
    for (auto & path_info : main_path_infos)
    {
        try
        {
            if (Poco::File dir(path_info.path); dir.exists())
            {
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
                LOG_WARNING(log, "Can not remove directory: " << path_info.path << ", it is not empty");
            }
        }
    }
    for (auto & path_info : latest_path_infos)
    {
        try
        {
            if (Poco::File dir(path_info.path); dir.exists())
            {
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
                LOG_WARNING(log, "Can not remove directory: " << path_info.path << ", it is not empty");
            }
        }
    }
}

//==========================================================================================
// private methods
//==========================================================================================

String StoragePathPool::getStorePath(const String & extra_path_root, const String & database_name, const String & table_name)
{
    if (likely(!path_need_database_name))
        return getNormalizedPath(extra_path_root + "/" + escapeForFileName(table_name));
    else
        return getNormalizedPath(extra_path_root + "/" + escapeForFileName(database_name) + "/" + escapeForFileName(table_name));
}

void StoragePathPool::renamePath(const String & old_path, const String & new_path)
{
    LOG_INFO(log, "Renaming " << old_path << " to " << new_path);
    if (auto file = Poco::File{old_path}; file.exists())
        file.renameTo(new_path);
    else
        LOG_WARNING(log, "Path \"" << old_path << "\" is missed.");
}

//==========================================================================================
// Generic functions
//==========================================================================================

template <typename T>
String genericChoosePath(const std::vector<T> & paths, const PathCapacityMetricsPtr & global_capacity, std::function<String(const std::vector<T> & paths, size_t idx)> path_generator, Poco::Logger * log, const String & log_msg)
{
    if (paths.size() == 1)
        return path_generator(paths, 0);

    UInt64 total_available_size = 0;
    std::vector<FsStats> stats;
    for (size_t i = 0; i < paths.size(); ++i)
    {
        stats.emplace_back(global_capacity->getFsStatsOfPath(paths[i].path));
        total_available_size += stats.back().avail_size;
    }

    // We should choose path even if there is no available space.
    // If the actual disk space is running out, let the later `write` to throw exception.
    // If available space is limited by the quota, then write down a GC-ed file can make
    // some files be deleted later.
    if (total_available_size == 0)
        LOG_WARNING(log, "No available space for all disks, choose randomly.");
    std::vector<double> ratio;
    for (size_t i = 0; i < stats.size(); ++i)
    {
        if (likely(total_available_size != 0))
            ratio.push_back(1.0 * stats[i].avail_size / total_available_size);
        else
        {
            // No available space for all disks, choose randomly
            ratio.push_back(1.0 / paths.size());
        }
    }

    double rand_number = (double)rand() / RAND_MAX;
    double ratio_sum = 0.0;
    for (size_t i = 0; i < ratio.size(); i++)
    {
        ratio_sum += ratio[i];
        if ((rand_number < ratio_sum) || (i == ratio.size() - 1))
        {
            LOG_INFO(log, "Choose path [index=" << i << "] " << log_msg);
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
    for (size_t i = 0; i < pool.main_path_infos.size(); ++i)
    {
        paths.push_back(pool.main_path_infos[i].path + "/" + StoragePathPool::STABLE_FOLDER_NAME);
    }
    return paths;
}

String StableDiskDelegator::choosePath() const
{
    std::function<String(const StoragePathPool::MainPathInfos & paths, size_t idx)> path_generator
        = [](const StoragePathPool::MainPathInfos & paths, size_t idx) -> String {
        return paths[idx].path + "/" + StoragePathPool::STABLE_FOLDER_NAME;
    };
    const String log_msg = "[type=stable] [database=" + pool.database + "] [table=" + pool.table + "]";
    return genericChoosePath(pool.main_path_infos, pool.global_capacity, path_generator, pool.log, log_msg);
}

String StableDiskDelegator::getDTFilePath(UInt64 file_id, bool throw_on_not_exist) const
{
    std::lock_guard<std::mutex> lock{pool.mutex};
    auto iter = pool.dt_file_path_map.find(file_id);
    if (likely(iter != pool.dt_file_path_map.end()))
        return pool.main_path_infos[iter->second].path + "/" + StoragePathPool::STABLE_FOLDER_NAME;
    if (likely(throw_on_not_exist))
        throw Exception("Can not find path for DMFile [id=" + toString(file_id) + "]");
    return "";
}

void StableDiskDelegator::addDTFile(UInt64 file_id, size_t file_size, std::string_view path)
{
    path.remove_suffix(1 + strlen(StoragePathPool::STABLE_FOLDER_NAME)); // remove '/stable' added in listPathsForStable/getDTFilePath
    std::lock_guard<std::mutex> lock{pool.mutex};
    if (auto iter = pool.dt_file_path_map.find(file_id); unlikely(iter != pool.dt_file_path_map.end()))
    {
        const auto & path_info = pool.main_path_infos[iter->second];
        throw DB::TiFlashException("Try to add a DTFile with duplicated id. [id=" + DB::toString(file_id) + "] [path=" + String(path)
                + "] [existed_path=" + path_info.path + "]",
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
            "Try to add a DTFile to an unrecognized path. [id=" + DB::toString(file_id) + "] [path=" + String(path) + "]",
            Errors::DeltaTree::Internal);
    pool.dt_file_path_map.emplace(file_id, index);
    pool.main_path_infos[index].file_size_map.emplace(file_id, file_size);
    // update global used size
    pool.global_capacity->addUsedSize(path, file_size);
}

void StableDiskDelegator::removeDTFile(UInt64 file_id)
{
    std::lock_guard<std::mutex> lock{pool.mutex};
    auto iter = pool.dt_file_path_map.find(file_id);
    if (unlikely(iter == pool.dt_file_path_map.end()))
        throw Exception("Cannot find DMFile for id " + toString(file_id));
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

size_t PSDiskDelegatorMulti::numPaths() const
{
    return pool.latest_path_infos.size();
}

String PSDiskDelegatorMulti::defaultPath() const
{
    return pool.latest_path_infos[default_path_index].path + "/" + path_prefix;
}

Strings PSDiskDelegatorMulti::listPaths() const
{
    // The delta data could be stored in all direcotries.
    std::vector<String> paths;
    for (size_t i = 0; i < pool.latest_path_infos.size(); ++i)
    {
        paths.push_back(pool.latest_path_infos[i].path + "/" + path_prefix);
    }
    return paths;
}

String PSDiskDelegatorMulti::choosePath(const PageFileIdAndLevel & id_lvl)
{
    std::function<String(const StoragePathPool::LatestPathInfos & paths, size_t idx)> path_generator =
        [this](const StoragePathPool::LatestPathInfos & paths, size_t idx) -> String {
        return paths[idx].path + "/" + this->path_prefix;
    };

    {
        std::lock_guard<std::mutex> lock{pool.mutex};
        /// If id exists in page_path_map, just return the same path
        if (auto iter = page_path_map.find(id_lvl); iter != page_path_map.end())
            return path_generator(pool.latest_path_infos, iter->second);
    }

    const String log_msg = "[type=ps_multi] [database=" + pool.database + "] [table=" + pool.table + "]";
    return genericChoosePath(pool.latest_path_infos, pool.global_capacity, path_generator, pool.log, log_msg);
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
        throw Exception("Unrecognized path " + upper_path);

    {
        std::lock_guard<std::mutex> lock{pool.mutex};
        if (need_insert_location)
            page_path_map[id_lvl] = index;
    }

    // update global used size
    pool.global_capacity->addUsedSize(upper_path, size_to_add);
    return index;
}

String PSDiskDelegatorMulti::getPageFilePath(const PageFileIdAndLevel & id_lvl) const
{
    std::lock_guard<std::mutex> lock{pool.mutex};
    auto iter = page_path_map.find(id_lvl);
    if (likely(iter != page_path_map.end()))
        return pool.latest_path_infos[iter->second].path + "/" + path_prefix;
    throw Exception("Can not find path for PageFile [id=" + toString(id_lvl.first) + "_" + toString(id_lvl.second) + "]");
}

void PSDiskDelegatorMulti::removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size, bool meta_left, bool remove_from_default_path)
{
    std::lock_guard<std::mutex> lock{pool.mutex};
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

size_t PSDiskDelegatorSingle::numPaths() const
{
    return 1;
}

String PSDiskDelegatorSingle::defaultPath() const
{
    return pool.latest_path_infos[0].path + "/" + path_prefix;
}

Strings PSDiskDelegatorSingle::listPaths() const
{
    // only stored in the first path.
    std::vector<String> paths;
    paths.push_back(pool.latest_path_infos[0].path + "/" + path_prefix);
    return paths;
}

String PSDiskDelegatorSingle::choosePath(const PageFileIdAndLevel & /*id_lvl*/)
{
    return pool.latest_path_infos[0].path + "/" + path_prefix;
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

String PSDiskDelegatorSingle::getPageFilePath(const PageFileIdAndLevel & /*id_lvl*/) const
{
    return pool.latest_path_infos[0].path + "/" + path_prefix;
}

void PSDiskDelegatorSingle::removePageFile(const PageFileIdAndLevel & /*id_lvl*/, size_t file_size, bool /*meta_left*/, bool /*remove_from_default_path*/)
{
    pool.global_capacity->freeUsedSize(pool.latest_path_infos[0].path, file_size);
}

//==========================================================================================
// Raft data
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

    {
        std::lock_guard lock{mutex};
        /// If id exists in page_path_map, just return the same path
        if (auto iter = page_path_map.find(id_lvl); iter != page_path_map.end())
            return path_generator(raft_path_infos, iter->second);
    }

    // Else choose path randomly
    const String log_msg = "[type=ps_raft]";
    return genericChoosePath(raft_path_infos, pool.global_capacity, path_generator, pool.log, log_msg);
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
        throw Exception("Unrecognized path " + upper_path);

    {
        std::lock_guard<std::mutex> lock{mutex};
        if (need_insert_location)
            page_path_map[id_lvl] = index;
    }

    // update global used size
    pool.global_capacity->addUsedSize(upper_path, size_to_add);
    return index;
}

String PSDiskDelegatorRaft::getPageFilePath(const PageFileIdAndLevel & id_lvl) const
{
    std::lock_guard<std::mutex> lock{mutex};
    auto iter = page_path_map.find(id_lvl);
    if (likely(iter != page_path_map.end()))
        return raft_path_infos[iter->second].path;
    throw Exception("Can not find path for PageFile [id=" + toString(id_lvl.first) + "_" + toString(id_lvl.second) + "]");
}

void PSDiskDelegatorRaft::removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size, bool meta_left, bool remove_from_default_path)
{
    std::lock_guard<std::mutex> lock{mutex};
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

} // namespace DB
