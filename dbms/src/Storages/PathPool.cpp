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

// Constructor to be used during initialization
PathPool::PathPool(const Strings & main_data_paths_, const Strings & latest_data_paths_, //
    PathCapacityMetricsPtr global_capacity_, FileProviderPtr file_provider_)
    : main_data_paths(main_data_paths_),
      latest_data_paths(latest_data_paths_),
      global_capacity(global_capacity_),
      file_provider(file_provider_)
{}

StoragePathPool PathPool::withTable(const String & database_, const String & table_, bool path_need_database_name_) const
{
    return StoragePathPool(main_data_paths, latest_data_paths, database_, table_, path_need_database_name_, global_capacity, file_provider);
}

std::vector<String> PathPool::listPaths() const
{
    std::set<String> path_set;
    for (const auto & p : main_data_paths)
        path_set.insert(Poco::Path{p + "/data"}.toString());
    for (const auto & p : latest_data_paths)
        path_set.insert(Poco::Path{p + "/data"}.toString());
    Strings paths;
    for (const auto & p : path_set)
        paths.emplace_back(p);
    return paths;
}

//==========================================================================================
// StoragePathPool
//==========================================================================================

StoragePathPool::StoragePathPool(                                       //
    const Strings & main_data_paths, const Strings & latest_data_paths, //
    String database_, String table_, bool path_need_database_name_,     //
    PathCapacityMetricsPtr global_capacity_, FileProviderPtr file_provider_)
    : database(std::move(database_)),
      table(std::move(table_)),
      path_need_database_name(path_need_database_name_),
      global_capacity(std::move(global_capacity_)),
      file_provider(std::move(file_provider_)),
      log(&Poco::Logger::get("StoragePathPool"))
{
    if (unlikely(database.empty() || table.empty()))
        throw Exception("Can NOT create StoragePathPool [database=" + database + "] [table=" + table + "]", ErrorCodes::LOGICAL_ERROR);

    for (const auto & p : main_data_paths)
    {
        MainPathInfo info;
        info.path = getStorePath(p + "/data", database, table);
        info.total_size = 0;
        main_path_infos.emplace_back(info);
    }
    for (const auto & p : latest_data_paths)
    {
        LatestPathInfo info;
        info.path = getStorePath(p + "/data", database, table);
        info.total_size = 0;
        latest_path_infos.emplace_back(info);
    }
}

StoragePathPool::StoragePathPool(const StoragePathPool & rhs)
    : main_path_infos(rhs.main_path_infos),
      latest_path_infos(rhs.latest_path_infos),
      dt_file_path_map(rhs.dt_file_path_map),
      page_path_map(rhs.page_path_map),
      database(rhs.database),
      table(rhs.table),
      path_need_database_name(rhs.path_need_database_name),
      global_capacity(rhs.global_capacity),
      file_provider(rhs.file_provider),
      log(rhs.log)
{}

StoragePathPool & StoragePathPool::operator=(const StoragePathPool & rhs)
{
    if (this != &rhs)
    {
        main_path_infos = rhs.main_path_infos;
        latest_path_infos = rhs.latest_path_infos;
        dt_file_path_map = rhs.dt_file_path_map;
        page_path_map = rhs.page_path_map;
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
                global_capacity->freeUsedSize(path_info.path, path_info.total_size);
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
                // update global used size
                global_capacity->freeUsedSize(path_info.path, path_info.total_size);
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
        return Poco::Path{extra_path_root + "/" + escapeForFileName(table_name)}.toString();
    else
        return Poco::Path{extra_path_root + "/" + escapeForFileName(database_name) + "/" + escapeForFileName(table_name)}.toString();
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
// Stable data
//==========================================================================================

Strings StableDelegator::listPaths() const
{
    std::lock_guard<std::mutex> lock{pool.mutex};
    std::vector<String> paths;
    for (size_t i = 0; i < pool.main_path_infos.size(); ++i)
    {
        paths.push_back(pool.main_path_infos[i].path + "/" + StoragePathPool::STABLE_FOLDER_NAME);
    }
    return paths;
}

String StableDelegator::choosePath() const
{
    std::lock_guard<std::mutex> lock{pool.mutex};
    UInt64 total_size = 0;
    for (size_t i = 0; i < pool.main_path_infos.size(); ++i)
    {
        total_size += pool.main_path_infos[i].total_size;
    }
    if (total_size == 0)
    {
        return pool.main_path_infos[0].path + "/" + StoragePathPool::STABLE_FOLDER_NAME;
    }

    std::vector<double> ratio;
    for (size_t i = 0; i < pool.main_path_infos.size(); ++i)
    {
        ratio.push_back((double)(total_size - pool.main_path_infos[i].total_size) / ((pool.main_path_infos.size() - 1) * total_size));
    }
    double rand_number = (double)rand() / RAND_MAX;
    double ratio_sum = 0;
    for (size_t i = 0; i < ratio.size(); i++)
    {
        ratio_sum += ratio[i];
        if ((rand_number < ratio_sum) || (i == ratio.size() - 1))
        {
            return pool.main_path_infos[i].path + "/" + StoragePathPool::STABLE_FOLDER_NAME;
        }
    }
    throw Exception("Should not reach here", ErrorCodes::LOGICAL_ERROR);
}

String StableDelegator::getDTFilePath(UInt64 file_id) const
{
    std::lock_guard<std::mutex> lock{pool.mutex};
    auto iter = pool.dt_file_path_map.find(file_id);
    if (likely(iter != pool.dt_file_path_map.end()))
        return pool.main_path_infos[iter->second].path + "/" + StoragePathPool::STABLE_FOLDER_NAME;
    throw Exception("Can not find path for DMFile [id=" + toString(file_id) + "]");
}

void StableDelegator::addDTFile(UInt64 file_id, size_t file_size, std::string_view path)
{
    path.remove_suffix(1 + strlen(StoragePathPool::STABLE_FOLDER_NAME)); // remove '/stable' added in listPathsForStable/getDTFilePath
    std::lock_guard<std::mutex> lock{pool.mutex};
    if (auto iter = pool.dt_file_path_map.find(file_id); iter != pool.dt_file_path_map.end())
    {
        auto & path_info = pool.main_path_infos[iter->second];
        path_info.total_size -= path_info.file_size_map.at(file_id);
        pool.dt_file_path_map.erase(iter);
        path_info.file_size_map.erase(file_id);
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
        throw Exception("Unrecognized path " + String(path));
    pool.dt_file_path_map.emplace(file_id, index);
    pool.main_path_infos[index].file_size_map.emplace(file_id, file_size);
    pool.main_path_infos[index].total_size += file_size;
    // update global used size
    pool.global_capacity->addUsedSize(path, file_size);
}

void StableDelegator::removeDTFile(UInt64 file_id)
{
    std::lock_guard<std::mutex> lock{pool.mutex};
    auto iter = pool.dt_file_path_map.find(file_id);
    if (unlikely(iter == pool.dt_file_path_map.end()))
        throw Exception("Cannot find DMFile for id " + toString(file_id));
    UInt32 index = iter->second;
    const auto file_size = pool.main_path_infos[index].file_size_map.at(file_id);
    pool.main_path_infos[index].total_size -= file_size;
    pool.dt_file_path_map.erase(file_id);
    pool.main_path_infos[index].file_size_map.erase(file_id);
    // update global used size
    pool.global_capacity->freeUsedSize(pool.main_path_infos[index].path, file_size);
}

//==========================================================================================
// Delta data
//==========================================================================================

size_t DeltaDelegator::numPaths() const { return pool.latest_path_infos.size(); }

String DeltaDelegator::normalPath() const { return pool.latest_path_infos[0].path + "/" + StoragePathPool::DELTA_FOLDER_NAME; }

Strings DeltaDelegator::listPaths() const
{
    std::lock_guard<std::mutex> lock{pool.mutex};
    // The delta data could be stored in all direcotries.
    std::vector<String> paths;
    for (size_t i = 0; i < pool.latest_path_infos.size(); ++i)
    {
        paths.push_back(pool.latest_path_infos[i].path + "/" + StoragePathPool::DELTA_FOLDER_NAME);
    }
    return paths;
}

String DeltaDelegator::choosePath(const PageFileIdAndLevel & id_lvl)
{
    auto return_path
        = [&](const size_t index) -> String { return pool.latest_path_infos[index].path + "/" + StoragePathPool::DELTA_FOLDER_NAME; };

    if (pool.latest_path_infos.size() == 1)
    {
        return return_path(0);
    }

    std::lock_guard<std::mutex> lock{pool.mutex};
    /// If id exists in page_path_map, just return the same path
    if (auto iter = pool.page_path_map.find(id_lvl); iter != pool.page_path_map.end())
    {
        return return_path(iter->second);
    }

    /// Else choose path randomly
    UInt64 total_size = 0;
    for (size_t i = 0; i < pool.latest_path_infos.size(); ++i)
    {
        total_size += pool.latest_path_infos[i].total_size;
    }
    if (total_size == 0)
    {
        LOG_DEBUG(pool.log, "database " + pool.database + " table " + pool.table + " no data currently. Choose path 0 for delta");
        return return_path(0);
    }

    std::vector<double> ratio;
    for (size_t i = 0; i < pool.latest_path_infos.size(); ++i)
    {
        ratio.push_back((double)(total_size - pool.latest_path_infos[i].total_size) / ((pool.latest_path_infos.size() - 1) * total_size));
    }
    double rand_number = (double)rand() / RAND_MAX;
    double ratio_sum = 0;
    for (size_t i = 0; i < ratio.size(); i++)
    {
        ratio_sum += ratio[i];
        if ((rand_number < ratio_sum) || (i == ratio.size() - 1))
        {
            LOG_DEBUG(pool.log, "database " + pool.database + " table " + pool.table + " choose path " + toString(i) + " for delta");
            return return_path(i);
        }
    }
    throw Exception("Should not reach here", ErrorCodes::LOGICAL_ERROR);
}

size_t DeltaDelegator::addPageFileUsedSize(
    const PageFileIdAndLevel & id_lvl, size_t size_to_add, const String & pf_parent_path, bool need_insert_location)
{
    if (pool.latest_path_infos.size() == 1)
    {
        // In this case, inserting to page_path_map or adding total_size for PathInfo seems useless.
        // Simply add used size for global capacity is OK.
        pool.global_capacity->addUsedSize(pf_parent_path, size_to_add);
        return 0;
    }

    // Get a normalized path without `StoragePathPool::DELTA_FOLDER_NAME` and trailing '/'
    String upper_path = Poco::Path(pf_parent_path).parent().toString();
    if (upper_path.back() == '/')
        upper_path.erase(upper_path.begin() + upper_path.size() - 1);
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
            pool.page_path_map[id_lvl] = index;
        pool.latest_path_infos[index].total_size += size_to_add;
    }

    // update global used size
    pool.global_capacity->addUsedSize(upper_path, size_to_add);
    return index;
}

String DeltaDelegator::getPageFilePath(const PageFileIdAndLevel & id_lvl) const
{
    if (pool.latest_path_infos.size() == 1)
        return pool.latest_path_infos[0].path + "/" + StoragePathPool::DELTA_FOLDER_NAME;

    std::lock_guard<std::mutex> lock{pool.mutex};
    auto iter = pool.page_path_map.find(id_lvl);
    if (likely(iter != pool.page_path_map.end()))
        return pool.latest_path_infos[iter->second].path + "/" + StoragePathPool::DELTA_FOLDER_NAME;
    throw Exception("Can not find path for PageFile [id=" + toString(id_lvl.first) + "_" + toString(id_lvl.second) + "]");
}

void DeltaDelegator::removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size)
{
    if (pool.latest_path_infos.size() == 1)
    {
        pool.global_capacity->freeUsedSize(pool.latest_path_infos[0].path, file_size);
        return;
    }

    std::lock_guard<std::mutex> lock{pool.mutex};
    auto iter = pool.page_path_map.find(id_lvl);
    if (unlikely(iter == pool.page_path_map.end()))
        return;
    auto index = iter->second;
    pool.latest_path_infos[index].total_size -= file_size;
    pool.page_path_map.erase(iter);

    pool.global_capacity->freeUsedSize(pool.latest_path_infos[index].path, file_size);
}

//==========================================================================================
// Normal data
//==========================================================================================

size_t NormalPathDelegator::numPaths() const { return 1; }

String NormalPathDelegator::normalPath() const { return pool.latest_path_infos[0].path + "/" + path_prefix; }

Strings NormalPathDelegator::listPaths() const
{
    std::lock_guard<std::mutex> lock{pool.mutex};
    // stored in the first directory.
    std::vector<String> paths;
    paths.push_back(pool.latest_path_infos[0].path + "/" + path_prefix);
    return paths;
}

String NormalPathDelegator::choosePath(const PageFileIdAndLevel & /*id_lvl*/) { return pool.latest_path_infos[0].path + "/" + path_prefix; }

size_t NormalPathDelegator::addPageFileUsedSize(
    const PageFileIdAndLevel & /*id_lvl*/, size_t size_to_add, const String & pf_parent_path, bool /*need_insert_location*/)
{
    // In this case, inserting to page_path_map or adding total_size for PathInfo seems useless.
    // Simply add used size for global capacity is OK.
    pool.global_capacity->addUsedSize(pf_parent_path, size_to_add);
    return 0;
}

String NormalPathDelegator::getPageFilePath(const PageFileIdAndLevel & /*id_lvl*/) const
{
    return pool.latest_path_infos[0].path + "/" + path_prefix;
}

void NormalPathDelegator::removePageFile(const PageFileIdAndLevel & /*id_lvl*/, size_t file_size)
{
    pool.global_capacity->freeUsedSize(pool.latest_path_infos[0].path, file_size);
}


} // namespace DB
