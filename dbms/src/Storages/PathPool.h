#pragma once

#include <Common/escapeForFileName.h>
#include <Core/Types.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/PathCapacityMetrics.h>
#include <common/logger_useful.h>

#include <random>
#include <unordered_map>

namespace DB
{
class PathPool
{
public:
    using DMFilePathMap = std::unordered_map<UInt64, UInt32>;
    struct PathInfo
    {
        String path;
        size_t total_size;
        std::unordered_map<UInt64, size_t> file_size_map;
    };
    using PathInfos = std::vector<PathInfo>;

    PathPool() = default;

    PathPool(const std::vector<String> & paths_, PathCapacityMetricsPtr global_capacity_, FileProviderPtr file_provider_)
        : global_capacity{std::move(global_capacity_)}, file_provider{std::move(file_provider_)}, log{&Logger::get("PathPool")}
    {
        for (auto & path : paths_)
        {
            PathInfo info;
            info.path = path;
            info.total_size = 0;
            path_infos.emplace_back(info);
        }
    }

private:
    PathPool(const std::vector<String> & paths_, const String & database_, const String & table_, bool path_need_database_name_,
        PathCapacityMetricsPtr global_capacity_, FileProviderPtr file_provider_)
        : database(database_),
          table(table_),
          path_need_database_name{path_need_database_name_},
          global_capacity{std::move(global_capacity_)},
          file_provider{std::move(file_provider_)},
          log{&Logger::get("PathPool")}
    {
        for (auto & path : paths_)
        {
            PathInfo info;
            info.path = getStorePath(path, database, table);
            info.total_size = 0;
            path_infos.emplace_back(info);
        }
    }

public:
    PathPool(const PathPool & path_pool) : global_capacity{path_pool.global_capacity}, file_provider{path_pool.file_provider}
    {
        path_infos.clear();
        path_map = path_pool.path_map;
        for (auto & path_info : path_pool.path_infos)
        {
            path_infos.emplace_back(path_info);
        }
        database = path_pool.database;
        table = path_pool.table;
        path_need_database_name = path_pool.path_need_database_name;
        log = path_pool.log;
    }

    PathPool & operator=(const PathPool & path_pool)
    {
        path_infos.clear();
        path_map = path_pool.path_map;
        for (auto & path_info : path_pool.path_infos)
        {
            path_infos.emplace_back(path_info);
        }
        database = path_pool.database;
        table = path_pool.table;
        path_need_database_name = path_pool.path_need_database_name;
        global_capacity = path_pool.global_capacity;
        file_provider = path_pool.file_provider;
        log = path_pool.log;
        return *this;
    }

    PathPool withTable(const String & database_, const String & table_, bool path_need_database_name_) const
    {
        if (unlikely(!database.empty() || !table.empty()))
            throw Exception("Already has database or table");
        std::vector<String> paths_;
        for (auto & path_info : path_infos)
        {
            paths_.emplace_back(path_info.path);
        }
        return PathPool(paths_, database_, table_, path_need_database_name_, global_capacity, file_provider);
    }

    void rename(const String & new_database, const String & new_table, bool clean_rename)
    {
        if (unlikely(database.empty() && table.empty()))
            throw Exception("Can not do rename for root PathPool");

        if (unlikely(new_database.empty() || new_table.empty()))
            throw Exception("Can not rename for PathPool to " + new_database + "." + new_table);

        if (clean_rename)
        {
            // caller ensure that no path need to be renamed.
            if (unlikely(path_need_database_name))
            {
                throw Exception("Can not do clean rename with path_need_database_name is true!");
            }
            std::lock_guard<std::mutex> lock{mutex};
            database = new_database;
            table = new_table;
        }
        else
        {
            if (unlikely(file_provider->isEncryptionEnabled()))
            {
                throw Exception("Encryption is only supported when using clean_rename");
            }
            // Note: changing these path is not atomic, we may lost data if process is crash here.

            std::lock_guard<std::mutex> lock{mutex};
            // Get root path without database and table
            std::vector<String> root_paths;
            for (auto & path_info : path_infos)
            {
                String root_path = Poco::Path(path_info.path).parent().toString();
                root_paths.emplace_back(root_path);
            }

            std::vector<String> new_paths;
            for (const auto & root_path : root_paths)
            {
                const String new_path = getStorePath(root_path, new_database, new_table);
                new_paths.emplace_back(new_path);
                renamePath(getStorePath(root_path, database, table), new_path);
            }

            database.clear();
            table.clear();
            *this = withTable(new_database, new_table, path_need_database_name);
        }
    }

    void drop(bool recursive, bool must_success = true)
    {
        if (unlikely(database.empty() && table.empty()))
            throw Exception("Can not do drop for root PathPool");

        std::lock_guard<std::mutex> lock{mutex};
        for (auto & path_info : path_infos)
        {
            try
            {
                Poco::File dir(path_info.path);
                if (dir.exists())
                    file_provider->deleteDirectory(dir.path(), false, recursive);

                // update global used size
                global_capacity->freeUsedSize(path_info.path, path_info.total_size);
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

    const String & choosePath() const
    {
        std::lock_guard<std::mutex> lock{mutex};
        UInt64 total_size = 0;
        for (auto & path_info : path_infos)
        {
            total_size += path_info.total_size;
        }
        if (total_size == 0)
        {
            LOG_DEBUG(log, "database " + database + " table " + table + " no dmfile currently. Choose path 0.");
            return path_infos[0].path;
        }

        std::vector<double> ratio;
        for (auto & path_info : path_infos)
        {
            ratio.push_back((double)(total_size - path_info.total_size) / ((path_infos.size() - 1) * total_size));
        }
        double rand_number = (double)rand() / RAND_MAX;
        double ratio_sum = 0;
        for (size_t i = 0; i < ratio.size(); i++)
        {
            ratio_sum += ratio[i];
            if ((rand_number < ratio_sum) || (i == ratio.size() - 1))
            {
                LOG_DEBUG(log, "database " + database + " table " + table + " choose path " + std::to_string(i));
                return path_infos[i].path;
            }
        }
        throw Exception("Should not reach here", ErrorCodes::LOGICAL_ERROR);
    }

    const String & getPath(UInt64 file_id) const
    {
        std::lock_guard<std::mutex> lock{mutex};
        if (unlikely(path_map.find(file_id) == path_map.end()))
            throw Exception("Cannot find DMFile for id " + std::to_string(file_id));
        return path_infos[path_map.at(file_id)].path;
    }

    void addDMFile(UInt64 file_id, size_t file_size, const String & path)
    {
        std::lock_guard<std::mutex> lock{mutex};
        if (path_map.find(file_id) != path_map.end())
        {
            auto & path_info = path_infos[path_map.at(file_id)];
            path_info.total_size -= path_info.file_size_map.at(file_id);
            path_map.erase(file_id);
            path_info.file_size_map.erase(file_id);
        }
        UInt32 index = UINT32_MAX;
        for (size_t i = 0; i < path_infos.size(); i++)
        {
            if (path_infos[i].path == path)
            {
                index = i;
                break;
            }
        }
        if (unlikely(index == UINT32_MAX))
            throw Exception("Unrecognized path " + path);
        path_map.emplace(file_id, index);
        path_infos[index].file_size_map.emplace(file_id, file_size);
        path_infos[index].total_size += file_size;
        // update global used size
        global_capacity->addUsedSize(path, file_size);
    }

    void removeDMFile(UInt64 file_id)
    {
        std::lock_guard<std::mutex> lock{mutex};
        if (unlikely(path_map.find(file_id) == path_map.end()))
            throw Exception("Cannot find DMFile for id " + std::to_string(file_id));
        UInt32 index = path_map.at(file_id);
        const auto file_size = path_infos[index].file_size_map.at(file_id);
        path_infos[index].total_size -= file_size;
        path_map.erase(file_id);
        path_infos[index].file_size_map.erase(file_id);
        // update global used size
        global_capacity->freeUsedSize(path_infos[index].path, file_size);
    }

    std::vector<String> listPaths() const
    {
        std::lock_guard<std::mutex> lock{mutex};
        std::vector<String> paths;
        for (auto & path_info : path_infos)
        {
            paths.push_back(path_info.path);
        }
        return paths;
    }

    bool empty() const
    {
        std::lock_guard<std::mutex> lock{mutex};
        return path_infos.empty();
    }

private:
    String getStorePath(const String & extra_path_root, const String & database_name, const String & table_name)
    {
        if (path_need_database_name)
            return extra_path_root + (endsWith(extra_path_root, "/") ? "" : "/") + escapeForFileName(database_name) + "/" + escapeForFileName(table_name);
        else
            return extra_path_root + (endsWith(extra_path_root, "/") ? "" : "/") + escapeForFileName(table_name);
    }

    void renamePath(const String & old_path, const String & new_path)
    {
        LOG_INFO(log, "Renaming " << old_path << " to " << new_path);
        if (auto file = Poco::File{old_path}; file.exists())
            file.renameTo(new_path);
        else
            LOG_WARNING(log, "Path \"" << old_path << "\" is missed.");
    }

private:
    DMFilePathMap path_map;
    PathInfos path_infos;

    String database;
    String table;

    mutable std::mutex mutex;

    bool path_need_database_name = false;

    PathCapacityMetricsPtr global_capacity;

    FileProviderPtr file_provider;

    Poco::Logger * log;
};

using PathPoolPtr = std::shared_ptr<PathPool>;

} // namespace DB
