#pragma once

#include <Core/Types.h>
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

    PathPool(const std::vector<String> & paths_) : log{&Logger::get("PathPool")}
    {
        for (auto & path : paths_)
        {
            PathInfo info;
            info.path = path;
            info.total_size = 0;
            path_infos.emplace_back(info);
        }
    }

    PathPool(const std::vector<String> & paths_, const String & database_, const String & table_)
        : database(database_), table(table_), log{&Logger::get("PathPool")}
    {
        for (auto & path : paths_)
        {
            PathInfo info;
            info.path = path + "/" + database + "/" + table;
            info.total_size = 0;
            path_infos.emplace_back(info);
        }
    }

    PathPool(const PathPool & path_pool)
    {
        path_infos.clear();
        path_map = path_pool.path_map;
        for (auto & path_info : path_pool.path_infos)
        {
            path_infos.emplace_back(path_info);
        }
        database = path_pool.database;
        table = path_pool.table;
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
        log = path_pool.log;
        return *this;
    }

    PathPool withTable(const String & database_, const String & table_) const
    {
        if (unlikely(!database.empty() || !table.empty()))
            throw Exception("Already has database or table");
        std::vector<String> paths_;
        for (auto & path_info : path_infos)
        {
            paths_.emplace_back(path_info.path);
        }
        return PathPool(paths_, database_, table_);
    }

    const String & choosePath()
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

    const String & getPath(UInt64 file_id)
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
    }

    void removeDMFile(UInt64 file_id)
    {
        std::lock_guard<std::mutex> lock{mutex};
        if (unlikely(path_map.find(file_id) == path_map.end()))
            throw Exception("Cannot find DMFile for id " + std::to_string(file_id));
        UInt32 index = path_map.at(file_id);
        path_infos[index].total_size -= path_infos[index].file_size_map.at(file_id);
        path_map.erase(file_id);
        path_infos[index].file_size_map.erase(file_id);
    }

    std::vector<String> listPaths() const
    {
        std::vector<String> paths;
        for (auto & path_info : path_infos)
        {
            paths.push_back(path_info.path);
        }
        return paths;
    }

    bool empty() const { return path_infos.empty(); }

private:
    DMFilePathMap path_map;
    PathInfos path_infos;

    String database;
    String table;

    std::mutex mutex;

    Poco::Logger * log;
};

using PathPoolPtr = std::shared_ptr<PathPool>;

} // namespace DB
