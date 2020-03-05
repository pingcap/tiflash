#pragma once

#include <Core/Types.h>
#include <random>
#include <unordered_map>

namespace DB
{
class PathPool
{
public:
    using DMFilePathMap = std::unordered_map<UInt64, UInt32>;
    using DMFileSizes = std::unordered_map<UInt64, size_t>;
    using Paths = std::vector<String>;
    using PathSizes = std::vector<size_t>;

    PathPool() = default;

    PathPool(const Paths & paths_) : paths(paths_), log{&Logger::get("PathPool")}
    {
        path_sizes.resize(paths.size(), 0);
    }

    PathPool(const Paths & paths_, const String & database_, const String & table_) : database(database_), table(table_), log{&Logger::get("PathPool")}
    {
        for (auto & path : paths_)
        {
            paths.emplace_back(path + "/" + database + "/" + table);
        }
        path_sizes.resize(paths.size(), 0);
    }

    PathPool withTable(const String & database_, const String & table_) const
    {
        if (unlikely(!database.empty() || !table.empty()))
            throw Exception("Already has database or table");
        return PathPool(paths, database_, table_);
    }

    const String & choosePath() const
    {
        UInt64 total_size = std::accumulate(path_sizes.begin(), path_sizes.end(), 0UL);
        if (total_size == 0)
        {
            LOG_DEBUG(log, "database " + database + " table " + table + " no dmfile currently. Choose path 0.");
            return paths[0];
        }

        std::vector<double> ratio;
        for (auto s : path_sizes)
        {
            ratio.push_back((double)(total_size - s) / ((paths.size() - 1) * total_size));
        }
        double rand_number = (double) rand() / RAND_MAX;
        double ratio_sum = 0;
        for (size_t i = 0; i < ratio.size(); i++)
        {
            ratio_sum += ratio[i];
            if ((rand_number < ratio_sum) || (i == ratio.size() - 1))
            {
                LOG_DEBUG(log, "database " + database + " table " + table + " choose path " + std::to_string(i));
                return paths[i];
            }
        }
        throw Exception("Should not reach here", ErrorCodes::LOGICAL_ERROR);
    }

    const String & getPath(UInt64 file_id) const
    {
        if (unlikely(path_map.find(file_id) == path_map.end()))
            throw Exception("Cannot find DMFile for id " + std::to_string(file_id));
        return paths[path_map.at(file_id)];
    }

    void addDMFile(UInt64 file_id, size_t file_size, const String& path)
    {
        if (path_map.find(file_id) != path_map.end())
        {
            path_sizes[path_map.at(file_id)] -= file_size_map.at(file_id);
            path_map.erase(file_id);
            file_size_map.erase(file_id);
        }
        auto iter = std::find(paths.begin(), paths.end(), path);
        if (unlikely(iter == paths.end()))
            throw Exception("Unrecognized path " + path);
        UInt32 index = std::distance(paths.begin(), iter);
        path_map.emplace(file_id, index);
        file_size_map.emplace(file_id, file_size);
        path_sizes[index] += file_size;
    }

    void removeDMFile(UInt64 file_id)
    {
        if (unlikely(path_map.find(file_id) == path_map.end()))
            throw Exception("Cannot find DMFile for id " + std::to_string(file_id));
        path_sizes[path_map.at(file_id)] -= file_size_map.at(file_id);
        path_map.erase(file_id);
        file_size_map.erase(file_id);
    }

    const Paths & listPaths() const { return paths; }

    bool empty() const { return paths.empty(); }

private:
    DMFilePathMap path_map;
    DMFileSizes file_size_map;
    Paths paths;
    PathSizes path_sizes;

    String database;
    String table;

    Logger * log;
};

using PathPoolPtr = std::shared_ptr<PathPool>;

} // namespace DB