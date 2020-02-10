#pragma once

#include <Core/Types.h>
#include <random>
#include <unordered_map>

namespace DB
{
class PathPool
{
public:
    using PathMap = std::unordered_map<UInt32, String>;
    using IdAndPath = std::pair<UInt32, String>;
    using Paths = std::vector<IdAndPath>;

    PathPool() = default;

    PathPool(const Paths & paths_) : paths(paths_)
    {
        for (auto & [id, path] : paths_)
        {
            if (path_map.count(id))
                throw Exception("Duplicated id :" + DB::toString(id));
            path_map.emplace(id, path);
        }
    }

    PathPool(const Paths & paths_, const String & database_, const String & table_) : database(database_), table(table_)
    {
        for (auto & [id, path] : paths_)
        {
            if (path_map.count(id))
                throw Exception("Duplicated id :" + DB::toString(id));

            auto p = path + "/" + database + "/" + table;
            path_map.emplace(id, p);
            paths.emplace_back(id, p);
        }
    }

    PathPool withTable(const String & database_, const String & table_) const
    {
        if (unlikely(!database.empty() || !table.empty()))
            throw Exception("Already has database or table");
        return PathPool(paths, database_, table_);
    }

    IdAndPath choosePath() const { return paths[rand() % path_map.size()]; }

    String getPath(UInt32 id) const { return path_map.at(id); }

    const Paths & listPaths() const { return paths; }

    bool empty() const { return paths.empty(); }

private:
    PathMap path_map;
    Paths paths;

    String database;
    String table;
};

using PathPoolPtr = std::shared_ptr<PathPool>;

} // namespace DB