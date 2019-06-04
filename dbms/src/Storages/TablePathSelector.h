#pragma once

#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>
#include <map>
#include <string>
#include <vector>

namespace DB
{
class TablePathSelector
{
public:
    TablePathSelector(const std::vector<std::string> & all_path, const std::string & persist_path_prefix)
        : all_path(all_path), persist_path(persist_path_prefix + "table_paths"), log(&Logger::get("StorageDirectoryMap"))
    {
        if (all_path.empty())
        {
            throw Exception("StorageDirectoryMap need at least one path to give out");
        }
        path_index = 0;
        tryInitializeFromFile();
    }

    const std::string getPathForStorage(const std::string & database, const std::string & table);
    void removePathForStorage(const std::string & database, const std::string & table);

private:
    std::map<std::string, std::string> table_paths;
    size_t path_index;
    const std::vector<std::string> & all_path;
    const std::string persist_path;
    Logger * log;

private:
    void tryInitializeFromFile();
    void addEntry(const std::string & database, const std::string & table, const std::string & path);
    void persist();
};

using TablePathSelectorPtr = std::shared_ptr<TablePathSelector>;
} // namespace DB