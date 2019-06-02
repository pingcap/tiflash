#pragma once

#include <Common/Exception.h>
#include <map>
#include <string>
#include <vector>

namespace DB
{
class StorageDirectoryMap
{
public:
    StorageDirectoryMap(const std::vector<std::string> & all_path, const std::string & persist_path) : all_path(all_path), persist_path(persist_path)
    {
        if (all_path.empty())
        {
            throw Exception("StorageDirectoryMap need at least one path to give out");
        }
        path_iter = all_path.cbegin();
        tryInitializeFromFile();
    }

    const std::string getPathForStorage(const std::string & database, const std::string & table);
    void removePathForStorage(const std::string & database, const std::string & table);

private:
    std::map<std::string, std::string> table_paths;
    std::vector<std::string>::const_iterator path_iter;
    const std::vector<std::string> & all_path;
    const std::string & persist_path;
    void tryInitializeFromFile();
    void addEntry(const std::string & database, const std::string & table, const std::string & path);
    void persist();
};

using StorageDirectoryMapPtr = std::shared_ptr<StorageDirectoryMap>;
} // namespace DB