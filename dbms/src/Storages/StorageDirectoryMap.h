#pragma once

#include <map>
#include <string>
#include <vector>
#include <Common/Exception.h>

namespace DB {
class StorageDirectoryMap {
public:
    StorageDirectoryMap(std::vector<std::string> & all_path, std::string & persist_path): _all_path(all_path), _persist_path(persist_path) {
        if (_all_path.empty()) {
            throw Exception("StorageDirectoryMap need at least one path to give out");
        }
        path_iter = _all_path.begin();
        tryInitializeFromFile();
    }

    std::string getPathForStorage(const std::string & database, const std::string & table);
    void removePathForStorage(const std::string & database, const std::string & table);
private:
    std::map<std::string, std::string> _storage_to_directory;
    std::vector<std::string>::iterator path_iter;
    std::vector<std::string> _all_path;
    std::string _persist_path;
    void tryInitializeFromFile();
    void addEntry(const std::string & database, const std::string & table, const std::string & path);
    void persist();
};

using StorageDirectoryMapPtr = std::shared_ptr<StorageDirectoryMap>;
}