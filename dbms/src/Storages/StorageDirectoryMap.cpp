#include <Storages/StorageDirectoryMap.h>
#include <fstream>
#include <string>
#include <Poco/StringTokenizer.h>


namespace DB {
void StorageDirectoryMap::tryInitializeFromFile()
{
    std::ifstream file(_persist_path);
    std::string line;
    while (std::getline(file, line))
    {
        Poco::StringTokenizer string_tokens(line, " ");
        std::vector<std::string> table_and_path;
        for (auto it = string_tokens.begin(); it != string_tokens.end(); it++) {
            table_and_path.push_back(*it);
        }
        if (table_and_path.size() != 2) {
            throw Exception("StorageDirectoryMap file wrong format");
        }
        _storage_to_directory.insert( std::pair<std::string, std::string>(table_and_path[0], table_and_path[1]) );
    }
}

void StorageDirectoryMap::addEntry(const std::string & database, const std::string & table, const std::string & path)
{
    _storage_to_directory.erase(database + "@" + table);
    _storage_to_directory.insert( std::pair<std::string, std::string>(database + "@" + table, path) );
}

std::string StorageDirectoryMap::getPathForStorage(const std::string & database, const std::string & table)
{
    auto it = _storage_to_directory.find(database + "@" + table);
    if (it != _storage_to_directory.end()) {
        return it->second;
    }
    if (path_iter == _all_path.end()) {
        path_iter = _all_path.begin();
    }
    std::string result = *path_iter + database + "/";
    path_iter++;
    addEntry(database, table, result);
    persist();
    return result;
}

void StorageDirectoryMap::removePathForStorage(const std::string & database, const std::string & table)
{
    _storage_to_directory.erase(database + "@" + table);
    persist();
}

void StorageDirectoryMap::persist()
{
    std::ofstream newFile(_persist_path);

    if (newFile.is_open()) {
        std::map<std::string, std::string>::iterator curit;
        for (auto it = _storage_to_directory.begin(); it != _storage_to_directory.end(); it++) {
            newFile << it->first << " " << it->second;
        }
    }
    else {
        throw Exception("StorageDirectoryMap cannot open file for persist");
    }

    newFile.close();
}

}