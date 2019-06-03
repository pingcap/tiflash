#include <Common/escapeForFileName.h>
#include <Poco/File.h>
#include <Poco/StringTokenizer.h>
#include <Storages/StorageDirectoryMap.h>
#include <fstream>
#include <string>


namespace DB
{
void StorageDirectoryMap::tryInitializeFromFile()
{
    LOG_DEBUG(log, "StorageDirectoryMap begin to initialize from file: " + persist_path);
    std::ifstream file(persist_path);
    std::string line;
    while (std::getline(file, line))
    {
        Poco::StringTokenizer string_tokens(line, " ");
        std::vector<std::string> table_and_path;
        for (auto it = string_tokens.begin(); it != string_tokens.end(); it++)
        {
            table_and_path.push_back(*it);
        }
        if (table_and_path.size() != 2)
        {
            throw Exception("StorageDirectoryMap file wrong format");
        }
        table_paths.insert(std::pair<std::string, std::string>(table_and_path[0], table_and_path[1]));
    }
}

void StorageDirectoryMap::addEntry(const std::string & database, const std::string & table, const std::string & path)
{
    table_paths.erase(database + "@" + table);
    table_paths.insert(std::pair<std::string, std::string>(database + "@" + table, path));
}

const std::string StorageDirectoryMap::getPathForStorage(const std::string & database, const std::string & table)
{
    LOG_INFO(log, "Trying to get data path for Database " << database << " Table " << table << " from StorageDirectoryMap");
    auto it = table_paths.find(database + "@" + table);
    if (it != table_paths.end())
    {
        return it->second;
    }
    if (path_iter == all_path.end())
    {
        path_iter = all_path.begin();
    }
    std::string result = *path_iter + "data/" + escapeForFileName(database) + "/";
    path_iter++;
    addEntry(database, table, result);
    persist();
    return result;
}

void StorageDirectoryMap::removePathForStorage(const std::string & database, const std::string & table)
{
    table_paths.erase(database + "@" + table);
    persist();
}

void StorageDirectoryMap::persist()
{
    std::ofstream newFile(persist_path);

    if (newFile.is_open())
    {
        for (auto it = table_paths.begin(); it != table_paths.end(); it++)
        {
            newFile << it->first << " " << it->second << std::endl;
        }
    }
    else
    {
        throw Exception("StorageDirectoryMap cannot open file for persist");
    }

    newFile.close();
}

} // namespace DB