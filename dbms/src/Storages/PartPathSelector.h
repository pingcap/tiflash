#pragma once

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <common/logger_useful.h>
#include <ctype.h>
#include <ext/scope_guard.h>
#include <map>
#include <string>
#include <vector>

namespace DB
{
class PartPathSelector
{
public:
    PartPathSelector(const std::vector<std::string> & all_path) : all_path(all_path), log(&Logger::get("PartPathSelector"))
    {
        if (all_path.empty())
        {
            throw Exception("PartPathSelector need at least one path to give out");
        }
    }

    const std::string getPathForPart(const std::string & database, const std::string & table, const std::string & part)
    {
        std::size_t path_index = std::hash<std::string>{}(database + "@" + table + "@" + part) % all_path.size();
        LOG_DEBUG(log,
            "database: " << database << " table: " << table << " part name: " << part << " path index: " << path_index
                         << " path: " << all_path[path_index] + "data/" + database + "/" + table + "/");
        return all_path[path_index] + "data/" + database + "/" + table + "/";
    }

private:
    const std::vector<std::string> & all_path;
    Logger * log;
};

using PartPathSelectorPtr = std::shared_ptr<PartPathSelector>;
} // namespace DB