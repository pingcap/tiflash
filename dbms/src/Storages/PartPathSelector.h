#pragma once

#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>
#include <map>
#include <string>
#include <vector>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{
class PartPathSelector
{
public:
    PartPathSelector(const std::vector<std::string> & all_path)
        : all_path(all_path), log(&Logger::get("PartPathSelector"))
    {
        if (all_path.empty())
        {
            throw Exception("PartPathSelector need at least one path to give out");
        }
    }

    const std::string getPathForPart(const std::string & database, const std::string & table, const std::string & part)
    {
        int part_name_start = 0;
        if (startsWith(part, "tmp_"))
        {
            part_name_start = 4;
        }
        LOG_DEBUG(log, "part name: " + part);
        std::size_t path_index = std::hash<std::string>{}(database + "@" + table + "@" + part.substr(part_name_start)) % all_path.size();
        return all_path[path_index] + database + "/" + table + "/";
    }

private:
    const std::vector<std::string> & all_path;
    Logger * log;
};

using PartPathSelectorPtr = std::shared_ptr<PartPathSelector>;
} // namespace DB