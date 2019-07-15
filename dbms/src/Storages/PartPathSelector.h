#pragma once

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <common/logger_useful.h>
#include <ctype.h>
#include <ext/scope_guard.h>
#include <map>
#include <string>
#include <vector>
#include <Common/escapeForFileName.h>

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
        std::stringstream log_buf;
        log_buf << "database: ";
        log_buf << database;
        log_buf << " table: ";
        log_buf << table;
        log_buf << " part name: ";
        log_buf << part;
        log_buf << " path index: ";
        log_buf << path_index;
        log_buf << " path: ";
        log_buf << all_path[path_index];
        log_buf << "data/";
        log_buf << database;
        log_buf << "/";
        log_buf << "/";
        LOG_DEBUG(log, log_buf.str());
        return all_path[path_index] + "data/" + escapeForFileName(database) + "/" + escapeForFileName(table) + "/";
    }

private:
    const std::vector<std::string> & all_path;
    Logger * log;
};

using PartPathSelectorPtr = std::shared_ptr<PartPathSelector>;
} // namespace DB