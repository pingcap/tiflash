#pragma once

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <common/logger_useful.h>
#include <ctype.h>
#include <ext/scope_guard.h>
#include <map>
#include <string>
#include <vector>

namespace DB
{
class MergeTreeData;
using String = std::string;
class PartPathSelector
{
public:
    PartPathSelector(const std::vector<String> & all_path) : all_path(all_path), log(&Logger::get("PartPathSelector"))
    {
        if (all_path.empty())
        {
            throw Exception("PartPathSelector need at least one path to give out");
        }
    }

    const String getPathForPart(MergeTreeData & data, const String & part_name) const;

private:
    const std::vector<String> & all_path;
    Logger * log;
};

using PartPathSelectorPtr = std::shared_ptr<PartPathSelector>;
} // namespace DB