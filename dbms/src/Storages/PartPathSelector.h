#pragma once

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <common/logger_useful.h>
#include <string>
#include <vector>

namespace DB
{
class MergeTreeData;
struct MergeTreePartInfo;
using String = std::string;
class PartPathSelector
{
public:
    PartPathSelector(const std::vector<String> & all_path, std::vector<std::string> && all_fast_path)
        : all_path(all_path), all_fast_path(std::move(all_fast_path)), log(&Logger::get("PartPathSelector"))
    {
        if (all_path.empty())
        {
            throw Exception("PartPathSelector need at least one path to give out");
        }
    }

    const String getPathForPart(MergeTreeData & data, const String & part_name,
            const MergeTreePartInfo & info, int part_size=0) const;

private:
    const std::vector<String> & all_path;
    std::vector<String> all_fast_path;
    Logger * log;
private:
    const static double max_part_ratio = 0.2;
    const static size_t min_space_reserved_for_level_zero_parts = 100 * 1024 * 1024 * 1024;
};

using PartPathSelectorPtr = std::shared_ptr<PartPathSelector>;
} // namespace DB