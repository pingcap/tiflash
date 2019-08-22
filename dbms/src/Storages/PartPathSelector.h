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
struct PartPathSelectorSetting {

};
class PartPathSelector
{
public:
    PartPathSelector(std::vector<String> && all_normal_path, std::vector<std::string> && all_fast_path)
        : all_path_(all_normal_path), log(&Logger::get("PartPathSelector"))
    {
        if (all_path_.empty())
        {
            throw Exception("PartPathSelector need at least one path to give out");
        }
        fast_path_start_index = all_path_.size();
        if (!all_fast_path.empty())
        {
            for (auto & path : all_fast_path)
            {
                // all_fast_path cannot be in all_path
                if (unlikely(std::find(all_normal_path.begin(), all_normal_path.end(), path) != all_normal_path.end()))
                {
                    throw Exception("Fast path shouldn't be included in normal path");
                }
                all_path_.emplace_back(std::move(path));
            }
        }
    }

    const String getPathForPart(MergeTreeData & data, const String & part_name,
            const MergeTreePartInfo & info, size_t part_size=0) const;

    const std::vector<String> & getAllPath()
    {
        return all_path_;
    }

    bool hasFastPath() const
    {
        return all_path_.size() == fast_path_start_index;
    }

    struct Settings
    {
        double part_other_than_level_zero_max_ratio = 0.2;
        size_t min_space_reserved_for_level_zero_parts = 100ULL * 1024 * 1024 * 1024;
    };

private:
    std::vector<String> all_path_;
    size_t fast_path_start_index;
    Logger * log;
private:
    const Settings settings;
};

using PartPathSelectorPtr = std::shared_ptr<PartPathSelector>;
} // namespace DB