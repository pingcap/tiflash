#pragma once

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <common/logger_useful.h>
#include <cstdlib>
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
    PartPathSelector(std::vector<String> && all_normal_path, std::vector<std::string> && all_fast_path)
        : normal_and_fast_path(all_normal_path), log(&Logger::get("PartPathSelector"))
    {
        if (normal_and_fast_path.empty())
        {
            throw Exception("PartPathSelector need at least one path to give out");
        }
        for (size_t i = 0; i < normal_and_fast_path.size(); i++)
        {
            normal_path_to_index_map.emplace(normal_and_fast_path[i], i);
        }
        fast_path_start_index = normal_and_fast_path.size();
        if (!all_fast_path.empty())
        {
            for (auto & path : all_fast_path)
            {
                // all_fast_path cannot be in all_path
                if (unlikely(std::find(all_normal_path.begin(), all_normal_path.end(), path) != all_normal_path.end()))
                {
                    throw Exception("Fast path shouldn't be included in normal path");
                }
                normal_and_fast_path.emplace_back(std::move(path));
            }
        }
    }

    const String getPathForPart(MergeTreeData & data, const String & part_name, const MergeTreePartInfo & info, size_t part_size = 0) const;

    const std::vector<String> & getAllPath() { return normal_and_fast_path; }

    bool hasFastPath() const { return normal_and_fast_path.size() > fast_path_start_index; }

    size_t getRandomFastPathIndex() const
    {
        if (unlikely(normal_and_fast_path.size() <= fast_path_start_index))
        {
            throw Exception("There is no fast path configured.");
        }
        return std::rand() % (normal_and_fast_path.size() - fast_path_start_index) + fast_path_start_index;
    }

    size_t getRandomNormalPathIndex() const
    {
        if (unlikely(fast_path_start_index <= 0))
        {
            throw Exception("There is no normal path configured.");
        }
        return std::rand() % fast_path_start_index;
    }

    struct Settings
    {
        double part_other_than_level_zero_max_ratio = 0.2;
        size_t min_space_reserved_for_level_zero_parts = 100ULL * 1024 * 1024 * 1024;
    };

private:
    std::vector<String> normal_and_fast_path;
    std::unordered_map<String, size_t> normal_path_to_index_map;
    size_t fast_path_start_index;
    Logger * log;

private:
    const Settings settings;
};

using PartPathSelectorPtr = std::shared_ptr<PartPathSelector>;
} // namespace DB