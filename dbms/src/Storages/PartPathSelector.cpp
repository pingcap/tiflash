#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/PartPathSelector.h>
#include <common/likely.h>
#include <map>

namespace DB
{
const String PartPathSelector::getPathForPart(MergeTreeData & data, const String & part_name,
        const MergeTreePartInfo & info, int part_size=0) const
{
    if (!all_fast_path.empty())
    {
        size_t max_available_space = DiskSpaceMonitor::getUnreservedFreeSpace(all_fast_path[0]);
        std::string & result_path = all_fast_path[0];
        for (auto & path : all_fast_path)
        {
            size_t s = DiskSpaceMonitor::getUnreservedFreeSpace(path);
            if (s > max_available_space)
            {
                max_available_space = s;
                result_path = path;
            }
        }
        if (info.level == 0)
        {
            return result_path;
        }
        else if (max_available_space >= min_space_reserved_for_level_zero_parts
            && part_size <= max_part_ratio * max_available_space)
        {
            return result_path;
        }
    }
    if (all_path.size() == 1)
    {
        return all_path[0];
    }
    std::unordered_map<String, UInt64> path_size_map;
    for (const auto & path : all_path)
    {
        path_size_map.emplace(path, 0);
    }
    for (const auto & part : data.getDataPartsVector())
    {
        if (unlikely(path_size_map.find(part->full_path_prefix) == path_size_map.end()))
        {
            throw Exception("Part " + part->relative_path + " got unexpected path " + part->full_path_prefix, ErrorCodes::LOGICAL_ERROR);
        }
        path_size_map[part->full_path_prefix] += part->bytes_on_disk;
    }
    String result = all_path[0];
    UInt64 parts_size = path_size_map[result];
    for (const auto & element : path_size_map)
    {
        if (element.second < parts_size)
        {
            result = element.first;
            parts_size = element.second;
        }
        LOG_DEBUG(log, "Path " << element.first << " size is " << element.second << " bytes.");
    }

    LOG_DEBUG(log, "database: " << data.getDatabaseName() << " table: " << data.getTableName() << " part name: " << part_name << " path: " << result);
    return result;
}
} // namespace DB