#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/PartPathSelector.h>
#include <common/likely.h>
#include <map>

namespace DB
{
const String PartPathSelector::getPathForPart(MergeTreeData & data, const String & part_name,
        const MergeTreePartInfo & info, size_t part_size) const
{
    // test whether this part can be put on fast path and return the path if it can
    if (hasFastPath())
    {
        size_t max_available_space = DiskSpaceMonitor::getUnreservedFreeSpace(all_path_[fast_path_start_index]);
        size_t path_index = fast_path_start_index;
        for (size_t i = fast_path_start_index + 1; i < all_path_.size(); i++)
        {
            size_t s = DiskSpaceMonitor::getUnreservedFreeSpace(all_path_[i]);
            if (s > max_available_space)
            {
                max_available_space = s;
                path_index = i;
            }
        }
        if (info.level == 0)
        {
            return all_path_[path_index];
        }
        else if (max_available_space >= settings.min_space_reserved_for_level_zero_parts
            && part_size <= settings.part_other_than_level_zero_max_ratio * max_available_space)
        {
            return all_path_[path_index];
        }
    }

    // there is only one normal path, just return it
    if (fast_path_start_index == 1)
    {
        return all_path_[0];
    }
    // find the normal path with least size of parts of this table
    std::unordered_map<String, UInt64> path_size_map;
    for (auto it = all_path_.begin(); it != all_path_.begin() + fast_path_start_index; it++)
    {
        path_size_map.emplace(*it, 0);
    }
    for (const auto & part : data.getDataPartsVector())
    {
        if (unlikely(path_size_map.find(part->full_path_prefix) == path_size_map.end()))
        {
            throw Exception("Part " + part->relative_path + " got unexpected path " + part->full_path_prefix, ErrorCodes::LOGICAL_ERROR);
        }
        path_size_map[part->full_path_prefix] += part->bytes_on_disk;
    }
    String result_path = all_path_[0];
    UInt64 parts_size_on_result_path = path_size_map[result_path];
    for (const auto & element : path_size_map)
    {
        if (element.second < parts_size_on_result_path)
        {
            result_path = element.first;
            parts_size_on_result_path = element.second;
        }
        LOG_TRACE(log, "Path " << element.first << " size is " << element.second << " bytes.");
    }
    LOG_DEBUG(log, "database: " << data.getDatabaseName() << " table: " << data.getTableName() << " part name: " << part_name << " path: " << result_path);

    return result_path;
}
} // namespace DB