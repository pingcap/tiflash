#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/PartPathSelector.h>
#include <common/likely.h>
#include <map>

namespace DB
{
const String PartPathSelector::getPathForPart(
    MergeTreeData & data, const String & part_name, const MergeTreePartInfo & info, size_t part_size) const
{
    // test whether this part can be put on fast path and return the path if it can
    if (hasFastPath())
    {
        if (info.level == 0)
        {
            return normal_and_fast_path[getRandomFastPathIndex()];
        }
        size_t max_available_space = DiskSpaceMonitor::getUnreservedFreeSpace(normal_and_fast_path[fast_path_start_index]);
        size_t path_index = fast_path_start_index;
        for (size_t i = fast_path_start_index + 1; i < normal_and_fast_path.size(); i++)
        {
            size_t s = DiskSpaceMonitor::getUnreservedFreeSpace(normal_and_fast_path[i]);
            if (s > max_available_space)
            {
                max_available_space = s;
                path_index = i;
            }
        }

        if (max_available_space >= settings.min_space_reserved_for_level_zero_parts
            && part_size <= settings.part_other_than_level_zero_max_ratio * max_available_space)
        {
            return normal_and_fast_path[path_index];
        }
    }

    // there is only one normal path, just return it
    if (fast_path_start_index == 1)
    {
        return normal_and_fast_path[0];
    }
    if (info.level == 0)
    {
        return normal_and_fast_path[getRandomNormalPathIndex()];
    }
    // find the normal path with least size of parts of this table
    std::vector<UInt64> path_parts_size;
    path_parts_size.resize(fast_path_start_index, 0);
    for (const auto & part : data.getDataPartsVector())
    {
        if (unlikely(normal_path_to_index_map.find(part->full_path_prefix) == normal_path_to_index_map.end()))
        {
            throw Exception("Part " + part->relative_path + " got unexpected path " + part->full_path_prefix, ErrorCodes::LOGICAL_ERROR);
        }
        path_parts_size[normal_path_to_index_map.at(part->full_path_prefix)] += part->bytes_on_disk;
    }
    size_t result_path_index = 0;
    UInt64 parts_size_on_result_path = path_parts_size[0];
    for (size_t i = 1; i < fast_path_start_index; i++)
    {
        if (path_parts_size[i] < parts_size_on_result_path)
        {
            result_path_index = i;
            parts_size_on_result_path = path_parts_size[i];
        }
        LOG_TRACE(log, "Path " << normal_and_fast_path[i] << " size is " << path_parts_size[i] << " bytes.");
    }
    LOG_TRACE(log,
        "database: " << data.getDatabaseName() << " table: " << data.getTableName() << " part name: " << part_name
                     << " path: " << normal_and_fast_path[result_path_index]);

    return normal_and_fast_path[result_path_index];
}
} // namespace DB