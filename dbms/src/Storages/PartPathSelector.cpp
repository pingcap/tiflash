#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/PartPathSelector.h>
#include <common/likely.h>

namespace DB
{
const String PartPathSelector::getPathForPart(MergeTreeData & data, const String & part_name) const
{
    std::unordered_map<String, size_t> path_size_map;
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
    size_t parts_size = path_size_map[result];
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