#include <Storages/PartPathSelector.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{
const String PartPathSelector::getPathForPart(MergeTreeData & data, const String & part_name) const
{
    std::unordered_map<String, size_t> path_size_map;
    for (auto & path : all_path)
    {
        path_size_map.emplace(path + "data/", 0);
    }
    for (auto & part : data.getDataPartsVector())
    {
        if (path_size_map.find(part->full_path_prefix) == path_size_map.end())
        {
            throw Exception("Part got unexpected path. This is bug.", ErrorCodes::LOGICAL_ERROR);
        }
        path_size_map[part->full_path_prefix] += part->bytes_on_disk;
    }
    String result;
    size_t parts_size = 0;
    for (auto element : path_size_map)
    {
        if (element.second >= parts_size)
        {
            result = element.first;
            parts_size = element.second;
        }
        LOG_DEBUG(log, "Path " << element.first << " size is " << element.second << " bytes.");
    }

    std::stringstream log_buf;
    log_buf << "database: ";
    log_buf << data.getDatabaseName();
    log_buf << " table: ";
    log_buf << data.getTableName();
    log_buf << " part name: ";
    log_buf << part_name;
    log_buf << " path: ";
    log_buf << result;
    log_buf << "data/";

    LOG_DEBUG(log, log_buf.str());
    return result;
}
} // namespace DB