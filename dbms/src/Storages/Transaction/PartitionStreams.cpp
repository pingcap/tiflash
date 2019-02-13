#include <common/logger_useful.h>
#include <functional>

#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/PartitionDataMover.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionPartition.h>
#include <Storages/Transaction/TMTContext.h>

#include <DataStreams/BlocksListBlockInputStream.h>

namespace DB
{

BlockInputStreamPtr RegionPartition::getBlockInputStreamByPartition(TableID table_id, UInt64 partition_id,
    const TiDB::TableInfo & table_info, const ColumnsDescription & columns, const Names & ordered_columns,
    bool remove_on_read, bool learner_read, bool resolve_locks, UInt64 start_ts)
{
    auto & kvstore = context.getTMTContext().kvstore;
    Regions partition_regions;
    {
        std::lock_guard<std::mutex> lock(mutex);

        auto & table = getOrCreateTable(table_id);
        auto & partition = table.partitions.get()[partition_id];

        for (auto & region_id : partition.region_ids)
        {
            auto region = kvstore->getRegion(region_id);
            if (!region)
                throw Exception("Region " + DB::toString(region_id) + " not found!", ErrorCodes::LOGICAL_ERROR);
            partition_regions.push_back(region);
        }
    }

    if (partition_regions.empty())
        return {};

    if (learner_read)
    {
        for (const auto & region : partition_regions)
        {
            region->wait_index(region->learner_read());
        }
    }

    auto schema_fetcher = [&](TableID) {
        return std::make_tuple<const TiDB::TableInfo *, const ColumnsDescription *, const Names *>(&table_info, &columns, &ordered_columns);
    };

    RegionBlockReader reader(schema_fetcher, std::move(partition_regions), table_id, remove_on_read, resolve_locks, start_ts);

    BlocksList blocks;
    Block block;
    TableID current_table_id;
    RegionID current_region_id;
    Region::LockInfoPtr lock_info;
    Region::LockInfos lock_infos;
    while (true)
    {
        std::tie(block, current_table_id, current_region_id, lock_info) = reader.next();
        if (lock_info)
        {
            lock_infos.emplace_back(std::move(lock_info));
            continue;
        }
        if (!block || block.rows() == 0)
            break;
        if (table_id != current_table_id)
            throw Exception("RegionPartition::getBlockInputStreamByPartition", ErrorCodes::LOGICAL_ERROR);
        blocks.push_back(block);
    }

    if (!lock_infos.empty())
        throw LockException(std::move(lock_infos));

    if (blocks.empty())
        return {};

    return std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
}


} // namespace DB