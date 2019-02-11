#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionPartition.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

#include <Raft/RaftService.h>

#include <DataStreams/BlocksListBlockInputStream.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/PartitionDataMover.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

RegionPartition::RegionPartition(const std::string & parent_path_) : parent_path(parent_path_), log(&Logger::get("RegionPartition"))
{
    Poco::File dir(parent_path + "tables/");
    if (!dir.exists())
        dir.createDirectories();

    std::vector<std::string> file_names;
    dir.list(file_names);

    // Restore all table mappings and region mappings.
    for (auto & name : file_names)
    {
        TableID table_id = std::stoull(name);
        auto p = tables.try_emplace(table_id, parent_path + "tables/", table_id);
        Table & table = p.first->second;
        for (PartitionID partition_id = 0; partition_id < table.partitions.get().size(); ++partition_id)
        {
            auto & partition = table.partitions.get()[partition_id];
            for (auto region_id : partition.region_ids)
            {
                auto it = regions.find(region_id);
                if (it == regions.end())
                    std::tie(it, std::ignore) = regions.try_emplace(region_id);
                RegionInfo & region_info = it->second;
                region_info.table_to_partition.emplace(table_id, partition_id);
            }
        }
    }
}

void RegionPartition::updateRegionRange(const RegionPtr & region)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto region_id = region->id();
    auto [start_key, end_key] = region->getRange();
    TableID start_table_id = start_key.empty() ? 0 : RecordKVFormat::getTableId(start_key);
    TableID end_table_id = end_key.empty() ? std::numeric_limits<TableID>::max() : RecordKVFormat::getTableId(end_key);

    auto it = regions.find(region_id);
    // if this region does not exist already, then nothing to shrink.
    if (it == regions.end())
        return;

    RegionInfo & region_info = it->second;
    auto t_it = region_info.table_to_partition.begin();
    while (t_it != region_info.table_to_partition.end())
    {
        auto table_id = t_it->first;
        auto partition_id = t_it->second;
        if (start_table_id <= table_id && table_id <= end_table_id)
        {
            ++t_it;
            continue;
        }

        // remove from table mapping
        auto table_it = tables.find(table_id);
        if (table_it == tables.end())
        {
            throw Exception("Table " + DB::toString(table_id) + " not found in table map", ErrorCodes::LOGICAL_ERROR);
        }

        Table & table = table_it->second;
        auto & partition = table.partitions.get().at(partition_id);
        partition.region_ids.erase(region_id);
        table.partitions.persist();

        // remove from region mapping
        t_it = region_info.table_to_partition.erase(t_it);
    }
}

void RegionPartition::insertRegion(Table & table, size_t partition_id, RegionID region_id)
{
    auto & table_partitions = table.partitions.get();
    // Insert table mapping.
    table_partitions[partition_id].region_ids.emplace(region_id);
    table.partitions.persist();

    // Insert region mapping.
    auto r_it = regions.find(region_id);
    if (r_it == regions.end())
        std::tie(r_it, std::ignore) = regions.try_emplace(region_id);
    RegionInfo & region_info = r_it->second;
    region_info.table_to_partition.emplace(table.table_id, partition_id);
}

UInt64 RegionPartition::getOrInsertRegion(TableID table_id, RegionID region_id, Context & context)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto & table = getTable(table_id, context);
    auto & table_partitions = table.partitions.get();

    for (size_t partition_index = 0; partition_index < table_partitions.size(); ++partition_index)
    {
        const auto & partition = table_partitions[partition_index];
        if (partition.region_ids.find(region_id) != partition.region_ids.end())
            return partition_index;
    }

    // Currently region_id does not exist in any regions in this table, let's insert into one.
    size_t partition_id = selectPartitionId(table, region_id);
    insertRegion(table, partition_id, region_id);
    return partition_id;
}

void RegionPartition::removeRegion(const RegionPtr & region, Context & context)
{
    std::unordered_map<TableID, PartitionID> table_partitions;
    auto region_id = region->id();
    {
        std::lock_guard<std::mutex> lock(mutex);

        auto r_it = regions.find(region_id);
        if (r_it == regions.end())
        {
            LOG_WARNING(log, "Being removed region " << region_id << " does not exist.");
            return;
        }
        RegionInfo & region_info = r_it->second;
        table_partitions.swap(region_info.table_to_partition);

        regions.erase(region_id);

        for (auto [table_id, partition_id] : table_partitions)
        {
            auto & table = getTable(table_id, context);
            Partition & partition = table.partitions.get().at(partition_id);
            partition.region_ids.erase(region_id);
            table.partitions.persist();
        }
    }

    // Note that we cannot use lock to protect following code, as deleteRangeInPartition will result in
    // calling getBlockInputStreamByPartition and lead to dead lock.
    auto & tmt_ctx = context.getTMTContext();
    for (auto [table_id, partition_id] : table_partitions)
    {
        auto storage = tmt_ctx.storages.get(table_id);
        if (storage == nullptr)
        {
            LOG_WARNING(log, "RegionPartition::removeRegion: " << table_id << " does not exist.");
            continue;
        }
        auto * merge_tree = dynamic_cast<StorageMergeTree *>(storage.get());
        auto [start_key, end_key] = region->getRange();
        auto [start_field, end_field] = getRegionRangeField(start_key, end_key, table_id);
        deleteRangeInPartition(context, merge_tree, partition_id, start_field, end_field);
    }
}

void learner_read_fn(RegionPtr region) {
    Int64 idx = region->learner_read();
    region->wait_index(idx);
}

BlockInputStreamPtr RegionPartition::getBlockInputStreamByPartition(TableID table_id, UInt64 partition_id,
    const TiDB::TableInfo & table_info, const ColumnsDescription & columns, const Names & ordered_columns, Context & context,
    bool remove_on_read,
    bool learner_read,
    bool resolve_locks,
    UInt64 start_ts)
{
    auto & kvstore = context.getTMTContext().kvstore;
    Regions partition_regions;
    {
        std::lock_guard<std::mutex> lock(mutex);

        auto & table = getTable(table_id, context);
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

    if (learner_read) {
        std::vector<std::thread> learner_threads;
        for (auto region: partition_regions) {
            learner_threads.push_back(std::thread(learner_read_fn, region));
        }
        for (auto & thread: learner_threads) {
            thread.join();
        }
    }

    auto schema_fetcher = [&](TableID) {
        // TODO: We may should clone all this vars, to avoid out of live time in multi thread environment
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

void RegionPartition::dropRegionsInTable(TableID /*table_id*/)
{
    // TODO: impl
}

void RegionPartition::traverseTablesOfRegion(RegionID region_id, std::function<void(TableID)> handler)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto it = regions.find(region_id);
    if (it == regions.end())
        return;
    RegionInfo & region_info = it->second;
    for (auto p : region_info.table_to_partition)
        handler(p.first);
}

void RegionPartition::traverseRegionsByTablePartition(const TableID table_id, const PartitionID partition_id,
                                                      Context& context, std::function<void(Regions)> callback)
{
    auto & kvstore = context.getTMTContext().kvstore;
    Regions partition_regions;
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto & table = getTable(table_id, context);
        auto & partition = table.partitions.get()[partition_id];

        for (auto & region_id : partition.region_ids)
        {
            auto region = kvstore->getRegion(region_id);
            if (!region)
                throw Exception("Region " + DB::toString(region_id) + " not found!", ErrorCodes::LOGICAL_ERROR);
            partition_regions.push_back(region);
        }
    }
    callback(partition_regions);
}

void RegionPartition::splitRegion(const RegionPtr & region, std::vector<RegionPtr> split_regions, Context & context)
{
    auto region_id = region->id();
    auto it = regions.find(region_id);

    // We cannot find this region in mapping, it means we have never flushed this region before.
    // It is a good news as we don't need to handle data in table partition.
    if (it == regions.end())
        return;

    RegionInfo & region_info = it->second;
    auto & tmt_ctx = context.getTMTContext();
    for (auto [table_id, current_partition_id] : region_info.table_to_partition)
    {

        // TODO Maybe we should lock current table here, but it will cause deadlock.
        // So I decide to leave it here until we implement atomic range data move.

        auto storage = tmt_ctx.storages.get(table_id);
        if (storage == nullptr)
        {
            LOG_WARNING(log, "RegionPartition::splitRegion: " << table_id << " does not exist.");
            continue;
        }

        auto * merge_tree = dynamic_cast<StorageMergeTree *>(storage.get());

        auto & table = getTable(table_id, context);
        auto & table_partitions = table.partitions.get();
        // Tables which have only one partition don't need to move data.
        if (table_partitions.size() == 1)
            continue;

        for (const RegionPtr & split_region : split_regions)
        {
            auto [start_key, end_key] = split_region->getRange();
            TableID start_table_id = start_key.empty() ? 0 : RecordKVFormat::getTableId(start_key);
            TableID end_table_id = end_key.empty() ? std::numeric_limits<TableID>::max() : RecordKVFormat::getTableId(end_key);

            // This region definitely does not contain any data in this table.
            if (start_table_id > table_id || end_table_id < table_id)
                continue;

            // Select another partition other than current_partition_id;
            auto split_region_id = split_region->id();
            size_t new_partition_id;
            while ((new_partition_id = selectPartitionId(table, split_region_id)) == current_partition_id) {}

            auto [start_field, end_field] = getRegionRangeField(start_key, end_key, table_id);
            moveRangeBetweenPartitions(context, merge_tree, current_partition_id, new_partition_id, start_field, end_field);

            insertRegion(table, new_partition_id, split_region_id);
        }
    }

    updateRegionRange(region);
}

RegionPartition::Table & RegionPartition::getTable(TableID table_id, Context & context)
{
    auto it = tables.find(table_id);
    if (it == tables.end())
    {
        // Load persisted info.
        auto & tmt_ctx = context.getTMTContext();
        auto storage = tmt_ctx.storages.get(table_id);
        if (!storage)
        {
            tmt_ctx.getSchemaSyncer()->syncSchema(table_id, context);
            storage = tmt_ctx.storages.get(table_id);
        }

        auto * merge_tree = dynamic_cast<StorageMergeTree *>(storage.get());
        auto partition_number = merge_tree->getData().settings.mutable_mergetree_partition_number;

        std::tie(it, std::ignore) = tables.try_emplace(table_id, parent_path + "tables/", table_id, partition_number);

        auto & table = it->second;
        table.partitions.persist();
    }
    return it->second;
}

void RegionPartition::dumpRegionMap(RegionPartition::RegionMap & res)
{
    std::lock_guard<std::mutex> lock(mutex);
    res = regions;
}

} // namespace DB
