#include <Raft/RaftService.h>

#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/Transaction/PartitionDataMover.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionPartition.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

// =============================================================
// Static methods.
// =============================================================

auto getRegionTableIds(const RegionPtr & region)
{
    std::unordered_set<TableID> table_ids;
    {
        auto scanner = region->createCommittedScanRemover(InvalidTableID);
        while (true)
        {
            TableID table_id = scanner->hasNext();
            if (table_id == InvalidTableID)
                break;
            table_ids.emplace(table_id);
            scanner->next();
        }
    }
    return table_ids;
}

Int64 calculatePartitionCacheBytes(KVStore & kvstore, const std::set<RegionID> & region_ids)
{
    Int64 bytes = 0;
    for (auto region_id : region_ids)
    {
        bytes += kvstore.getRegion(region_id)->dataSize();
    }
    return bytes;
}

// =============================================================
// Private member functions.
// =============================================================

RegionPartition::Table & RegionPartition::getOrCreateTable(TableID table_id)
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

size_t RegionPartition::selectPartitionId(Table & table, RegionID region_id)
{
    // size_t partition_id = rng() % table.partitions.get().size();
    size_t partition_id = (next_partition_id++) % table.partitions.get().size();
    LOG_DEBUG(log, "Table " << table.table_id << " assign region " << region_id << " to partition " << partition_id);
    return partition_id;
}

std::pair<PartitionID, RegionPartition::Partition &> RegionPartition::insertRegion(Table & table, size_t partition_id, RegionID region_id)
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

    return {partition_id, table_partitions[partition_id]};
}

std::pair<PartitionID, RegionPartition::Partition &> RegionPartition::getOrInsertRegion(TableID table_id, RegionID region_id)
{
    auto & table = getOrCreateTable(table_id);
    auto & table_partitions = table.partitions.get();

    for (size_t partition_index = 0; partition_index < table_partitions.size(); ++partition_index)
    {
        const auto & partition = table_partitions[partition_index];
        if (partition.region_ids.find(region_id) != partition.region_ids.end())
            return {partition_index, table_partitions[partition_index]};
    }

    // Currently region_id does not exist in any regions in this table, let's insert into one.
    size_t partition_id = selectPartitionId(table, region_id);
    return insertRegion(table, partition_id, region_id);
}

void RegionPartition::updateRegionRange(const RegionPtr & region)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto region_id = region->id();
    const auto range = region->getRange();

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
        if (TiKVRange::checkTableInvolveRange(table_id, range))
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

bool RegionPartition::shouldFlush(const Partition & partition)
{
    if (partition.pause_flush)
        return false;
    if (partition.must_flush)
        return true;
    if (!partition.updated || !partition.cache_bytes)
        return false;
    auto period_time = Clock::now() - partition.last_flush_time;
    for (auto && [th_bytes, th_duration] : flush_thresholds)
    {
        if (partition.cache_bytes >= th_bytes && period_time >= th_duration)
            return true;
    }
    return false;
}

void RegionPartition::flushPartition(TableID table_id, PartitionID partition_id)
{
    if (log->debug())
    {
        auto & table = getOrCreateTable(table_id);
        auto & partition = table.partitions.get()[partition_id];
        std::string region_ids;
        for (auto id : partition.region_ids)
            region_ids += DB::toString(id) + ",";
        if (!region_ids.empty())
            region_ids.pop_back();
        LOG_DEBUG(log,
            "Flush regions - table_id: " + DB::toString(table_id) + ", partition_id: " + DB::toString(partition_id) + ", ~ "
                + DB::toString(partition.cache_bytes) + " bytes, containing region_ids: " + region_ids);
    }

    TMTContext & tmt = context.getTMTContext();
    tmt.getSchemaSyncer()->syncSchema(table_id, context);

    StoragePtr storage = tmt.storages.get(table_id);

    // TODO: handle if storage is nullptr
    // drop table and create another with same name, but the previous one will still flush
    if (storage == nullptr)
    {

        LOG_ERROR(log, "table " << table_id << " flush partition " << partition_id << " , but storage is not found");
        return;
    }

    auto * merge_tree = dynamic_cast<StorageMergeTree *>(storage.get());

    const auto & table_info = merge_tree->getTableInfo();
    const auto & columns = merge_tree->getColumns();
    // TODO: confirm names is right
    Names names = columns.getNamesOfPhysical();

    BlockInputStreamPtr input = getBlockInputStreamByPartition(table_id, partition_id, table_info, columns, names, true, false, false, 0);
    if (!input)
        return;

    TxnMergeTreeBlockOutputStream output(*merge_tree, partition_id);
    input->readPrefix();
    output.writePrefix();
    while (true)
    {
        Block block = input->read();
        if (!block || block.rows() == 0)
            break;
        output.write(block);
    }
    output.writeSuffix();
    input->readSuffix();
}

// =============================================================
// Public member functions.
// =============================================================

static const Int64 FTH_BYTES_1 = 1024;             // 1 KB
static const Int64 FTH_BYTES_2 = 1024 * 1024;      // 1 MB
static const Int64 FTH_BYTES_3 = 1024 * 1024 * 10; // 10 MBs
static const Int64 FTH_BYTES_4 = 1024 * 1024 * 50; // 50 MBs

static const Seconds FTH_PERIOD_1(60 * 60); // 1 hour
static const Seconds FTH_PERIOD_2(60 * 5);  // 5 minutes
static const Seconds FTH_PERIOD_3(60);      // 1 minute
static const Seconds FTH_PERIOD_4(5);       // 5 seconds

RegionPartition::RegionPartition(Context & context_, const std::string & parent_path_, std::function<RegionPtr(RegionID)> region_fetcher)
    : parent_path(parent_path_),
      flush_thresholds{
          {FTH_BYTES_1, FTH_PERIOD_1},
          {FTH_BYTES_2, FTH_PERIOD_2},
          {FTH_BYTES_3, FTH_PERIOD_3},
          {FTH_BYTES_4, FTH_PERIOD_4},
      },
      context(context_),
      log(&Logger::get("RegionPartition"))
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
                // Update cache infos
                auto region_ptr = region_fetcher(region_id);
                if (!region_ptr)
                    throw Exception("Region with id " + DB::toString(region_id) + " not found", ErrorCodes::LOGICAL_ERROR);
                partition.cache_bytes += region_ptr->dataSize();

                // Update region_id -> table_id & partition_id
                auto it = regions.find(region_id);
                if (it == regions.end())
                    std::tie(it, std::ignore) = regions.try_emplace(region_id);
                RegionInfo & region_info = it->second;
                region_info.table_to_partition.emplace(table_id, partition_id);
            }
        }
    }
}

void RegionPartition::updateRegion(const RegionPtr & region, size_t before_cache_bytes, TableIDSet relative_table_ids)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto region_id = region->id();
    Int64 delta = region->dataSize() - before_cache_bytes;
    for (auto table_id : relative_table_ids)
    {
        auto & partition = getOrInsertRegion(table_id, region_id).second;
        partition.updated = true;
        partition.cache_bytes += delta;
    }
}

void RegionPartition::applySnapshotRegion(const RegionPtr & region)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto region_id = region->id();
    auto table_ids = getRegionTableIds(region);
    for (auto table_id : table_ids)
    {
        auto & partition = getOrInsertRegion(table_id, region_id).second;
        partition.must_flush = true;
        partition.cache_bytes += region->dataSize();
    }
}

void RegionPartition::splitRegion(const RegionPtr & region, std::vector<RegionPtr> split_regions)
{
    struct MoveAction
    {
        StorageMergeTree * storage;
        PartitionID current_partition_id;
        PartitionID new_partition_id;
        Field start;
        Field end;
    };
    std::vector<MoveAction> move_actions;

    {
        std::lock_guard<std::mutex> lock(mutex);

        auto region_id = region->id();
        auto it = regions.find(region_id);

        if (it == regions.end())
        {
            // If region doesn't exist, usually means it does not contain any data we interested. Just ignore it.
            return;
        }

        RegionInfo & region_info = it->second;
        auto & tmt_ctx = context.getTMTContext();
        for (auto [table_id, current_partition_id] : region_info.table_to_partition)
        {
            auto storage = tmt_ctx.storages.get(table_id);
            if (storage == nullptr)
            {
                throw Exception("Table " + DB::toString(table_id) + " not found", ErrorCodes::UNKNOWN_TABLE);
            }

            auto * merge_tree_storage = dynamic_cast<StorageMergeTree *>(storage.get());

            auto & table = getOrCreateTable(table_id);
            auto & table_partitions = table.partitions.get();
            // Tables which have only one partition don't need to move data.
            if (table_partitions.size() == 1)
                continue;

            for (const RegionPtr & split_region : split_regions)
            {
                const auto range = split_region->getRange();
                // This region definitely does not contain any data in this table.
                if (!TiKVRange::checkTableInvolveRange(table_id, range))
                    continue;

                // Select another partition other than current_partition_id;
                auto split_region_id = split_region->id();
                size_t new_partition_id;
                while ((new_partition_id = selectPartitionId(table, split_region_id)) == current_partition_id) {}

                auto [start_field, end_field] = getRegionRangeField(range.first, range.second, table_id);
                move_actions.push_back({merge_tree_storage, current_partition_id, new_partition_id, start_field, end_field});

                auto & partition = insertRegion(table, new_partition_id, split_region_id).second;
                // Mark flush flag.
                partition.must_flush = true;
            }
        }

        updateRegionRange(region);
    }

    // FIXME: move data should be locked for safety, and do it aysnchronized.
    for (const auto & action : move_actions)
    {
        moveRangeBetweenPartitions(context, action.storage, action.current_partition_id, action.new_partition_id, action.start, action.end);
    }
}

void RegionPartition::removeRegion(const RegionPtr & region)
{
    std::unordered_map<TableID, PartitionID> table_partitions;
    auto region_id = region->id();
    auto region_cache_bytes = region->dataSize();
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
            auto & table = getOrCreateTable(table_id);
            Partition & partition = table.partitions.get().at(partition_id);
            partition.cache_bytes -= region_cache_bytes;
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

bool RegionPartition::tryFlushRegions()
{
    KVStore & kvstore = *context.getTMTContext().kvstore;
    std::set<std::pair<TableID, PartitionID>> to_flush;
    {
        traversePartitions([&](TableID table_id, PartitionID partition_id, Partition & partition) {
            if (shouldFlush(partition))
            {
                to_flush.emplace(table_id, partition_id);
                // Stop other flush threads.
                partition.pause_flush = true;
            }
        });
    }

    for (auto [table_id, partition_id] : to_flush)
    {
        flushPartition(table_id, partition_id);
    }

    {
        // Now reset status infomations.
        Timepoint now = Clock::now();
        traversePartitions([&](TableID table_id, PartitionID partition_id, Partition & partition) {
            if (to_flush.count({table_id, partition_id}))
            {
                partition.pause_flush = false;
                partition.must_flush = false;
                partition.updated = false;
                partition.cache_bytes = calculatePartitionCacheBytes(kvstore, partition.region_ids);
                partition.last_flush_time = now;
            }
        });
    }

    return !to_flush.empty();
}

void RegionPartition::traversePartitions(std::function<void(TableID, PartitionID, Partition &)> callback)
{
    std::lock_guard<std::mutex> lock(mutex);
    for (auto && [table_id, table] : tables)
    {
        size_t id = 0;
        for (auto & partition : table.partitions.get())
        {
            callback(table_id, id, partition);
            ++id;
        }
    }
}

void RegionPartition::traverseRegionsByTablePartition(
    const TableID table_id, const PartitionID partition_id, std::function<void(Regions)> callback)
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
    callback(partition_regions);
}

void RegionPartition::dumpRegionMap(RegionPartition::RegionMap & res)
{
    std::lock_guard<std::mutex> lock(mutex);
    res = regions;
}

void RegionPartition::dropRegionsInTable(TableID /*table_id*/)
{
    // TODO: impl
}

} // namespace DB
