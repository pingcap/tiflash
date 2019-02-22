#pragma once

#include <functional>
#include <map>
#include <random>
#include <vector>

#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/Region.h>

#include <Common/PersistedContainer.h>
#include <Common/randomSeed.h>

namespace DB
{

/// Store and manage the mapping between regions and partitions.
///
/// A table is consisted by multiple partitions, and a partition is consisted by multiple regions.
/// A region could be contained by multiple tables. And in a table, one specific region can only exist in one partition.
/// Regions have [start_end, end_key) bound, which is used to determine which tables it could be contained.
///
/// Here is an example of storage range of region data:
/// |.......region 0.........|.......region 1.........|.......................region 2..........|
/// ^                        ^                        ^                   ^                     ^
/// |                        |                        |                   |                     |
///  ------part a1----------- --------par a2---------- ------par b1------- --------par c1-------
/// |                                                 |                   |                     |
///  ----------------------table a-------------------- -----table b------- --------table c------
///
class RegionPartition : private boost::noncopyable
{
public:
    struct Partition
    {
        using RegionIDSet = std::set<RegionID>;
        RegionIDSet region_ids;

        Partition() {}
        Partition(const Partition & p) : region_ids(p.region_ids) {}
        Partition(RegionIDSet && region_ids_) : region_ids(std::move(region_ids_)) {}

        struct Write
        {
            void operator()(const Partition & p, DB::WriteBuffer & buf)
            {
                writeIntBinary(p.region_ids.size(), buf);
                for (auto id : p.region_ids)
                {
                    writeIntBinary(id, buf);
                }
            }
        };

        struct Read
        {
            Partition operator()(DB::ReadBuffer & buf)
            {
                UInt64 size;
                readIntBinary(size, buf);
                RegionIDSet region_ids;
                for (size_t i = 0; i < size; ++i)
                {
                    RegionID id;
                    readIntBinary(id, buf);
                    region_ids.insert(id);
                }
                return {std::move(region_ids)};
            }
        };

        // Statistics to serve flush.
        // Note that it is not 100% accurate because one region could belongs to more than one partition by different tables.
        // And when a region is updated, we don't distinguish carefully which partition, simply update them all.
        // It is not a real issue as do flush on a partition is not harmful. Besides, the situation is very rare that a region belongs to many partitions.
        // Those members below are not persisted.

        bool pause_flush = false;
        bool must_flush = false;
        bool updated = false;
        Int64 cache_bytes = 0;
        Timepoint last_flush_time = Clock::now();
    };

    struct Table
    {
        Table(const std::string & parent_path, TableID table_id_, size_t partition_number)
            : table_id(table_id_), partitions(parent_path + DB::toString(table_id))
        {
            partitions.restore();

            if (partitions.get().empty())
            {
                std::vector<Partition> new_partitions(partition_number);
                partitions.get().swap(new_partitions);

                partitions.persist();
            }
        }

        Table(const std::string & parent_path, TableID table_id_) : table_id(table_id_), partitions(parent_path + DB::toString(table_id))
        {
            partitions.restore();
        }

        using Partitions = PersistedContainerVector<Partition, std::vector, Partition::Write, Partition::Read>;

        TableID table_id;
        Partitions partitions;
    };

    struct RegionInfo
    {
        std::unordered_map<TableID, PartitionID> table_to_partition;
    };

    using TableMap = std::unordered_map<TableID, Table>;
    using RegionMap = std::unordered_map<RegionID, RegionInfo>;
    using FlushThresholds = std::vector<std::pair<Int64, Seconds>>;

private:
    const std::string parent_path;

    TableMap tables;
    RegionMap regions;

    FlushThresholds flush_thresholds;

    // std::minstd_rand rng = std::minstd_rand(randomSeed());
    size_t next_partition_id = 0;

    Context & context;

    mutable std::mutex mutex;
    Logger * log;

private:
    Table & getOrCreateTable(TableID table_id);
    size_t selectPartitionId(Table & table, RegionID region_id);
    std::pair<PartitionID, Partition &> insertRegion(Table & table, size_t partition_id, RegionID region_id);
    std::pair<PartitionID, Partition &> getOrInsertRegion(TableID table_id, RegionID region_id);

    /// This functional only shrink the table range of this region_id, range expand will (only) be done at flush.
    /// Note that region update range should not affect the data in storage.
    void updateRegionRange(const RegionPtr & region);

    bool shouldFlush(const Partition & partition);
    void flushPartition(TableID table_id, PartitionID partition_id);

public:
    RegionPartition(Context & context_, const std::string & parent_path_, std::function<RegionPtr(RegionID)> region_fetcher);
    void setFlushThresholds(FlushThresholds flush_thresholds_) { flush_thresholds = std::move(flush_thresholds_); }

    /// After the region is updated (insert or delete KVs).
    void updateRegion(const RegionPtr & region, size_t before_cache_bytes, TableIDSet relative_table_ids);
    /// A new region arrived by apply snapshot command, this function store the region into selected partitions.
    void applySnapshotRegion(const RegionPtr & region);
    /// Manage data after region split into split_regions.
    /// i.e. split_regions could have assigned to another partitions, we need to move the data belong with them.
    void splitRegion(const RegionPtr & region, std::vector<RegionPtr> split_regions);
    /// Remove a region from corresponding partitions.
    void removeRegion(const RegionPtr & region);

    /// Try pick some regions and flush.
    /// Note that flush is organized by partition. i.e. if a regions is selected to be flushed, all regions belong to its partition will also flushed.
    /// This function will be called constantly by background threads.
    /// Returns whether this function has done any meaningful job.
    bool tryFlushRegions();

    void traversePartitions(std::function<void(TableID, PartitionID, Partition &)> callback);
    void traverseRegionsByTablePartition(const TableID table_id, const PartitionID partition_id, std::function<void(Regions)> callback);

    BlockInputStreamPtr getBlockInputStreamByPartition( //
        TableID table_id,
        UInt64 partition_id,
        const TiDB::TableInfo & table_info,
        const ColumnsDescription & columns,
        const Names & ordered_columns,
        bool remove_on_read,
        bool learner_read,
        bool resolve_locks,
        UInt64 start_ts);

    // For debug
    void dumpRegionMap(RegionPartition::RegionMap & res);
    void dropRegionsInTable(TableID table_id);
};

using RegionPartitionPtr = std::shared_ptr<RegionPartition>;

} // namespace DB
