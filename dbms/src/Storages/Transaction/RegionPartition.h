#pragma once

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
    RegionPartition(const std::string & parent_path_);

    /// Remove range data of splitted_regions, from corresponding partitions
    void splitRegion(const RegionPtr & region, std::vector<RegionPtr> split_regions, Context & context);

    /// Return partition_id. If region does not exist in this table, choose one partition to insert.
    UInt64 getOrInsertRegion(TableID table_id, RegionID region_id, Context & context);
    void removeRegion(const RegionPtr & region, Context & context);

    // TODO optimize the params.
    BlockInputStreamPtr getBlockInputStreamByPartition(TableID table_id,
                                                       UInt64 partition_id,
                                                       const TiDB::TableInfo & table_info,
                                                       const ColumnsDescription & columns,
                                                       const Names & ordered_columns,
                                                       Context & context,
                                                       bool remove_on_read,
                                                       bool learner_read,
                                                       bool resolve_locks,
                                                       UInt64 start_ts);

    /// This functional only shrink the table range of this region_id, range expand will (only) be done at flush.
    /// Note that region update range should not affect the data in storage.
    void updateRegionRange(const RegionPtr & region);

    void dropRegionsInTable(TableID table_id);

    void traverseTablesOfRegion(RegionID region_id, std::function<void(TableID)> callback);

    void traverseRegionsByTablePartition(const TableID table_id, const PartitionID partition_id, Context& context,
                                         std::function<void(Regions)> callback);

public:
    using RegionIDSet = std::set<RegionID>;

    struct Partition
    {
        RegionIDSet region_ids;

        struct Write
        {
            void operator()(Partition p, DB::WriteBuffer & buf)
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
            auto operator()(DB::ReadBuffer & buf)
            {
                UInt64 size;
                Partition p;
                readIntBinary(size, buf);
                for (size_t i = 0; i < size; ++i)
                {
                    RegionID id;
                    readIntBinary(id, buf);
                    p.region_ids.insert(id);
                }
                return p;
            }
        };
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

    Table & getTable(TableID table_id, Context & context);

    void insertRegion(Table & table, size_t partition_id, RegionID region_id);

    size_t selectPartitionId(Table & table, RegionID region_id)
    {
        // size_t partition_id = rng() % table.partitions.get().size();
        size_t partition_id = (next_partition_id ++) % table.partitions.get().size();
        LOG_DEBUG(log, "Table " << table.table_id << " assign region " << region_id << " to partition " << partition_id);
        return partition_id;
    }

    using TableMap = std::unordered_map<TableID, Table>;
    using RegionMap = std::unordered_map<RegionID, RegionInfo>;

    // For debug
    void dumpRegionMap(RegionPartition::RegionMap & res);

private:
    const std::string parent_path;

    // TODO: One container mutex + one mutext for per partition will be faster
    // Partition info persisting is slow, it's slow when all persistings are under one mutex
    TableMap tables;
    RegionMap regions;

    mutable std::mutex mutex;

    // TODO fix me: currently we use random to pick one partition here, may need to change that.
    // std::minstd_rand rng = std::minstd_rand(randomSeed());

    size_t next_partition_id = 0;

    Logger * log;
};

using RegionPartitionPtr = std::shared_ptr<RegionPartition>;

} // namespace DB
