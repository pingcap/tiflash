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

class RegionPartition : private boost::noncopyable
{
public:
    struct InternalRegion
    {
        InternalRegion() {}
        InternalRegion(const InternalRegion & p) : region_id(p.region_id) {}
        InternalRegion(RegionID region_id_) : region_id(region_id_) {}

        RegionID region_id;
        bool pause_flush = false;
        bool must_flush = false;
        bool updated = false;
        Int64 cache_bytes = 0;
        Timepoint last_flush_time = Clock::now();
    };

    struct Table
    {
        Table(const std::string & parent_path, TableID table_id_) : table_id(table_id_), regions(parent_path + DB::toString(table_id))
        {
            regions.restore();
        }

        struct Write
        {
            void operator()(const RegionID k, const InternalRegion & v, DB::WriteBuffer & buf)
            {
                std::ignore = v;
                writeIntBinary(k, buf);
            }
        };

        struct Read
        {
            std::pair<RegionID, InternalRegion> operator()(DB::ReadBuffer & buf)
            {
                RegionID region_id;
                readIntBinary(region_id, buf);
                return {region_id, InternalRegion(region_id)};
            }
        };

        using InternalRegions = PersistedContainerMap<RegionID, InternalRegion, std::unordered_map, Write, Read>;

        TableID table_id;
        InternalRegions regions;
    };

    struct RegionInfo
    {
        std::unordered_set<TableID> tables;
    };

    enum RegionReadStatus: UInt8
    {
        OK,
        NOT_FOUND,
        VERSION_ERROR,
    };

    static const String RegionReadStatusString(RegionReadStatus s)
    {
        switch(s)
        {
            case OK: return "OK";
            case NOT_FOUND: return "NOT_FOUND";
            case VERSION_ERROR: return "VERSION_ERROR";
        }
        return "Unknown";
    };

    using TableMap = std::unordered_map<TableID, Table>;
    using RegionMap = std::unordered_map<RegionID, RegionInfo>;
    using FlushThresholds = std::vector<std::pair<Int64, Seconds>>;

private:
    const std::string parent_path;

    TableMap tables;
    RegionMap regions;

    FlushThresholds flush_thresholds;

    Context & context;

    mutable std::mutex mutex;
    Logger * log;

private:
    Table & getOrCreateTable(TableID table_id);
    InternalRegion & insertRegion(Table & table, RegionID region_id);
    InternalRegion & getOrInsertRegion(TableID table_id, RegionID region_id);

    /// This functional only shrink the table range of this region_id, range expand will (only) be done at flush.
    /// Note that region update range should not affect the data in storage.
    void updateRegionRange(const RegionPtr & region);

    bool shouldFlush(const InternalRegion & region);
    void flushRegion(TableID table_id, RegionID partition_id);

public:
    RegionPartition(Context & context_, const std::string & parent_path_, std::function<RegionPtr(RegionID)> region_fetcher);
    void setFlushThresholds(FlushThresholds flush_thresholds_) { flush_thresholds = std::move(flush_thresholds_); }

    /// After the region is updated (insert or delete KVs).
    void updateRegion(const RegionPtr & region, const TableIDSet & relative_table_ids);
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

    void traverseRegions(std::function<void(TableID, InternalRegion & )> callback);
    void traverseRegionsByTable(const TableID table_id, std::function<void(Regions)> callback);

    std::tuple<BlockInputStreamPtr, RegionReadStatus, size_t> getBlockInputStreamByRegion(
        TableID table_id,
        const RegionID region_id,
        const RegionVersion region_version,
        const TiDB::TableInfo & table_info,
        const ColumnsDescription & columns,
        const Names & ordered_columns,
        bool learner_read,
        bool resolve_locks,
        UInt64 start_ts,
        std::vector<TiKVKey> * keys=nullptr);

    // For debug
    void dumpRegionMap(RegionPartition::RegionMap & res);
    void dropRegionsInTable(TableID table_id);
};

using RegionPartitionPtr = std::shared_ptr<RegionPartition>;

} // namespace DB
