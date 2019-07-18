#pragma once

#include <functional>
#include <optional>
#include <vector>

#include <Core/Names.h>
#include <Storages/Transaction/RegionDataRead.h>
#include <Storages/Transaction/TiKVHandle.h>
#include <common/logger_useful.h>

namespace TiDB
{
struct TableInfo;
};

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;
struct ColumnsDescription;
class Context;
class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;
class TMTContext;
class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
class Block;
// for debug
struct MockTiDBTable;
using RegionMap = std::unordered_map<RegionID, RegionPtr>;

class RegionTable : private boost::noncopyable
{
public:
    struct InternalRegion
    {
        InternalRegion(const InternalRegion & p) : region_id(p.region_id), range_in_table(p.range_in_table) {}
        InternalRegion(const RegionID region_id_, const HandleRange<HandleID> & range_in_table_ = {0, 0})
            : region_id(region_id_), range_in_table(range_in_table_)
        {}

        RegionID region_id;
        HandleRange<HandleID> range_in_table;
        bool pause_flush = false;
        bool must_flush = false;
        bool updated = false;
        Int64 cache_bytes = 0;
        Timepoint last_flush_time = Clock::now();
    };

    using InternalRegions = std::unordered_map<RegionID, InternalRegion>;

    struct Table
    {
        Table(const TableID table_id_) : table_id(table_id_) {}
        TableID table_id;
        InternalRegions regions;
    };

    enum RegionReadStatus : UInt8
    {
        OK,
        NOT_FOUND,
        VERSION_ERROR,
        PENDING_REMOVE,
    };

    static const char * RegionReadStatusString(RegionReadStatus s)
    {
        switch (s)
        {
            case OK:
                return "OK";
            case NOT_FOUND:
                return "NOT_FOUND";
            case VERSION_ERROR:
                return "VERSION_ERROR";
            case PENDING_REMOVE:
                return "PENDING_REMOVE";
        }
        return "Unknown";
    };

    using RegionInfo = std::unordered_set<TableID>;
    using TableMap = std::unordered_map<TableID, Table>;
    using RegionInfoMap = std::unordered_map<RegionID, RegionInfo>;

    struct FlushThresholds
    {
        using FlushThresholdsData = std::vector<std::pair<Int64, Seconds>>;

        FlushThresholdsData data;
        mutable std::mutex mutex;

        FlushThresholds(const FlushThresholdsData & data_) { data = data_; }
        FlushThresholds(FlushThresholdsData && data_) { data = std::move(data_); }

        void setFlushThresholds(const FlushThresholdsData & flush_thresholds_)
        {
            std::lock_guard<std::mutex> lock(mutex);
            data = flush_thresholds_;
        }

        const FlushThresholdsData & getData() const
        {
            std::lock_guard<std::mutex> lock(mutex);
            return data;
        }

        template <typename T>
        T traverse(std::function<T(const FlushThresholdsData & data)> && f) const
        {
            std::lock_guard<std::mutex> lock(mutex);
            return f(data);
        }
    };

private:
    const std::string parent_path;

    TableMap tables;
    RegionInfoMap regions;

    FlushThresholds flush_thresholds;

    Context & context;

    mutable std::mutex mutex;
    Logger * log;

private:
    Table & getOrCreateTable(const TableID table_id);

    InternalRegion & insertRegion(Table & table, const Region & region);
    InternalRegion & getOrInsertRegion(TableID table_id, const Region & region);

    bool shouldFlush(const InternalRegion & region) const;

    void flushRegion(TableID table_id, RegionID partition_id, size_t & cache_size, const bool try_persist = true);

    // For debug
    friend struct MockTiDBTable;

    void mockDropRegionsInTable(TableID table_id);
    void doShrinkRegionRange(const Region & region);

public:
    RegionTable(Context & context_, const std::string & parent_path_);
    void restore();

    void setFlushThresholds(const FlushThresholds::FlushThresholdsData & flush_thresholds_);

    /// Remove a table and associated regions.
    void removeTable(TableID table_id);

    /// After the region is updated (insert or delete KVs).
    void updateRegion(const Region & region, const TableIDSet & relative_table_ids);
    /// A new region arrived by apply snapshot command, this function store the region into selected partitions.
    void applySnapshotRegion(const Region & region);
    void applySnapshotRegions(const RegionMap & regions);

    void updateRegionForSplit(const Region & split_region, const RegionID source_region);

    /// This functional only shrink the table range of this region_id
    void shrinkRegionRange(const Region & region);

    void removeRegion(const RegionID region_id);

    /// Try pick some regions and flush.
    /// Note that flush is organized by partition. i.e. if a regions is selected to be flushed, all regions belong to its partition will also flushed.
    /// This function will be called constantly by background threads.
    /// Returns whether this function has done any meaningful job.
    bool tryFlushRegions();

    void tryFlushRegion(RegionID region_id);

    void traverseInternalRegions(std::function<void(TableID, InternalRegion &)> && callback);
    void traverseInternalRegionsByTable(const TableID table_id, std::function<void(const InternalRegion &)> && callback);
    std::vector<std::pair<RegionID, RegionPtr>> getRegionsByTable(const TableID table_id);

    /// Write the data of the given region into the table with the given table ID, fill the data list for outer to remove.
    /// Will trigger schema sync on read error for only once,
    /// assuming that newer schema can always apply to older data by setting force_decode to true in readRegionBlock.
    /// Note that table schema must be keep unchanged throughout the process of read then write, we take good care of the lock.
    static void writeBlockByRegion(Context & context, TableID table_id, RegionPtr region, RegionDataReadInfoList & data_list_for_remove);

    /// Read the data of the given region into block, take good care of learner read and locks.
    /// Assuming that the schema has been properly synced by outer, i.e. being new enough to decode data before start_ts,
    /// we directly ask readRegionBlock to perform a read with the given start_ts and force_decode being true.
    static std::tuple<std::optional<Block>, RegionReadStatus> getBlockByRegion(const TiDB::TableInfo & table_info,
        const ColumnsDescription & columns,
        const Names & column_names_to_read,
        const RegionPtr & region,
        RegionVersion region_version,
        RegionVersion conf_version,
        bool resolve_locks,
        Timestamp start_ts);

    TableIDSet getAllMappedTables(const RegionID region_id) const;
};

using RegionPartitionPtr = std::shared_ptr<RegionTable>;

} // namespace DB
