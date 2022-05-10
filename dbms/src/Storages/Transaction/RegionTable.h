// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/nocopyable.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionDataRead.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/RegionLockInfo.h>
#include <Storages/Transaction/TiKVHandle.h>
#include <common/logger_useful.h>

#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <variant>
#include <vector>

namespace TiDB
{
struct TableInfo;
};

namespace DB
{
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
class RegionRangeKeys;
class RegionTaskLock;
struct RegionPtrWithBlock;
struct RegionPtrWithSnapshotFiles;
class RegionScanFilter;
using RegionScanFilterPtr = std::shared_ptr<RegionScanFilter>;

class RegionTable : private boost::noncopyable
{
public:
    struct InternalRegion
    {
        InternalRegion(const RegionID region_id_, const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & range_in_table_)
            : region_id(region_id_)
            , range_in_table(range_in_table_)
        {}

        RegionID region_id;
        std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> range_in_table;
        bool pause_flush = false;
        Int64 cache_bytes = 0;
        Timepoint last_flush_time = Clock::now();
    };

    using InternalRegions = std::unordered_map<RegionID, InternalRegion>;

    struct Table : boost::noncopyable
    {
        Table(const TableID table_id_)
            : table_id(table_id_)
        {}
        TableID table_id;
        InternalRegions regions;
    };

    using TableMap = std::unordered_map<TableID, Table>;
    using RegionInfoMap = std::unordered_map<RegionID, TableID>;

    using DirtyRegions = std::unordered_set<RegionID>;
    using TableToOptimize = std::unordered_set<TableID>;

    struct FlushThresholds
    {
        using FlushThresholdsData = std::vector<std::pair<Int64, Seconds>>;

        FlushThresholds(FlushThresholdsData && data_)
            : data(std::make_shared<FlushThresholdsData>(std::move(data_)))
        {}

        void setFlushThresholds(const FlushThresholdsData & flush_thresholds_)
        {
            auto flush_thresholds = std::make_shared<FlushThresholdsData>(flush_thresholds_);
            {
                std::lock_guard lock(mutex);
                data = std::move(flush_thresholds);
            }
        }

        auto getData() const
        {
            std::lock_guard lock(mutex);
            return data;
        }

    private:
        std::shared_ptr<const FlushThresholdsData> data;
        mutable std::mutex mutex;
    };

    RegionTable(Context & context_);
    void restore();

    void setFlushThresholds(const FlushThresholds::FlushThresholdsData & flush_thresholds_);

    void updateRegion(const Region & region);

    /// This functional only shrink the table range of this region_id
    void shrinkRegionRange(const Region & region);

    void removeRegion(const RegionID region_id, bool remove_data, const RegionTaskLock &);

    bool tryFlushRegions();
    RegionDataReadInfoList tryFlushRegion(RegionID region_id, bool try_persist = false);
    RegionDataReadInfoList tryFlushRegion(const RegionPtrWithBlock & region, bool try_persist);

    void handleInternalRegionsByTable(const TableID table_id, std::function<void(const InternalRegions &)> && callback) const;
    std::vector<std::pair<RegionID, RegionPtr>> getRegionsByTable(const TableID table_id) const;

    /// Write the data of the given region into the table with the given table ID, fill the data list for outer to remove.
    /// Will trigger schema sync on read error for only once,
    /// assuming that newer schema can always apply to older data by setting force_decode to true in RegionBlockReader::read.
    /// Note that table schema must be keep unchanged throughout the process of read then write, we take good care of the lock.
    static void writeBlockByRegion(Context & context,
                                   const RegionPtrWithBlock & region,
                                   RegionDataReadInfoList & data_list_to_remove,
                                   Poco::Logger * log,
                                   bool lock_region = true);

    /// Read the data of the given region into block, take good care of learner read and locks.
    /// Assuming that the schema has been properly synced by outer, i.e. being new enough to decode data before start_ts,
    /// we directly ask RegionBlockReader::read to perform a read with the given start_ts and force_decode being true.
    using ReadBlockByRegionRes = std::variant<Block, RegionException::RegionReadStatus>;
    static ReadBlockByRegionRes readBlockByRegion(const TiDB::TableInfo & table_info,
                                                  const ColumnsDescription & columns,
                                                  const Names & column_names_to_read,
                                                  const RegionPtr & region,
                                                  RegionVersion region_version,
                                                  RegionVersion conf_version,
                                                  bool resolve_locks,
                                                  Timestamp start_ts,
                                                  const std::unordered_set<UInt64> * bypass_lock_ts,
                                                  RegionScanFilterPtr scan_filter = nullptr);

    /// Check transaction locks in region, and write committed data in it into storage engine if check passed. Otherwise throw an LockException.
    /// The write logic is the same as #writeBlockByRegion, with some extra checks about region version and conf_version.
    using ResolveLocksAndWriteRegionRes = std::variant<LockInfoPtr, RegionException::RegionReadStatus>;
    static ResolveLocksAndWriteRegionRes resolveLocksAndWriteRegion(TMTContext & tmt,
                                                                    const TiDB::TableID table_id,
                                                                    const RegionPtr & region,
                                                                    const Timestamp start_ts,
                                                                    const std::unordered_set<UInt64> * bypass_lock_ts,
                                                                    RegionVersion region_version,
                                                                    RegionVersion conf_version,
                                                                    Poco::Logger * log);

    /// extend range for possible InternalRegion or add one.
    void extendRegionRange(const RegionID region_id, const RegionRangeKeys & region_range_keys);

private:
    friend class MockTiDB;
    friend class StorageDeltaMerge;

    Table & getOrCreateTable(const TableID table_id);
    void removeTable(TableID table_id);
    InternalRegion & insertRegion(Table & table, const Region & region);
    InternalRegion & getOrInsertRegion(const Region & region);
    InternalRegion & insertRegion(Table & table, const RegionRangeKeys & region_range_keys, const RegionID region_id);
    InternalRegion & doGetInternalRegion(TableID table_id, RegionID region_id);

    RegionDataReadInfoList flushRegion(const RegionPtrWithBlock & region, bool try_persist) const;
    bool shouldFlush(const InternalRegion & region) const;
    RegionID pickRegionToFlush();

private:
    TableMap tables;
    RegionInfoMap regions;
    DirtyRegions dirty_regions;

    FlushThresholds flush_thresholds;

    Context * const context;

    mutable std::mutex mutex;

    Poco::Logger * log;
};


// Block cache of region data with schema version.
struct RegionPreDecodeBlockData
{
    Block block;
    Int64 schema_version;
    RegionDataReadInfoList data_list_read; // if schema version changed, use kv data to rebuild block cache

    RegionPreDecodeBlockData(Block && block_, Int64 schema_version_, RegionDataReadInfoList && data_list_read_)
        : block(std::move(block_))
        , schema_version(schema_version_)
        , data_list_read(std::move(data_list_read_))
    {}
    DISALLOW_COPY(RegionPreDecodeBlockData);
    void toString(std::stringstream & ss) const
    {
        ss << " {";
        ss << " schema_version: " << schema_version;
        ss << ", data_list size: " << data_list_read.size();
        ss << ", block row: " << block.rows() << " col: " << block.columns() << " bytes: " << block.bytes();
        ss << " }";
    }
};

// A wrap of RegionPtr, could try to use its block cache while writing region data to storage.
struct RegionPtrWithBlock
{
    using Base = RegionPtr;
    using CachePtr = std::unique_ptr<RegionPreDecodeBlockData>;

    /// can accept const ref of RegionPtr without cache
    RegionPtrWithBlock(const Base & base_, CachePtr cache = nullptr)
        : base(base_)
        , pre_decode_cache(std::move(cache))
    {}

    /// to be compatible with usage as RegionPtr.
    Base::element_type * operator->() const { return base.operator->(); }
    const Base::element_type & operator*() const { return base.operator*(); }

    /// make it could be cast into RegionPtr implicitly.
    operator const Base &() const { return base; }

    const Base & base;
    CachePtr pre_decode_cache;
};


// A wrap of RegionPtr, with snapshot files directory waitting to be ingested
struct RegionPtrWithSnapshotFiles
{
    using Base = RegionPtr;

    /// can accept const ref of RegionPtr without cache
    RegionPtrWithSnapshotFiles(const Base & base_, std::vector<UInt64> ids_ = {})
        : base(base_)
        , ingest_ids(std::move(ids_))
    {}

    /// to be compatible with usage as RegionPtr.
    Base::element_type * operator->() const { return base.operator->(); }
    const Base::element_type & operator*() const { return base.operator*(); }

    /// make it could be cast into RegionPtr implicitly.
    operator const Base &() const { return base; }

    const Base & base;
    const std::vector<UInt64> ingest_ids;
};

} // namespace DB
