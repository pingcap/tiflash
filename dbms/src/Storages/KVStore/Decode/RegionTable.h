// Copyright 2023 PingCAP, Inc.
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
#include <Interpreters/Context_fwd.h>
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/KVStore/Decode/RegionDataRead.h>
#include <Storages/KVStore/Decode/TiKVHandle.h>
#include <Storages/KVStore/Read/RegionException.h>
#include <Storages/KVStore/Read/RegionLockInfo.h>
#include <Storages/KVStore/Region.h>
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
struct MockRaftCommand;
struct ColumnsDescription;
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
struct CheckpointInfo;
using CheckpointInfoPtr = std::shared_ptr<CheckpointInfo>;
struct CheckpointIngestInfo;
using CheckpointIngestInfoPtr = std::shared_ptr<CheckpointIngestInfo>;

using SafeTS = UInt64;
enum : SafeTS
{
    InvalidSafeTS = std::numeric_limits<UInt64>::max(),
};

using TsoShiftBits = UInt64;
enum : TsoShiftBits
{
    TsoPhysicalShiftBits = 18,
};

class RegionTable : private boost::noncopyable
{
public:
    struct InternalRegion
    {
        InternalRegion(
            const RegionID region_id_,
            const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & range_in_table_)
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
        explicit Table(const TableID table_id_)
            : table_id(table_id_)
        {}
        TableID table_id;
        InternalRegions regions;
    };

    explicit RegionTable(Context & context_);
    void restore();

    void updateRegion(const Region & region);

    /// This functional only shrink the table range of this region_id
    void shrinkRegionRange(const Region & region);

    /// extend range for possible InternalRegion or add one.
    void extendRegionRange(RegionID region_id, const RegionRangeKeys & region_range_keys);

    void removeRegion(RegionID region_id, bool remove_data, const RegionTaskLock &);

    // Protects writeBlockByRegionAndFlush and ensures it's executed by only one thread at the same time.
    // Only one thread can do this at the same time.
    // The original name for this function is tryFlushRegion.
    RegionDataReadInfoList tryWriteBlockByRegion(const RegionPtrWithBlock & region);

    void handleInternalRegionsByTable(
        KeyspaceID keyspace_id,
        TableID table_id,
        std::function<void(const InternalRegions &)> && callback) const;

    std::vector<RegionID> getRegionIdsByTable(KeyspaceID keyspace_id, TableID table_id) const;
    std::vector<std::pair<RegionID, RegionPtr>> getRegionsByTable(KeyspaceID keyspace_id, TableID table_id) const;

    /// Write the data of the given region into the table with the given table ID, fill the data list for outer to remove.
    /// Will trigger schema sync on read error for only once,
    /// assuming that newer schema can always apply to older data by setting force_decode to true in RegionBlockReader::read.
    /// Note that table schema must be keep unchanged throughout the process of read then write, we take good care of the lock.
    static DM::WriteResult writeCommittedByRegion(
        Context & context,
        const RegionPtrWithBlock & region,
        RegionDataReadInfoList & data_list_to_remove,
        const LoggerPtr & log,
        bool lock_region = true);

    /// Check transaction locks in region, and write committed data in it into storage engine if check passed. Otherwise throw an LockException.
    /// The write logic is the same as #writeCommittedByRegion, with some extra checks about region version and conf_version.
    using ResolveLocksAndWriteRegionRes = std::variant<LockInfoPtr, RegionException::RegionReadStatus>;
    static ResolveLocksAndWriteRegionRes resolveLocksAndWriteRegion(
        TMTContext & tmt,
        const TableID table_id,
        const RegionPtr & region,
        const Timestamp start_ts,
        const std::unordered_set<UInt64> * bypass_lock_ts,
        RegionVersion region_version,
        RegionVersion conf_version,
        const LoggerPtr & log);

    void clear();

public:
    // safe ts is maintained by check_leader RPC (https://github.com/tikv/tikv/blob/1ea26a2ac8761af356cc5c0825eb89a0b8fc9749/components/resolved_ts/src/advance.rs#L262),
    // leader_safe_ts is the safe_ts in leader, leader will send <applied_index, safe_ts> to learner to advance safe_ts of learner, and TiFlash will record the safe_ts into safe_ts_map in check_leader RPC.
    // self_safe_ts is the safe_ts in TiFlah learner. When TiFlash proxy receive <applied_index, safe_ts> from leader, TiFlash will update safe_ts_map when TiFlash has applied the raft log to applied_index.
    struct SafeTsEntry
    {
        explicit SafeTsEntry(UInt64 leader_safe_ts, UInt64 self_safe_ts)
            : leader_safe_ts(leader_safe_ts)
            , self_safe_ts(self_safe_ts)
        {}
        std::atomic<UInt64> leader_safe_ts;
        std::atomic<UInt64> self_safe_ts;
    };
    using SafeTsEntryPtr = std::unique_ptr<SafeTsEntry>;
    using SafeTsMap = std::unordered_map<RegionID, SafeTsEntryPtr>;

    void updateSafeTS(UInt64 region_id, UInt64 leader_safe_ts, UInt64 self_safe_ts);

    // unit: ms. If safe_ts diff is larger than 2min, we think the data synchronization progress is far behind the leader.
    static const UInt64 SafeTsDiffThreshold = 2 * 60 * 1000;
    bool isSafeTSLag(UInt64 region_id, UInt64 * leader_safe_ts, UInt64 * self_safe_ts);

    UInt64 getSelfSafeTS(UInt64 region_id) const;

private:
    friend class MockTiDB;
    friend class StorageDeltaMerge;

    Table & getOrCreateTable(KeyspaceID keyspace_id, TableID table_id);
    void removeTable(KeyspaceID keyspace_id, TableID table_id);
    InternalRegion & getOrInsertRegion(const Region & region);
    InternalRegion & insertRegion(Table & table, const RegionRangeKeys & region_range_keys, RegionID region_id);
    InternalRegion & insertRegion(Table & table, const Region & region);
    InternalRegion & doGetInternalRegion(KeyspaceTableID ks_table_id, RegionID region_id);

private:
    using TableMap = std::unordered_map<KeyspaceTableID, Table, boost::hash<KeyspaceTableID>>;
    TableMap tables;

    using RegionInfoMap = std::unordered_map<RegionID, KeyspaceTableID>;
    RegionInfoMap regions;
    SafeTsMap safe_ts_map;

    Context * const context;

    mutable std::mutex mutex;
    mutable std::shared_mutex rw_lock;

    LoggerPtr log;
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

    RegionPtrWithBlock(const Base & base_)
        : base(base_)
        , pre_decode_cache(nullptr)
    {}

    /// to be compatible with usage as RegionPtr.
    Base::element_type * operator->() const { return base.operator->(); }
    const Base::element_type & operator*() const { return base.operator*(); }

    /// make it could be cast into RegionPtr implicitly.
    operator const Base &() const { return base; }

    const Base & base;
    CachePtr pre_decode_cache;

private:
    friend struct MockRaftCommand;
    /// Can accept const ref of RegionPtr without cache
    RegionPtrWithBlock(const Base & base_, CachePtr cache)
        : base(base_)
        , pre_decode_cache(std::move(cache))
    {}
};


// A wrap of RegionPtr, with snapshot files directory waitting to be ingested
struct RegionPtrWithSnapshotFiles
{
    using Base = RegionPtr;

    /// can accept const ref of RegionPtr without cache
    RegionPtrWithSnapshotFiles(
        const Base & base_,
        PrehandleResult::Stats && prehandle_stats_,
        std::vector<DM::ExternalDTFileInfo> && external_files_ = {});

    /// to be compatible with usage as RegionPtr.
    Base::element_type * operator->() const { return base.operator->(); }
    const Base::element_type & operator*() const { return base.operator*(); }

    /// make it could be cast into RegionPtr implicitly.
    operator const Base &() const { return base; }


    UInt64 getPrehandleElapsedMillis() const { return prehandle_stats.end_time - prehandle_stats.start_time; }

    UInt64 getPrehandleEndMillis() const { return prehandle_stats.end_time; }

    const Base & base;
    PrehandleResult::Stats prehandle_stats;
    const std::vector<DM::ExternalDTFileInfo> external_files;
};

// A wrap of RegionPtr, with checkpoint info to be ingested
struct RegionPtrWithCheckpointInfo
{
    using Base = RegionPtr;

    RegionPtrWithCheckpointInfo(const Base & base_, CheckpointIngestInfoPtr checkpoint_info_);

    /// to be compatible with usage as RegionPtr.
    Base::element_type * operator->() const { return base.operator->(); }
    const Base::element_type & operator*() const { return base.operator*(); }

    /// make it could be cast into RegionPtr implicitly.
    operator const Base &() const { return base; }

    UInt64 getPrehandleElapsedMillis() const
    {
        // FAP has no prehandle stage
        return 0;
    }

    UInt64 getPrehandleEndMillis() const { return 0; }

    const Base & base;
    CheckpointIngestInfoPtr checkpoint_info;
};

} // namespace DB
