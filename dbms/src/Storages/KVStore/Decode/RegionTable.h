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
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/KVStore/Decode/RegionDataRead.h>
#include <Storages/KVStore/Decode/RegionTable_fwd.h>
#include <Storages/KVStore/Decode/SafeTsManager.h>
#include <Storages/KVStore/Decode/TiKVHandle.h>
#include <Storages/KVStore/Read/RegionException.h>
#include <Storages/KVStore/Read/RegionLockInfo.h>
#include <Storages/KVStore/Region.h>
#include <common/logger_useful.h>

#include <functional>
#include <mutex>
#include <variant>
#include <vector>

namespace DB
{
class RegionRangeKeys;
class RegionTaskLock;

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
    };

    using InternalRegions = std::unordered_map<RegionID, InternalRegion>;

    struct Table : boost::noncopyable
    {
        explicit Table(const TableID table_id_)
            : table_id(table_id_)
            , ctx(createRegionTableCtx())
        {}
        TableID table_id;
        InternalRegions internal_regions;
        RegionTableCtxPtr ctx;
    };

    explicit RegionTable(Context & context_);

    // Iterate over all regions in KVStore, and add them to RegionTable.
    void restore();

    // When a region is added to region table, happens when split and restore.
    void addRegion(const Region & region);

    // Most of the regions are scheduled to TiFlash by a raft snapshot.
    void addPrehandlingRegion(const Region & region);

    // When a region is removed out of TiFlash.
    void removeRegion(RegionID region_id, bool remove_data, const RegionTaskLock &);

    // Used by apply snapshot.
    void replaceRegion(const RegionPtr & old_region, const RegionPtr & new_region);

    /// This functional only shrink the table range of this region_id
    void shrinkRegionRange(const Region & region);

    /// extend range for possible InternalRegion or add one.
    void extendRegionRange(const Region & region, const RegionRangeKeys & region_range_keys);

    // Protects writeBlockByRegionAndFlush and ensures it's executed by only one thread at the same time.
    // Only one thread can do this at the same time.
    // The original name for this function is tryFlushRegion.
    RegionDataReadInfoList tryWriteBlockByRegion(const RegionPtr & region);

    void handleInternalRegionsByTable(
        KeyspaceID keyspace_id,
        TableID table_id,
        std::function<void(const InternalRegions &)> && callback) const;
    void handleInternalRegionsByKeyspace(
        KeyspaceID keyspace_id,
        std::function<void(const TableID table_id, const InternalRegions &)> && callback) const;

    std::vector<RegionID> getRegionIdsByTable(KeyspaceID keyspace_id, TableID table_id) const;
    std::vector<std::pair<RegionID, RegionPtr>> getRegionsByTable(KeyspaceID keyspace_id, TableID table_id) const;

    /// Write the data of the given region into the table with the given table ID, fill the data list for outer to remove.
    /// Will trigger schema sync on read error for only once,
    /// assuming that newer schema can always apply to older data by setting force_decode to true in RegionBlockReader::read.
    /// Note that table schema must be keep unchanged throughout the process of read then write, we take good care of the lock.
    static DM::WriteResult writeCommittedByRegion(
        Context & context,
        const RegionPtr & region,
        RegionDataReadInfoList & data_list_to_remove,
        const LoggerPtr & log,
        bool lock_region = true);

    /// Check region metas and get transaction locks in region.
    static std::variant<LockInfoPtr, RegionException::RegionReadStatus> checkRegionAndGetLocks(
        const TableID table_id,
        const RegionPtr & region,
        const Timestamp start_ts,
        const std::unordered_set<UInt64> * bypass_lock_ts,
        RegionVersion region_version,
        RegionVersion conf_version);

    void clear();

    size_t getTableRegionSize(KeyspaceID keyspace_id, TableID table_id) const;
    void debugClearTableRegionSize(KeyspaceID keyspace_id, TableID table_id);

    SafeTsManager & safeTsMgr() { return safe_ts_mgr; }
    const SafeTsManager & safeTsMgr() const { return safe_ts_mgr; }


private:
    friend class MockTiDB;
    friend class StorageDeltaMerge;

    Table & getOrCreateTable(KeyspaceID keyspace_id, TableID table_id);
    void removeTable(KeyspaceID keyspace_id, TableID table_id);
    InternalRegion & getOrInsertRegion(const Region & region);
    InternalRegion & insertRegion(Table & table, const RegionRangeKeys & region_range_keys, const Region & region);
    InternalRegion & doGetInternalRegion(KeyspaceTableID ks_table_id, RegionID region_id);
    void addTableToIndex(KeyspaceID keyspace_id, TableID table_id);
    void removeTableFromIndex(KeyspaceID keyspace_id, TableID table_id);

private:
    using TableMap = std::unordered_map<KeyspaceTableID, Table, boost::hash<KeyspaceTableID>>;
    TableMap tables;

    using RegionInfoMap = std::unordered_map<RegionID, KeyspaceTableID>;
    RegionInfoMap region_infos;

    using KeyspaceIndex = std::unordered_map<KeyspaceID, std::unordered_set<TableID>, boost::hash<KeyspaceID>>;
    KeyspaceIndex keyspace_index;

    Context * const context;

    SafeTsManager safe_ts_mgr;

    mutable std::mutex mutex;

    LoggerPtr log;
};


} // namespace DB
