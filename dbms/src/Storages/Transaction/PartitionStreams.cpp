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

#include <Common/Allocator.h>
#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/PartitionStreams.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>
#include <Storages/Transaction/Utils.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <common/logger_useful.h>

namespace DB
{
namespace FailPoints
{
extern const char pause_before_apply_raft_cmd[];
extern const char pause_before_apply_raft_snapshot[];
extern const char force_set_safepoint_when_decode_block[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int REGION_DATA_SCHEMA_UPDATED;
extern const int ILLFORMAT_RAFT_ROW;
extern const int TABLE_IS_DROPPED;
} // namespace ErrorCodes


static void writeRegionDataToStorage(
    Context & context,
    const RegionPtrWithBlock & region,
    RegionDataReadInfoList & data_list_read,
    Poco::Logger * log)
{
    constexpr auto FUNCTION_NAME = __FUNCTION__; // NOLINT(readability-identifier-naming)
    const auto & tmt = context.getTMTContext();
    TableID table_id = region->getMappedTableID();
    UInt64 region_decode_cost = -1, write_part_cost = -1;

    /// Declare lambda of atomic read then write to call multiple times.
    auto atomic_read_write = [&](bool force_decode) {
        /// Get storage based on table ID.
        auto storage = tmt.getStorages().get(table_id);
        if (storage == nullptr || storage->isTombstone())
        {
            if (!force_decode) // Need to update.
                return false;
            if (storage == nullptr) // Table must have just been GC-ed.
                return true;
        }

        /// Get a structure read lock throughout decode, during which schema must not change.
        TableStructureLockHolder lock;
        try
        {
            lock = storage->lockStructureForShare(getThreadName());
        }
        catch (DB::Exception & e)
        {
            // If the storage is physical dropped (but not removed from `ManagedStorages`) when we want to write raft data into it, consider the write done.
            if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
                return true;
            else
                throw;
        }

        Block block;
        bool need_decode = true;

        // try to use block cache if exists
        if (region.pre_decode_cache)
        {
            auto schema_version = storage->getTableInfo().schema_version;
            std::stringstream ss;
            region.pre_decode_cache->toString(ss);
            LOG_FMT_DEBUG(log, "{}: {} got pre-decode cache {}, storage schema version: {}", FUNCTION_NAME, region->toString(), ss.str(), schema_version);

            if (region.pre_decode_cache->schema_version == schema_version)
            {
                block = std::move(region.pre_decode_cache->block);
                need_decode = false;
            }
            else
            {
                LOG_FMT_DEBUG(log, "{}: schema version not equal, try to re-decode region cache into block", FUNCTION_NAME);
                region.pre_decode_cache->block.clear();
            }
        }

        /// Read region data as block.
        Stopwatch watch;

        Int64 block_decoding_schema_version = -1;
        BlockUPtr block_ptr = nullptr;
        if (need_decode)
        {
            LOG_FMT_TRACE(log, "{} begin to decode table {}, region {}", FUNCTION_NAME, table_id, region->id());
            DecodingStorageSchemaSnapshotConstPtr decoding_schema_snapshot;
            std::tie(decoding_schema_snapshot, block_ptr) = storage->getSchemaSnapshotAndBlockForDecoding(lock, true);
            block_decoding_schema_version = decoding_schema_snapshot->decoding_schema_version;

            auto reader = RegionBlockReader(decoding_schema_snapshot);
            if (!reader.read(*block_ptr, data_list_read, force_decode))
                return false;
            region_decode_cost = watch.elapsedMilliseconds();
            GET_METRIC(tiflash_raft_write_data_to_storage_duration_seconds, type_decode).Observe(region_decode_cost / 1000.0);
        }

        /// Write block into storage.
        // Release the alter lock so that writing does not block DDL operations
        TableLockHolder drop_lock;
        std::tie(std::ignore, drop_lock) = std::move(lock).release();
        watch.restart();
        // Note: do NOT use typeid_cast, since Storage is multi-inherited and typeid_cast will return nullptr
        switch (storage->engineType())
        {
        case ::TiDB::StorageEngine::DT:
        {
            auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
            if (need_decode)
                dm_storage->write(*block_ptr, context.getSettingsRef());
            else
                dm_storage->write(block, context.getSettingsRef());
            break;
        }
        default:
            throw Exception("Unknown StorageEngine: " + toString(static_cast<Int32>(storage->engineType())), ErrorCodes::LOGICAL_ERROR);
        }
        write_part_cost = watch.elapsedMilliseconds();
        GET_METRIC(tiflash_raft_write_data_to_storage_duration_seconds, type_write).Observe(write_part_cost / 1000.0);
        if (need_decode)
            storage->releaseDecodingBlock(block_decoding_schema_version, std::move(block_ptr));

        LOG_FMT_TRACE(log, "{}: table {}, region {}, cost [region decode {},  write part {}] ms", FUNCTION_NAME, table_id, region->id(), region_decode_cost, write_part_cost);
        return true;
    };

    /// In TiFlash, the actions between applying raft log and schema changes are not strictly synchronized.
    /// There could be a chance that some raft logs come after a table gets tombstoned. Take care of it when
    /// decoding data. Check the test case for more details.
    FAIL_POINT_PAUSE(FailPoints::pause_before_apply_raft_cmd);

    /// Try read then write once.
    {
        if (atomic_read_write(false))
            return;
    }

    /// If first try failed, sync schema and force read then write.
    {
        GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Increment();
        tmt.getSchemaSyncer()->syncSchemas(context);

        if (!atomic_read_write(true))
            // Failure won't be tolerated this time.
            // TODO: Enrich exception message.
            throw Exception("Write region " + std::to_string(region->id()) + " to table " + std::to_string(table_id) + " failed",
                            ErrorCodes::LOGICAL_ERROR);
    }
}

std::variant<RegionDataReadInfoList, RegionException::RegionReadStatus, LockInfoPtr> resolveLocksAndReadRegionData(
    const TiDB::TableID table_id,
    const RegionPtr & region,
    const Timestamp start_ts,
    const std::unordered_set<UInt64> * bypass_lock_ts,
    RegionVersion region_version,
    RegionVersion conf_version,
    bool resolve_locks,
    bool need_data_value)
{
    RegionDataReadInfoList data_list_read;
    DecodedLockCFValuePtr lock_value;
    {
        auto scanner = region->createCommittedScanner();

        /// Some sanity checks for region meta.
        {
            /**
             * special check: when source region is merging, read_index can not guarantee the behavior about target region.
             * Reject all read request for safety.
             * Only when region is Normal can continue read process.
             */
            if (region->peerState() != raft_serverpb::PeerState::Normal)
                return RegionException::RegionReadStatus::NOT_FOUND;

            const auto & meta_snap = region->dumpRegionMetaSnapshot();
            // No need to check conf_version if its peer state is normal
            std::ignore = conf_version;
            if (meta_snap.ver != region_version)
                return RegionException::RegionReadStatus::EPOCH_NOT_MATCH;

            // todo check table id
            TableID mapped_table_id;
            if (!computeMappedTableID(*meta_snap.range->rawKeys().first, mapped_table_id) || mapped_table_id != table_id)
                throw Exception("Should not happen, region not belong to table: table id in region is " + std::to_string(mapped_table_id)
                                    + ", expected table id is " + std::to_string(table_id),
                                ErrorCodes::LOGICAL_ERROR);
        }

        /// Deal with locks.
        if (resolve_locks)
        {
            /// Check if there are any lock should be resolved, if so, throw LockException.
            lock_value = scanner.getLockInfo(RegionLockReadQuery{.read_tso = start_ts, .bypass_lock_ts = bypass_lock_ts});
        }

        /// If there is no lock, leave scope of region scanner and raise LockException.
        /// Read raw KVs from region cache.
        if (!lock_value)
        {
            // Shortcut for empty region.
            if (!scanner.hasNext())
                return data_list_read;

            data_list_read.reserve(scanner.writeMapSize());

            // Tiny optimization for queries that need only handle, tso, delmark.
            do
            {
                data_list_read.emplace_back(scanner.next(need_data_value));
            } while (scanner.hasNext());
        }
    }

    if (lock_value)
        return lock_value->intoLockInfo();

    return data_list_read;
}

std::optional<RegionDataReadInfoList> ReadRegionCommitCache(const RegionPtr & region, bool lock_region = true)
{
    auto scanner = region->createCommittedScanner(lock_region);

    /// Some sanity checks for region meta.
    {
        if (region->isPendingRemove())
            return std::nullopt;
    }

    /// Read raw KVs from region cache.
    {
        // Shortcut for empty region.
        if (!scanner.hasNext())
            return std::nullopt;

        RegionDataReadInfoList data_list_read;
        data_list_read.reserve(scanner.writeMapSize());

        do
        {
            data_list_read.emplace_back(scanner.next());
        } while (scanner.hasNext());
        return data_list_read;
    }
}

void RemoveRegionCommitCache(const RegionPtr & region, const RegionDataReadInfoList & data_list_read, bool lock_region = true)
{
    /// Remove data in region.
    auto remover = region->createCommittedRemover(lock_region);
    for (const auto & [handle, write_type, commit_ts, value] : data_list_read)
    {
        std::ignore = write_type;
        std::ignore = value;

        remover.remove({handle, commit_ts});
    }
}

// ParseTS parses the ts to (physical,logical).
// Reference: https://github.com/tikv/pd/blob/v5.1.1/pkg/tsoutil/tso.go#L22-L33
// ts: timestamp from TSO.
// Return <physical time in ms, logical time>.
static inline std::pair<UInt64, UInt64> parseTS(UInt64 ts)
{
    constexpr int physical_shift_bits = 18;
    constexpr UInt64 logical_bits = (1 << physical_shift_bits) - 1;
    auto logical = ts & logical_bits;
    auto physical = ts >> physical_shift_bits;
    return {physical, logical};
}

static inline void reportUpstreamLatency(const RegionDataReadInfoList & data_list_read)
{
    if (unlikely(data_list_read.empty()))
    {
        return;
    }
    auto ts = std::get<2>(data_list_read.front());
    auto [physical_ms, logical] = parseTS(ts);
    std::ignore = logical;
    UInt64 curr_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    if (likely(curr_ms > physical_ms))
    {
        auto latency_ms = curr_ms - physical_ms;
        GET_METRIC(tiflash_raft_upstream_latency, type_write).Observe(static_cast<double>(latency_ms) / 1000.0);
    }
}

void RegionTable::writeBlockByRegion(
    Context & context,
    const RegionPtrWithBlock & region,
    RegionDataReadInfoList & data_list_to_remove,
    Poco::Logger * log,
    bool lock_region)
{
    std::optional<RegionDataReadInfoList> data_list_read = std::nullopt;
    if (region.pre_decode_cache)
    {
        // if schema version changed, use the kv data to rebuild block cache
        data_list_read = std::move(region.pre_decode_cache->data_list_read);
    }
    else
    {
        data_list_read = ReadRegionCommitCache(region, lock_region);
    }

    if (!data_list_read)
        return;

    reportUpstreamLatency(*data_list_read);
    writeRegionDataToStorage(context, region, *data_list_read, log);

    RemoveRegionCommitCache(region, *data_list_read, lock_region);

    /// Save removed data to outer.
    data_list_to_remove = std::move(*data_list_read);
}

RegionTable::ResolveLocksAndWriteRegionRes RegionTable::resolveLocksAndWriteRegion(TMTContext & tmt,
                                                                                   const TiDB::TableID table_id,
                                                                                   const RegionPtr & region,
                                                                                   const Timestamp start_ts,
                                                                                   const std::unordered_set<UInt64> * bypass_lock_ts,
                                                                                   RegionVersion region_version,
                                                                                   RegionVersion conf_version,
                                                                                   Poco::Logger * log)
{
    auto region_data_lock = resolveLocksAndReadRegionData(table_id,
                                                          region,
                                                          start_ts,
                                                          bypass_lock_ts,
                                                          region_version,
                                                          conf_version,
                                                          /* resolve_locks */ true,
                                                          /* need_data_value */ true);

    return std::visit(variant_op::overloaded{
                          [&](RegionDataReadInfoList & data_list_read) -> ResolveLocksAndWriteRegionRes {
                              if (data_list_read.empty())
                                  return RegionException::RegionReadStatus::OK;
                              auto & context = tmt.getContext();
                              writeRegionDataToStorage(context, region, data_list_read, log);
                              RemoveRegionCommitCache(region, data_list_read);
                              return RegionException::RegionReadStatus::OK;
                          },
                          [](auto & r) -> ResolveLocksAndWriteRegionRes { return std::move(r); },
                      },
                      region_data_lock);
}

/// Pre-decode region data into block cache and remove committed data from `region`
RegionPtrWithBlock::CachePtr GenRegionPreDecodeBlockData(const RegionPtr & region, Context & context)
{
    const auto & tmt = context.getTMTContext();
    {
        Timestamp gc_safe_point = 0;
        if (auto pd_client = tmt.getPDClient(); !pd_client->isMock())
        {
            gc_safe_point
                = PDClientHelper::getGCSafePointWithRetry(pd_client, false, context.getSettingsRef().safe_point_update_interval_seconds);
        }
        /**
         * In 5.0.1, feature `compaction filter` is enabled by default. Under such feature tikv will do gc in write & default cf individually.
         * If some rows were updated and add tiflash replica, tiflash store may receive region snapshot with unmatched data in write & default cf sst files.
         */
        region->tryCompactionFilter(gc_safe_point);
    }
    std::optional<RegionDataReadInfoList> data_list_read = std::nullopt;
    try
    {
        data_list_read = ReadRegionCommitCache(region);
        if (!data_list_read)
            return nullptr;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::ILLFORMAT_RAFT_ROW)
        {
            // br or lighting may write illegal data into tikv, skip pre-decode and ingest sst later.
            LOG_FMT_WARNING(&Poco::Logger::get(__PRETTY_FUNCTION__),
                            "Got error while reading region committed cache: {}. Skip pre-decode and keep original cache.",
                            e.displayText());
            // set data_list_read and let apply snapshot process use empty block
            data_list_read = RegionDataReadInfoList();
        }
        else
            throw;
    }

    TableID table_id = region->getMappedTableID();
    Int64 schema_version = DEFAULT_UNSPECIFIED_SCHEMA_VERSION;
    Block res_block;

    const auto atomic_decode = [&](bool force_decode) -> bool {
        Stopwatch watch;
        auto storage = tmt.getStorages().get(table_id);
        if (storage == nullptr || storage->isTombstone())
        {
            if (!force_decode) // Need to update.
                return false;
            if (storage == nullptr) // Table must have just been GC-ed.
                return true;
        }

        /// Get a structure read lock throughout decode, during which schema must not change.
        TableStructureLockHolder lock;
        try
        {
            lock = storage->lockStructureForShare(getThreadName());
        }
        catch (DB::Exception & e)
        {
            // If the storage is physical dropped (but not removed from `ManagedStorages`) when we want to decode snapshot, consider the decode done.
            if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
                return true;
            else
                throw;
        }

        DecodingStorageSchemaSnapshotConstPtr decoding_schema_snapshot;
        std::tie(decoding_schema_snapshot, std::ignore) = storage->getSchemaSnapshotAndBlockForDecoding(lock, false);
        res_block = createBlockSortByColumnID(decoding_schema_snapshot);
        auto reader = RegionBlockReader(decoding_schema_snapshot);
        if (!reader.read(res_block, *data_list_read, force_decode))
            return false;
        GET_METRIC(tiflash_raft_write_data_to_storage_duration_seconds, type_decode).Observe(watch.elapsedSeconds());
        return true;
    };

    /// In TiFlash, the actions between applying raft log and schema changes are not strictly synchronized.
    /// There could be a chance that some raft logs come after a table gets tombstoned. Take care of it when
    /// decoding data. Check the test case for more details.
    FAIL_POINT_PAUSE(FailPoints::pause_before_apply_raft_snapshot);

    if (!atomic_decode(false))
    {
        GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Increment();
        tmt.getSchemaSyncer()->syncSchemas(context);

        if (!atomic_decode(true))
            throw Exception("Pre-decode " + region->toString() + " cache to table " + std::to_string(table_id) + " block failed",
                            ErrorCodes::LOGICAL_ERROR);
    }

    RemoveRegionCommitCache(region, *data_list_read);

    return std::make_unique<RegionPreDecodeBlockData>(std::move(res_block), schema_version, std::move(*data_list_read));
}

std::tuple<TableLockHolder, std::shared_ptr<StorageDeltaMerge>, DecodingStorageSchemaSnapshotConstPtr> //
AtomicGetStorageSchema(const RegionPtr & region, TMTContext & tmt)
{
    TableLockHolder drop_lock = nullptr;
    std::shared_ptr<StorageDeltaMerge> dm_storage;
    DecodingStorageSchemaSnapshotConstPtr schema_snapshot;

    auto table_id = region->getMappedTableID();
    LOG_FMT_DEBUG(&Poco::Logger::get(__PRETTY_FUNCTION__), "Get schema for table {}", table_id);
    auto context = tmt.getContext();
    const auto atomic_get = [&](bool force_decode) -> bool {
        auto storage = tmt.getStorages().get(table_id);
        if (storage == nullptr)
        {
            if (!force_decode)
                return false;
            if (storage == nullptr) // Table must have just been GC-ed
                return true;
        }
        // Get a structure read lock. It will throw exception if the table has been dropped,
        // the caller should handle this situation.
        auto table_lock = storage->lockStructureForShare(getThreadName());
        dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
        // only dt storage engine support `getSchemaSnapshotAndBlockForDecoding`, other engine will throw exception
        std::tie(schema_snapshot, std::ignore) = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false);
        std::tie(std::ignore, drop_lock) = std::move(table_lock).release();
        return true;
    };

    if (!atomic_get(false))
    {
        GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Increment();
        tmt.getSchemaSyncer()->syncSchemas(context);

        if (!atomic_get(true))
            throw Exception("Get " + region->toString() + " belonging table " + DB::toString(table_id) + " is_command_handle fail",
                            ErrorCodes::LOGICAL_ERROR);
    }

    return {std::move(drop_lock), std::move(dm_storage), std::move(schema_snapshot)};
}

static Block sortColumnsBySchemaSnap(Block && ori, const DM::ColumnDefines & schema)
{
#ifndef NDEBUG
    // Some trival check to ensure the input is legal
    if (ori.columns() != schema.size())
    {
        throw Exception("Try to sortColumnsBySchemaSnap with different column size [block_columns=" + DB::toString(ori.columns())
                        + "] [schema_columns=" + DB::toString(schema.size()) + "]");
    }
#endif

    std::map<DB::ColumnID, size_t> index_by_cid;
    for (size_t i = 0; i < ori.columns(); ++i)
    {
        const ColumnWithTypeAndName & c = ori.getByPosition(i);
        index_by_cid[c.column_id] = i;
    }

    Block res;
    for (const auto & cd : schema)
    {
        res.insert(ori.getByPosition(index_by_cid[cd.id]));
    }
#ifndef NDEBUG
    assertBlocksHaveEqualStructure(res, DM::toEmptyBlock(schema), "sortColumnsBySchemaSnap");
#endif

    return res;
}

/// Decode region data into block and belonging schema snapshot, remove committed data from `region`
/// The return value is a block that store the committed data scanned and removed from `region`.
/// The columns of returned block is sorted by `schema_snap`.
Block GenRegionBlockDataWithSchema(const RegionPtr & region, //
                                   const DecodingStorageSchemaSnapshotConstPtr & schema_snap,
                                   Timestamp gc_safepoint,
                                   bool force_decode,
                                   TMTContext & tmt)
{
    // In 5.0.1, feature `compaction filter` is enabled by default. Under such feature tikv will do gc in write & default cf individually.
    // If some rows were updated and add tiflash replica, tiflash store may receive region snapshot with unmatched data in write & default cf sst files.
    fiu_do_on(FailPoints::force_set_safepoint_when_decode_block,
              { gc_safepoint = 10000000; }); // Mock a GC safepoint for testing compaction filter
    region->tryCompactionFilter(gc_safepoint);

    std::optional<RegionDataReadInfoList> data_list_read = std::nullopt;
    data_list_read = ReadRegionCommitCache(region);

    Block res_block;
    // No committed data, just return
    if (!data_list_read)
        return res_block;

    auto context = tmt.getContext();

    {
        Stopwatch watch;
        {
            // Compare schema_snap with current schema, throw exception if changed.
            auto reader = RegionBlockReader(schema_snap);
            res_block = createBlockSortByColumnID(schema_snap);
            if (unlikely(!reader.read(res_block, *data_list_read, force_decode)))
                throw Exception("RegionBlockReader decode error", ErrorCodes::REGION_DATA_SCHEMA_UPDATED);
        }

        GET_METRIC(tiflash_raft_write_data_to_storage_duration_seconds, type_decode).Observe(watch.elapsedSeconds());
    }

    res_block = sortColumnsBySchemaSnap(std::move(res_block), *(schema_snap->column_defines));

    // Remove committed data
    RemoveRegionCommitCache(region, *data_list_read);

    return res_block;
}

} // namespace DB
