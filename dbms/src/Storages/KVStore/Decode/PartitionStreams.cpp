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

#include <Common/Allocator.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/KVStore/Decode/PartitionStreams.h>
#include <Storages/KVStore/Decode/RegionBlockReader.h>
#include <Storages/KVStore/Decode/RegionTable.h>
#include <Storages/KVStore/Decode/TiKVRange.h>
#include <Storages/KVStore/MultiRaft/Spill/RegionUncommittedDataList.h>
#include <Storages/KVStore/Read/LockException.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Utils.h>
#include <Storages/StorageDeltaMerge.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <common/logger_useful.h>
#include <fiu.h>

namespace DB
{
namespace FailPoints
{
extern const char pause_before_apply_raft_cmd[];
extern const char force_set_safepoint_when_decode_block[];
extern const char unblock_query_init_after_write[];
extern const char pause_query_init[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int REGION_DATA_SCHEMA_UPDATED;
extern const int ILLFORMAT_RAFT_ROW;
extern const int TABLE_IS_DROPPED;
} // namespace ErrorCodes

struct AtomicReadWriteCtx
{
    AtomicReadWriteCtx(
        const LoggerPtr & log_,
        const Context & context_,
        const TMTContext & tmt_,
        KeyspaceID keyspace_id_,
        TableID table_id_)
        : log(log_)
        , context(context_)
        , tmt(tmt_)
        , keyspace_id(keyspace_id_)
        , table_id(table_id_)
    {}

    const LoggerPtr & log;
    const Context & context;
    const TMTContext & tmt;
    const KeyspaceID keyspace_id;
    const TableID table_id;
    DM::WriteResult write_result = std::nullopt;
    UInt64 region_decode_cost = -1;
    UInt64 write_part_cost = -1;
};

static void inline writeCommittedBlockDataIntoStorage(
    AtomicReadWriteCtx & rw_ctx,
    TableStructureLockHolder & lock,
    ManageableStoragePtr & storage,
    Block & block,
    RegionAppliedStatus applied_status)
{
    /// Write block into storage.
    // Release the alter lock so that writing does not block DDL operations
    TableLockHolder drop_lock;
    std::tie(std::ignore, drop_lock) = std::move(lock).release();
    Stopwatch watch;

    RUNTIME_CHECK_MSG(
        storage->engineType() == ::TiDB::StorageEngine::DT,
        "Unknown StorageEngine: {}",
        static_cast<Int32>(storage->engineType()));
    // Note: do NOT use typeid_cast, since Storage is multi-inherited and typeid_cast will return nullptr
    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    rw_ctx.write_result = dm_storage->write(block, rw_ctx.context.getSettingsRef(), applied_status);
    rw_ctx.write_part_cost = watch.elapsedMilliseconds();
    GET_METRIC(tiflash_raft_write_data_to_storage_duration_seconds, type_write)
        .Observe(rw_ctx.write_part_cost / 1000.0);
}

template <typename ReadList>
static inline bool atomicReadWrite(
    AtomicReadWriteCtx & rw_ctx,
    const RegionPtrWithBlock & region,
    ReadList & data_list_read,
    bool force_decode)
{
    /// Get storage based on table ID.
    auto storage = rw_ctx.tmt.getStorages().get(rw_ctx.keyspace_id, rw_ctx.table_id);
    if (storage == nullptr)
    {
        // - force_decode == false and storage not exist, let upper level sync schema and retry.
        // - force_decode == true and storage not exist. It could be the RaftLog or Snapshot comes
        //   after the schema is totally exceed the GC safepoint. And TiFlash know nothing about
        //   the schema. We can only throw away those committed rows.
        // In both cases, no exception will be thrown.
        return force_decode;
    }

    /// Get a structure read lock throughout decode, during which schema must not change.
    TableStructureLockHolder lock;
    try
    {
        lock = storage->lockStructureForShare(getThreadNameAndID());
    }
    catch (DB::Exception & e)
    {
        // If the storage is physical dropped (but not removed from `ManagedStorages`) when we want to write raft data into it, consider the write done.
        if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
            return true;
        else
            throw;
    }

    /// Read region data as block.
    Stopwatch watch;
    Int64 block_decoding_schema_epoch = -1;
    BlockUPtr block_ptr = nullptr;
    bool should_handle_version_col = true;
    if constexpr (std::is_same_v<ReadList, RegionUncommittedDataList>)
    {
        should_handle_version_col = false;
    }

    // Currently, RegionPtrWithBlock with a not-null CachePtr is only used in debug functions
    // to apply a pre-decoded snapshot. So it will not take place here.
    // In short, we always decode here because there is no pre-decode cache.
    {
        LOG_TRACE(
            rw_ctx.log,
            "begin to decode keyspace={} table_id={} region_id={}",
            rw_ctx.keyspace_id,
            rw_ctx.table_id,
            region->id());
        DecodingStorageSchemaSnapshotConstPtr decoding_schema_snapshot;
        std::tie(decoding_schema_snapshot, block_ptr)
            = storage->getSchemaSnapshotAndBlockForDecoding(lock, true, should_handle_version_col);
        block_decoding_schema_epoch = decoding_schema_snapshot->decoding_schema_epoch;

        auto reader = RegionBlockReader(decoding_schema_snapshot);
        if (!reader.read(*block_ptr, data_list_read, force_decode))
            return false;
        rw_ctx.region_decode_cost = watch.elapsedMilliseconds();
        GET_METRIC(tiflash_raft_write_data_to_storage_duration_seconds, type_decode)
            .Observe(rw_ctx.region_decode_cost / 1000.0);
    }
    if constexpr (std::is_same_v<ReadList, RegionDataReadInfoList>)
    {
        RUNTIME_CHECK(block_ptr != nullptr);
        writeCommittedBlockDataIntoStorage(
            rw_ctx,
            lock,
            storage,
            *block_ptr,
            {.region_id = region->id(), .applied_index = region->appliedIndex()});
        storage->releaseDecodingBlock(block_decoding_schema_epoch, std::move(block_ptr));
    }
    else
    {
        // TODO(Spill) Implement spill logic.
        RUNTIME_CHECK(false);
    }
    LOG_TRACE(
        rw_ctx.log,
        "keyspace={} table_id={} region_id={} cost [region decode {}, write part {}] ms",
        rw_ctx.keyspace_id,
        rw_ctx.table_id,
        region->id(),
        rw_ctx.region_decode_cost,
        rw_ctx.write_part_cost);
    return true;
}

template DM::WriteResult writeRegionDataToStorage<RegionUncommittedDataList>(
    Context & context,
    const RegionPtrWithBlock & region,
    RegionUncommittedDataList & data_list_read,
    const LoggerPtr & log);
template DM::WriteResult writeRegionDataToStorage<RegionDataReadInfoList>(
    Context & context,
    const RegionPtrWithBlock & region,
    RegionDataReadInfoList & data_list_read,
    const LoggerPtr & log);

// TODO(Spill) rename it after we support spill.
// ReadList could be RegionDataReadInfoList
template <typename ReadList>
DM::WriteResult writeRegionDataToStorage(
    Context & context,
    const RegionPtrWithBlock & region,
    ReadList & data_list_read,
    const LoggerPtr & log)
{
    const auto & tmt = context.getTMTContext();
    const auto keyspace_id = region->getKeyspaceID();
    const auto table_id = region->getMappedTableID();

    AtomicReadWriteCtx rw_ctx(log, context, tmt, keyspace_id, table_id);

    /// In TiFlash, the actions between applying raft log and schema changes are not strictly synchronized.
    /// There could be a chance that some raft logs come after a table gets tombstoned. Take care of it when
    /// decoding data. Check the test case for more details.
    FAIL_POINT_PAUSE(FailPoints::pause_before_apply_raft_cmd);

    /// disable pause_query_init when the write action finish, to make the query action continue.
    /// the usage of unblock_query_init_after_write and pause_query_init can refer to InterpreterSelectQuery::init
    SCOPE_EXIT({
        fiu_do_on(FailPoints::unblock_query_init_after_write, {
            FailPointHelper::disableFailPoint(FailPoints::pause_query_init);
        });
    });

    /// Try read then write once.
    {
        if (atomicReadWrite(rw_ctx, region, data_list_read, false))
        {
            return std::move(rw_ctx.write_result);
        }
    }

    /// If first try failed, sync schema and force read then write.
    {
        GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Increment();
        Stopwatch watch;
        tmt.getSchemaSyncerManager()->syncTableSchema(context, keyspace_id, table_id);
        auto schema_sync_cost = watch.elapsedMilliseconds();
        LOG_INFO(log, "sync schema cost {} ms, keyspace={} table_id={}", schema_sync_cost, keyspace_id, table_id);
        if (!atomicReadWrite(rw_ctx, region, data_list_read, true))
        {
            // Failure won't be tolerated this time.
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Write region failed! region_id={} keyspace={} table_id={}",
                region->id(),
                keyspace_id,
                table_id);
        }
        return std::move(rw_ctx.write_result);
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
        auto scanner = region->createCommittedScanner(true, need_data_value);

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
            if (!computeMappedTableID(*meta_snap.range->rawKeys().first, mapped_table_id)
                || mapped_table_id != table_id)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Should not happen, region not belong to table, table_id={} expect_table_id={}",
                    mapped_table_id,
                    table_id);
        }

        /// Deal with locks.
        if (resolve_locks)
        {
            /// Check if there are any lock should be resolved, if so, throw LockException.
            lock_value
                = scanner.getLockInfo(RegionLockReadQuery{.read_tso = start_ts, .bypass_lock_ts = bypass_lock_ts});
        }

        /// If there is no lock, leave scope of region scanner and raise LockException.
        /// Read raw KVs from region cache.
        if (!lock_value)
        {
            // Shortcut for empty region.
            if (!scanner.hasNext())
                return data_list_read;

            // If worked with raftstore v2, the final size may not equal to here.
            data_list_read.reserve(scanner.writeMapSize());

            // Tiny optimization for queries that need only handle, tso, delmark.
            do
            {
                data_list_read.emplace_back(scanner.next());
            } while (scanner.hasNext());
        }
    }

    if (lock_value)
        return lock_value->intoLockInfo();

    return data_list_read;
}

std::optional<RegionDataReadInfoList> ReadRegionCommitCache(const RegionPtr & region, bool lock_region)
{
    auto scanner = region->createCommittedScanner(lock_region, true);

    /// Some sanity checks for region meta.
    if (region->isPendingRemove())
        return std::nullopt;

    /// Read raw KVs from region cache.
    // Shortcut for empty region.
    if (!scanner.hasNext())
        return std::nullopt;

    RegionDataReadInfoList data_list_read;
    data_list_read.reserve(scanner.writeMapSize());
    auto read_tso = region->getLastObservedReadTso();
    Timestamp min_error_read_tso = std::numeric_limits<Timestamp>::max();
    size_t error_prone_count = 0;
    do
    {
        // A read index request with read_tso will stop concurrency manager from committing txns with smaller tso by advancing max_ts to at least read_tso.
        auto data_read = scanner.next();
        // It's a natual fallback when there has not been any read_tso on this region.
        if (data_read.commit_ts <= read_tso)
        {
            error_prone_count++;
            min_error_read_tso = std::min(min_error_read_tso, data_read.commit_ts);
        }
        data_list_read.emplace_back(std::move(data_read));
    } while (scanner.hasNext());
    if (error_prone_count > 0)
    {
        LOG_INFO(
            DB::Logger::get(),
            "Error prone txn commit error_prone_count={} min_rso={} region_id={} applied_index={}",
            error_prone_count,
            min_error_read_tso,
            region->id(),
            region->appliedIndex());
    }
    return data_list_read;
}

void RemoveRegionCommitCache(const RegionPtr & region, const RegionDataReadInfoList & data_list_read, bool lock_region)
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
    auto ts = data_list_read.front().commit_ts;
    auto [physical_ms, logical] = parseTS(ts);
    std::ignore = logical;
    UInt64 curr_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now())
                         .time_since_epoch()
                         .count();
    if (likely(curr_ms > physical_ms))
    {
        auto latency_ms = curr_ms - physical_ms;
        GET_METRIC(tiflash_raft_upstream_latency, type_write).Observe(static_cast<double>(latency_ms) / 1000.0);
    }
}

DM::WriteResult RegionTable::writeCommittedByRegion(
    Context & context,
    const RegionPtrWithBlock & region,
    RegionDataReadInfoList & data_list_to_remove,
    const LoggerPtr & log,
    bool lock_region)
{
    std::optional<RegionDataReadInfoList> maybe_data_list_read = std::nullopt;
    if (region.pre_decode_cache)
    {
        // If schema version changed, use the kv data to rebuild block cache
        maybe_data_list_read = std::move(region.pre_decode_cache->data_list_read);
    }
    else
    {
        maybe_data_list_read = ReadRegionCommitCache(region, lock_region);
    }

    if (!maybe_data_list_read.has_value())
        return std::nullopt;

    RegionDataReadInfoList & data_list_read = maybe_data_list_read.value();
    reportUpstreamLatency(data_list_read);
    auto write_result = writeRegionDataToStorage(context, region, data_list_read, log);
    auto prev_region_size = region->dataSize();
    RemoveRegionCommitCache(region, data_list_read, lock_region);
    auto new_region_size = region->dataSize();
    if likely (new_region_size <= prev_region_size)
    {
        auto committed_bytes = prev_region_size - new_region_size;
        GET_METRIC(tiflash_raft_write_flow_bytes, type_write_committed).Observe(committed_bytes);
        GET_METRIC(tiflash_raft_throughput_bytes, type_write_committed).Increment(committed_bytes);
        GET_METRIC(tiflash_raft_raft_frequent_events_count, type_write_commit).Increment(1);
    }
    /// Save removed data to outer.
    data_list_to_remove = std::move(data_list_read);
    return write_result;
}

RegionTable::ResolveLocksAndWriteRegionRes RegionTable::resolveLocksAndWriteRegion(
    TMTContext & tmt,
    const TiDB::TableID table_id,
    const RegionPtr & region,
    const Timestamp start_ts,
    const std::unordered_set<UInt64> * bypass_lock_ts,
    RegionVersion region_version,
    RegionVersion conf_version,
    const LoggerPtr & log)
{
    auto region_data_lock = resolveLocksAndReadRegionData(
        table_id,
        region,
        start_ts,
        bypass_lock_ts,
        region_version,
        conf_version,
        /* resolve_locks */ true,
        /* need_data_value */ true);

    return std::visit(
        variant_op::overloaded{
            [&](RegionDataReadInfoList & data_list_read) -> ResolveLocksAndWriteRegionRes {
                if (data_list_read.empty())
                    return RegionException::RegionReadStatus::OK;
                auto & context = tmt.getContext();
                // There is no raft input here, so we can just ignore the fg flush request.
                writeRegionDataToStorage(context, region, data_list_read, log);
                RemoveRegionCommitCache(region, data_list_read);
                return RegionException::RegionReadStatus::OK;
            },
            [](auto & r) -> ResolveLocksAndWriteRegionRes { return std::move(r); },
        },
        region_data_lock);
}

std::tuple<TableLockHolder, std::shared_ptr<StorageDeltaMerge>, DecodingStorageSchemaSnapshotConstPtr> //
AtomicGetStorageSchema(const RegionPtr & region, TMTContext & tmt)
{
    TableLockHolder drop_lock = nullptr;
    std::shared_ptr<StorageDeltaMerge> dm_storage;
    DecodingStorageSchemaSnapshotConstPtr schema_snapshot;

    auto keyspace_id = region->getKeyspaceID();
    auto table_id = region->getMappedTableID();
    LOG_DEBUG(Logger::get(__PRETTY_FUNCTION__), "Get schema, keyspace={} table_id={}", keyspace_id, table_id);
    auto & context = tmt.getContext();
    const auto atomic_get = [&](bool force_decode) -> bool {
        auto storage = tmt.getStorages().get(keyspace_id, table_id);
        if (storage == nullptr)
        {
            if (!force_decode)
                return false;
            if (storage == nullptr) // Table must have just been GC-ed
                return true;
        }
        // Get a structure read lock. It will throw exception if the table has been dropped,
        // the caller should handle this situation.
        auto table_lock = storage->lockStructureForShare(getThreadNameAndID());
        dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
        // only dt storage engine support `getSchemaSnapshotAndBlockForDecoding`, other engine will throw exception
        std::tie(schema_snapshot, std::ignore) = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false, true);
        std::tie(std::ignore, drop_lock) = std::move(table_lock).release();
        return true;
    };

    if (!atomic_get(false))
    {
        GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Increment();
        Stopwatch watch;
        tmt.getSchemaSyncerManager()->syncTableSchema(context, keyspace_id, table_id);
        auto schema_sync_cost = watch.elapsedMilliseconds();
        LOG_INFO(
            Logger::get("AtomicGetStorageSchema"),
            "sync schema cost {} ms, keyspace={} table_id={}",
            schema_sync_cost,
            keyspace_id,
            table_id);

        if (!atomic_get(true))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "AtomicGetStorageSchema failed, region={} keyspace={} table_id={}",
                region->toString(),
                keyspace_id,
                table_id);
    }

    return {std::move(drop_lock), std::move(dm_storage), std::move(schema_snapshot)};
}

static Block sortColumnsBySchemaSnap(Block && ori, const DM::ColumnDefines & schema)
{
#ifndef NDEBUG
    // Some trival check to ensure the input is legal
    if (ori.columns() != schema.size())
    {
        throw Exception(
            "Try to sortColumnsBySchemaSnap with different column size [block_columns=" + DB::toString(ori.columns())
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
Block GenRegionBlockDataWithSchema(
    const RegionPtr & region, //
    const DecodingStorageSchemaSnapshotConstPtr & schema_snap,
    Timestamp gc_safepoint,
    bool force_decode,
    TMTContext & /* */)
{
    // In 5.0.1, feature `compaction filter` is enabled by default. Under such feature tikv will do gc in write & default cf individually.
    // If some rows were updated and add tiflash replica, tiflash store may receive region snapshot with unmatched data in write & default cf sst files.
    fiu_do_on(FailPoints::force_set_safepoint_when_decode_block, {
        gc_safepoint = 10000000;
    }); // Mock a GC safepoint for testing compaction filter
    region->tryCompactionFilter(gc_safepoint);

    std::optional<RegionDataReadInfoList> data_list_read = ReadRegionCommitCache(region, true);

    Block res_block;
    // No committed data, just return
    if (!data_list_read)
        return res_block;
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

    auto prev_region_size = region->dataSize();
    // Remove committed data
    RemoveRegionCommitCache(region, *data_list_read);
    auto new_region_size = region->dataSize();
    if likely (new_region_size <= prev_region_size)
    {
        auto committed_bytes = prev_region_size - new_region_size;
        GET_METRIC(tiflash_raft_throughput_bytes, type_snapshot_committed).Increment(committed_bytes);
    }
    return res_block;
}

} // namespace DB
