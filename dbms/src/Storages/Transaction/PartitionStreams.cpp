#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>
#include <Storages/Transaction/Utils.h>
#include <common/logger_useful.h>

namespace DB
{
namespace FailPoints
{
extern const char pause_before_apply_raft_cmd[];
extern const char pause_before_apply_raft_snapshot[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ILLFORMAT_RAFT_ROW;
} // namespace ErrorCodes


static void writeRegionDataToStorage(
    Context & context, const RegionPtrWithBlock & region, RegionDataReadInfoList & data_list_read, Logger * log)
{
    constexpr auto FUNCTION_NAME = __FUNCTION__;
    const auto & tmt = context.getTMTContext();
    auto metrics = context.getTiFlashMetrics();
    TableID table_id = region->getMappedTableID();
    UInt64 region_decode_cost = -1, write_part_cost = -1;

    /// Declare lambda of atomic read then write to call multiple times.
    auto atomicReadWrite = [&](bool force_decode) {
        /// Get storage based on table ID.
        auto storage = tmt.getStorages().get(table_id);
        if (storage == nullptr || storage->isTombstone())
        {
            if (!force_decode) // Need to update.
                return false;
            if (storage == nullptr) // Table must have just been GC-ed.
                return true;
        }

        /// Lock throughout decode and write, during which schema must not change.
        TableStructureReadLockPtr lock;
        try
        {
            lock = storage->lockStructure(true, FUNCTION_NAME);
        }
        catch (DB::Exception & e)
        {
            // If the storage is physical dropped (but not removed from `ManagedStorages`) when we want to flsuh raft data into it, consider the write done.
            if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
                return true;
            else
                throw;
        }

        Block block;
        bool ok = false, need_decode = true;

        // try to use block cache if exists
        if (region.pre_decode_cache)
        {
            auto schema_version = storage->getTableInfo().schema_version;
            LOG_DEBUG(log, FUNCTION_NAME << ": " << region->toString() << " got pre-decode cache";
                      region.pre_decode_cache->toString(oss_internal_rare);
                      oss_internal_rare << ", storage schema version: " << schema_version);

            if (region.pre_decode_cache->schema_version == schema_version)
            {
                block = std::move(region.pre_decode_cache->block);
                ok = true;
                need_decode = false;
            }
            else
            {
                LOG_DEBUG(log, FUNCTION_NAME << ": schema version not equal, try to re-decode region cache into block");
                region.pre_decode_cache->block.clear();
            }
        }

        /// Read region data as block.
        Stopwatch watch;

        if (need_decode)
        {
            auto reader = RegionBlockReader(storage);
            std::tie(block, ok) = reader.read(data_list_read, force_decode);
            if (!ok)
                return false;
            region_decode_cost = watch.elapsedMilliseconds();
            GET_METRIC(metrics, tiflash_raft_write_data_to_storage_duration_seconds, type_decode).Observe(region_decode_cost / 1000.0);
        }

        /// Write block into storage.
        watch.restart();
        // Note: do NOT use typeid_cast, since Storage is multi-inherite and typeid_cast will return nullptr
        switch (storage->engineType())
        {
            case ::TiDB::StorageEngine::TMT:
            {

                auto tmt_storage = std::dynamic_pointer_cast<StorageMergeTree>(storage);
                TxnMergeTreeBlockOutputStream output(*tmt_storage);
                output.write(std::move(block));
                break;
            }
            case ::TiDB::StorageEngine::DT:
            {
                auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
                dm_storage->write(std::move(block), context.getSettingsRef());
                break;
            }
            default:
                throw Exception("Unknown StorageEngine: " + toString(static_cast<Int32>(storage->engineType())), ErrorCodes::LOGICAL_ERROR);
        }
        write_part_cost = watch.elapsedMilliseconds();
        GET_METRIC(metrics, tiflash_raft_write_data_to_storage_duration_seconds, type_write).Observe(write_part_cost / 1000.0);

        LOG_TRACE(log,
            FUNCTION_NAME << ": table " << table_id << ", region " << region->id() << ", cost [region decode " << region_decode_cost
                          << ", write part " << write_part_cost << "] ms");
        return true;
    };

    /// In TiFlash, the actions between applying raft log and schema changes are not strictly synchronized.
    /// There could be a chance that some raft logs come after a table gets tombstoned. Take care of it when
    /// decoding data. Check the test case for more details.
    FAIL_POINT_PAUSE(FailPoints::pause_before_apply_raft_cmd);

    /// Try read then write once.
    {
        if (atomicReadWrite(false))
            return;
    }

    /// If first try failed, sync schema and force read then write.
    {
        GET_METRIC(metrics, tiflash_schema_trigger_count, type_raft_decode).Increment();
        tmt.getSchemaSyncer()->syncSchemas(context);

        if (!atomicReadWrite(true))
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
    std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & range,
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

            range = meta_snap.range->rawKeys();
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
                return std::move(data_list_read);

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

    return std::move(data_list_read);
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
        return std::move(data_list_read);
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

void RegionTable::writeBlockByRegion(
    Context & context, const RegionPtrWithBlock & region, RegionDataReadInfoList & data_list_to_remove, Logger * log, bool lock_region)
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

    writeRegionDataToStorage(context, region, *data_list_read, log);

    RemoveRegionCommitCache(region, *data_list_read, lock_region);

    /// Save removed data to outer.
    data_list_to_remove = std::move(*data_list_read);
}

RegionTable::ReadBlockByRegionRes RegionTable::readBlockByRegion(const TiDB::TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & column_names_to_read,
    const RegionPtr & region,
    RegionVersion region_version,
    RegionVersion conf_version,
    bool resolve_locks,
    Timestamp start_ts,
    const std::unordered_set<UInt64> * bypass_lock_ts,
    std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & range,
    RegionScanFilterPtr scan_filter)
{
    if (!region)
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": region is null", ErrorCodes::LOGICAL_ERROR);

    // Tiny optimization for queries that need only handle, tso, delmark.
    bool need_value = column_names_to_read.size() != 3;
    auto region_data_lock = resolveLocksAndReadRegionData(
        table_info.id, region, start_ts, bypass_lock_ts, region_version, conf_version, range, resolve_locks, need_value);

    return std::visit(variant_op::overloaded{
                          [&](RegionDataReadInfoList & data_list_read) -> ReadBlockByRegionRes {
                              /// Read region data as block.
                              Block block;
                              {
                                  bool ok = false;
                                  auto reader = RegionBlockReader(table_info, columns);
                                  std::tie(block, ok) = reader.setStartTs(start_ts)
                                                            .setFilter(scan_filter)
                                                            .read(column_names_to_read, data_list_read, /*force_decode*/ true);
                                  if (!ok)
                                      // TODO: Enrich exception message.
                                      throw Exception("Read region " + std::to_string(region->id()) + " of table "
                                              + std::to_string(table_info.id) + " failed",
                                          ErrorCodes::LOGICAL_ERROR);
                              }
                              return block;
                          },
                          [&](LockInfoPtr & lock_value) -> ReadBlockByRegionRes {
                              assert(lock_value);
                              throw LockException(region->id(), std::move(lock_value));
                          },
                          [](RegionException::RegionReadStatus & s) -> ReadBlockByRegionRes { return s; },
                      },
        region_data_lock);
}

RegionTable::ResolveLocksAndWriteRegionRes RegionTable::resolveLocksAndWriteRegion(TMTContext & tmt,
    const TiDB::TableID table_id,
    const RegionPtr & region,
    const Timestamp start_ts,
    const std::unordered_set<UInt64> * bypass_lock_ts,
    RegionVersion region_version,
    RegionVersion conf_version,
    std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & range,
    Logger * log)
{
    auto region_data_lock = resolveLocksAndReadRegionData(table_id,
        region,
        start_ts,
        bypass_lock_ts,
        region_version,
        conf_version,
        range,
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

/// pre-decode region data into block cache and remove
RegionPtrWithBlock::CachePtr GenRegionPreDecodeBlockData(const RegionPtr & region, Context & context)
{
    const auto & tmt = context.getTMTContext();
    {
        Timestamp gc_safe_point = UINT64_MAX;
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
            LOG_WARNING(&Logger::get(__PRETTY_FUNCTION__),
                "Got error while reading region committed cache: " << e.displayText() << ". Skip pre-decode and keep original cache.");
            // set data_list_read and let apply snapshot process use empty block
            data_list_read = RegionDataReadInfoList();
        }
        else
            throw;
    }

    auto metrics = context.getTiFlashMetrics();
    TableID table_id = region->getMappedTableID();
    Int64 schema_version = DEFAULT_UNSPECIFIED_SCHEMA_VERSION;
    Block res_block;

    const auto atomicDecode = [&](bool force_decode) -> bool {
        Stopwatch watch;
        auto storage = tmt.getStorages().get(table_id);
        if (storage == nullptr || storage->isTombstone())
        {
            if (!force_decode) // Need to update.
                return false;
            if (storage == nullptr) // Table must have just been GC-ed.
                return true;
        }

        /// Lock throughout decode and write, during which schema must not change.
        TableStructureReadLockPtr lock;
        try
        {
            lock = storage->lockStructure(false, __PRETTY_FUNCTION__);
        }
        catch (DB::Exception & e)
        {
            // If the storage is physical dropped (but not removed from `ManagedStorages`) when we want to decode snapshot, consider the decode done.
            if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
                return true;
            else
                throw;
        }
        auto reader = RegionBlockReader(storage);
        auto [block, ok] = reader.read(*data_list_read, force_decode);
        if (!ok)
            return false;
        schema_version = storage->getTableInfo().schema_version;
        res_block = std::move(block);
        GET_METRIC(metrics, tiflash_raft_write_data_to_storage_duration_seconds, type_decode).Observe(watch.elapsedSeconds());
        return true;
    };

    /// In TiFlash, the actions between applying raft log and schema changes are not strictly synchronized.
    /// There could be a chance that some raft logs come after a table gets tombstoned. Take care of it when
    /// decoding data. Check the test case for more details.
    FAIL_POINT_PAUSE(FailPoints::pause_before_apply_raft_snapshot);

    if (!atomicDecode(false))
    {
        GET_METRIC(metrics, tiflash_schema_trigger_count, type_raft_decode).Increment();
        tmt.getSchemaSyncer()->syncSchemas(context);

        if (!atomicDecode(true))
            throw Exception("Pre-decode " + region->toString() + " cache to table " + std::to_string(table_id) + " block failed",
                ErrorCodes::LOGICAL_ERROR);
    }

    RemoveRegionCommitCache(region, *data_list_read);

    return std::make_unique<RegionPreDecodeBlockData>(std::move(res_block), schema_version, std::move(*data_list_read));
}

} // namespace DB
