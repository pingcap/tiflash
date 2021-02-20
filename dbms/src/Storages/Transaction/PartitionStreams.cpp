#include <Common/TiFlashMetrics.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/StorageDebugging.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

std::tuple<Block, bool> readRegionBlock(const ManageableStoragePtr & storage, RegionDataReadInfoList & data_list, bool force_decode)
{
    return readRegionBlock(storage->getTableInfo(),
        storage->getColumns(),
        storage->getColumns().getNamesOfPhysical(),
        data_list,
        std::numeric_limits<Timestamp>::max(),
        force_decode,
        nullptr);
}

static void writeRegionDataToStorage(Context & context, const RegionPtrWrap & region, RegionDataReadInfoList & data_list_read, Logger * log)
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
            // Table must have just been dropped or truncated.
            return true;
        }

        /// Lock throughout decode and write, during which schema must not change.
        auto lock = storage->lockStructure(true, FUNCTION_NAME);

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
            std::tie(block, ok) = readRegionBlock(storage, data_list_read, force_decode);
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
            case ::TiDB::StorageEngine::DEBUGGING_MEMORY:
            {
                auto debugging_storage = std::dynamic_pointer_cast<StorageDebugging>(storage);
                ASTPtr query(new ASTInsertQuery(debugging_storage->getDatabaseName(), debugging_storage->getTableName(), true));
                BlockOutputStreamPtr output = debugging_storage->write(query, context.getSettingsRef());
                output->writePrefix();
                output->write(block);
                output->writeSuffix();
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

std::pair<RegionDataReadInfoList, RegionException::RegionReadStatus> resolveLocksAndReadRegionData(const TiDB::TableID table_id,
    const RegionPtr & region,
    const Timestamp start_ts,
    const std::unordered_set<UInt64> * bypass_lock_ts,
    RegionVersion region_version,
    RegionVersion conf_version,
    DB::HandleRange<HandleID> & handle_range,
    bool resolve_locks,
    bool need_data_value)
{
    RegionDataReadInfoList data_list_read;
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
                return {{}, RegionException::RegionReadStatus::NOT_FOUND};

            const auto & meta_snap = region->dumpRegionMetaSnapshot();
            if (meta_snap.ver != region_version || meta_snap.conf_ver != conf_version)
                return {{}, RegionException::RegionReadStatus::EPOCH_NOT_MATCH};

            handle_range = meta_snap.range->getHandleRangeByTable(table_id);
        }

        /// Deal with locks.
        if (resolve_locks)
        {
            /// Check if there are any lock should be resolved, if so, throw LockException.
            if (LockInfoPtr lock_info = scanner.getLockInfo(RegionLockReadQuery{.read_tso = start_ts, .bypass_lock_ts = bypass_lock_ts});
                lock_info)
            {
                LockInfos lock_infos;
                lock_infos.emplace_back(std::move(lock_info));
                throw LockException(region->id(), std::move(lock_infos));
            }
        }

        /// Read raw KVs from region cache.
        {
            // Shortcut for empty region.
            if (!scanner.hasNext())
                return {{}, RegionException::RegionReadStatus::OK};

            data_list_read.reserve(scanner.writeMapSize());

            // Tiny optimization for queries that need only handle, tso, delmark.
            do
            {
                data_list_read.emplace_back(scanner.next(need_data_value));
            } while (scanner.hasNext());
        }
    }
    return {std::move(data_list_read), RegionException::RegionReadStatus::OK};
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
    Context & context, const RegionPtrWrap & region, RegionDataReadInfoList & data_list_to_remove, Logger * log, bool lock_region)
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

std::tuple<Block, RegionException::RegionReadStatus> RegionTable::readBlockByRegion(const TiDB::TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & column_names_to_read,
    const RegionPtr & region,
    RegionVersion region_version,
    RegionVersion conf_version,
    bool resolve_locks,
    Timestamp start_ts,
    const std::unordered_set<UInt64> * bypass_lock_ts,
    DB::HandleRange<HandleID> & handle_range,
    RegionScanFilterPtr scan_filter)
{
    if (!region)
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": region is null", ErrorCodes::LOGICAL_ERROR);

    // Tiny optimization for queries that need only handle, tso, delmark.
    bool need_value = column_names_to_read.size() != 3;
    auto [data_list_read, read_status] = resolveLocksAndReadRegionData(
        table_info.id, region, start_ts, bypass_lock_ts, region_version, conf_version, handle_range, resolve_locks, need_value);
    if (read_status != RegionException::RegionReadStatus::OK)
        return {Block(), read_status};

    /// Read region data as block.
    Block block;
    {
        bool ok = false;
        std::tie(block, ok) = readRegionBlock(table_info, columns, column_names_to_read, data_list_read, start_ts, true, scan_filter);
        if (!ok)
            // TODO: Enrich exception message.
            throw Exception("Read region " + std::to_string(region->id()) + " of table " + std::to_string(table_info.id) + " failed",
                ErrorCodes::LOGICAL_ERROR);
    }

    return {std::move(block), RegionException::RegionReadStatus::OK};
}

RegionException::RegionReadStatus RegionTable::resolveLocksAndWriteRegion(TMTContext & tmt,
    const TiDB::TableID table_id,
    const RegionPtr & region,
    const Timestamp start_ts,
    const std::unordered_set<UInt64> * bypass_lock_ts,
    RegionVersion region_version,
    RegionVersion conf_version,
    DB::HandleRange<HandleID> & handle_range,
    Logger * log)
{
    auto [data_list_read, read_status] = resolveLocksAndReadRegionData(table_id,
        region,
        start_ts,
        bypass_lock_ts,
        region_version,
        conf_version,
        handle_range,
        /* resolve_locks */ true,
        /* need_data_value */ true);
    if (read_status != RegionException::RegionReadStatus::OK)
        return read_status;

    if (data_list_read.empty())
        return read_status;

    auto & context = tmt.getContext();
    writeRegionDataToStorage(context, region, data_list_read, log);

    RemoveRegionCommitCache(region, data_list_read);

    return RegionException::RegionReadStatus::OK;
}

/// pre-decode region data into block cache and remove
RegionPtrWrap::CachePtr GenRegionPreDecodeBlockData(const RegionPtr & region, Context & context)
{
    auto data_list_read = ReadRegionCommitCache(region);

    if (!data_list_read)
        return nullptr;

    auto metrics = context.getTiFlashMetrics();
    const auto & tmt = context.getTMTContext();
    TableID table_id = region->getMappedTableID();
    Int64 schema_version = DEFAULT_UNSPECIFIED_SCHEMA_VERSION;
    Block res_block;

    const auto atomicDecode = [&](bool force_decode) -> bool {
        Stopwatch watch;
        auto storage = tmt.getStorages().get(table_id);
        if (storage == nullptr || storage->isTombstone())
        {
            if (!force_decode)
                return false;
            return true;
        }
        auto lock = storage->lockStructure(false, __PRETTY_FUNCTION__);
        auto [block, ok] = readRegionBlock(storage, *data_list_read, force_decode);
        if (!ok)
            return false;
        schema_version = storage->getTableInfo().schema_version;
        res_block = std::move(block);
        GET_METRIC(metrics, tiflash_raft_write_data_to_storage_duration_seconds, type_decode).Observe(watch.elapsedSeconds());
        return true;
    };

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
