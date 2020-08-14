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

void writeRegionDataToStorage(Context & context, const RegionPtr & region, RegionDataReadInfoList & data_list_read, Logger * log)
{
    const auto & tmt = context.getTMTContext();
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
        auto lock = storage->lockStructure(true, __PRETTY_FUNCTION__);

        /// Read region data as block.
        auto start_time = Clock::now();
        auto [block, ok] = readRegionBlock(storage->getTableInfo(),
            storage->getColumns(),
            storage->getColumns().getNamesOfPhysical(),
            data_list_read,
            std::numeric_limits<Timestamp>::max(),
            force_decode);
        if (!ok)
            return false;
        region_decode_cost = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_time).count();

        /// Write block into storage.
        start_time = Clock::now();
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
        write_part_cost = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_time).count();

        return true;
    };

    /// Try read then write once.
    {
        if (atomicReadWrite(false))
            return;
    }

    /// If first try failed, sync schema and force read then write.
    {
        tmt.getSchemaSyncer()->syncSchemas(context);

        if (!atomicReadWrite(true))
            // Failure won't be tolerated this time.
            // TODO: Enrich exception message.
            throw Exception("Write region " + std::to_string(region->id()) + " to table " + std::to_string(table_id) + " failed",
                ErrorCodes::LOGICAL_ERROR);
    }

    LOG_TRACE(log,
        __FUNCTION__ << ": table " << table_id << ", region " << region->id() << ", cost [region decode " << region_decode_cost
                     << ", write part " << write_part_cost << "] ms");
}

std::pair<RegionDataReadInfoList, RegionException::RegionReadStatus> resolveLocksAndReadRegionData(const TiDB::TableID table_id,
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
                return {{}, RegionException::NOT_FOUND};

            const auto & [version, conf_ver, key_range] = region->dumpVersionRange();
            if (version != region_version || conf_ver != conf_version)
                return {{}, RegionException::VERSION_ERROR};

            // todo check table id
            TableID mapped_table_id;
            if (!computeMappedTableID(*key_range->rawKeys().first, mapped_table_id) || mapped_table_id != table_id)
                throw Exception("Should not happen, region not belong to table: table id in region is " + std::to_string(mapped_table_id)
                        + ", expected table id is " + std::to_string(table_id),
                    ErrorCodes::LOGICAL_ERROR);

            range = key_range->rawKeys();
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
                return {{}, RegionException::OK};

            data_list_read.reserve(scanner.writeMapSize());

            // Tiny optimization for queries that need only handle, tso, delmark.
            do
            {
                data_list_read.emplace_back(scanner.next(need_data_value));
            } while (scanner.hasNext());
        }
    }
    return {std::move(data_list_read), RegionException::OK};
}

void RegionTable::writeBlockByRegion(
    Context & context, const RegionPtr & region, RegionDataReadInfoList & data_list_to_remove, Logger * log, bool lock_region)
{
    RegionDataReadInfoList data_list_read;
    {
        auto scanner = region->createCommittedScanner(lock_region);

        /// Some sanity checks for region meta.
        {
            if (region->isPendingRemove())
                return;
        }

        /// Read raw KVs from region cache.
        {
            // Shortcut for empty region.
            if (!scanner.hasNext())
                return;

            data_list_read.reserve(scanner.writeMapSize());

            do
            {
                data_list_read.emplace_back(scanner.next());
            } while (scanner.hasNext());
        }
    }

    writeRegionDataToStorage(context, region, data_list_read, log);

    /// Remove data in region.
    {
        auto remover = region->createCommittedRemover(lock_region);
        for (const auto & [handle, write_type, commit_ts, value] : data_list_read)
        {
            std::ignore = write_type;
            std::ignore = value;

            remover.remove({handle, commit_ts});
        }
    }

    /// Save removed data to outer.
    data_list_to_remove = std::move(data_list_read);
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
    std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & range,
    RegionScanFilterPtr scan_filter)
{
    if (!region)
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": region is null", ErrorCodes::LOGICAL_ERROR);

    // Tiny optimization for queries that need only handle, tso, delmark.
    bool need_value = column_names_to_read.size() != 3;
    auto [data_list_read, read_status] = resolveLocksAndReadRegionData(
        table_info.id, region, start_ts, bypass_lock_ts, region_version, conf_version, range, resolve_locks, need_value);
    if (read_status != RegionException::OK)
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

    return {std::move(block), RegionException::OK};
}

RegionException::RegionReadStatus RegionTable::resolveLocksAndWriteRegion(TMTContext & tmt,
    const TiDB::TableID table_id,
    const RegionPtr & region,
    const Timestamp start_ts,
    const std::unordered_set<UInt64> * bypass_lock_ts,
    RegionVersion region_version,
    RegionVersion conf_version,
    std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & range,
    Logger * log)
{
    auto [data_list_read, read_status] = resolveLocksAndReadRegionData(table_id,
        region,
        start_ts,
        bypass_lock_ts,
        region_version,
        conf_version,
        range,
        /* resolve_locks */ true,
        /* need_data_value */ true);
    if (read_status != RegionException::OK)
        return read_status;

    auto & context = tmt.getContext();
    writeRegionDataToStorage(context, region, data_list_read, log);

    /// Remove committed data
    {
        auto remover = region->createCommittedRemover();
        for (const auto & [handle, write_type, commit_ts, value] : data_list_read)
        {
            std::ignore = write_type;
            std::ignore = value;

            remover.remove({handle, commit_ts});
        }
    }

    return RegionException::OK;
}

} // namespace DB
