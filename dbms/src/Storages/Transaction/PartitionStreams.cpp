#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

using BlockOption = std::optional<Block>;

void RegionTable::writeBlockByRegion(Context & context, TableID table_id, RegionPtr region, RegionDataReadInfoList & data_list_to_remove)
{
    // TODO: Logging.

    const auto & tmt = context.getTMTContext();

    /// Read raw KVs from region cache.
    RegionDataReadInfoList data_list_read;
    {
        auto scanner = region->createCommittedScanner(table_id);
        // Shortcut for empty region.
        if (!scanner->hasNext())
            return;
        do
        {
            data_list_read.emplace_back(scanner->next());
        } while (scanner->hasNext());
    }


    /// Declare lambda of atomic read then write to call multiple times.
    auto atomicReadWrite = [&](bool force_decode) {
        /// Get storage based on table ID.
        auto storage = tmt.getStorages().get(table_id);
        if (storage == nullptr)
        {
            if (!force_decode) // Need to update.
                return false;
            // Table must have just been dropped or truncated.
            // TODO: What if we support delete range? Do we still want to remove KVs from region cache?
            data_list_to_remove = std::move(data_list_read);
            return true;
        }

        /// Lock throughout decode and write, during which schema must not change.
        auto lock = storage->lockStructure(true, __PRETTY_FUNCTION__);

        /// Read region data as block.
        auto [block, ok] = readRegionBlock(storage->getTableInfo(),
            storage->getColumns(),
            storage->getColumns().getNamesOfPhysical(),
            data_list_read,
            std::numeric_limits<Timestamp>::max(),
            force_decode);
        if (!ok)
            return false;

        /// Write block into storage.
        TxnMergeTreeBlockOutputStream output(*storage);
        output.write(block);

        /// Move read data to outer to remove.
        data_list_to_remove = std::move(data_list_read);

        return true;
    };

    /// Try read then write once.
    if (atomicReadWrite(false))
        return;

    /// If first try failed, sync schema and force read then write.
    tmt.getSchemaSyncer()->syncSchemas(context);

    if (!atomicReadWrite(true))
        // Failure won't be tolerated this time.
        // TODO: Enrich exception message.
        throw Exception(
            "Write region " + std::to_string(region->id()) + " to table " + std::to_string(table_id) + " failed", ErrorCodes::LOGICAL_ERROR);
}

std::tuple<std::optional<Block>, RegionTable::RegionReadStatus> RegionTable::getBlockByRegion(const TiDB::TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & column_names_to_read,
    const RegionPtr & region,
    RegionVersion region_version,
    RegionVersion conf_version,
    bool resolve_locks,
    Timestamp start_ts)
{
    // TODO: Logging.

    if (!region)
        return {BlockOption{}, NOT_FOUND};

    auto scanner = region->createCommittedScanner(table_info.id);

    /// Blocking learner read.
    {
        region->waitIndex(region->learnerRead());
    }

    /// Some sanity checks for region meta.
    {
        if (region->isPendingRemove())
            return {BlockOption{}, PENDING_REMOVE};

        if (region_version != InvalidRegionVersion && (region->version() != region_version || region->confVer() != conf_version))
            return {BlockOption{}, VERSION_ERROR};
    }

    /// Deal with locks.
    {
        if (resolve_locks)
        {
            LockInfoPtr lock_info = scanner->getLockInfo(start_ts);
            if (lock_info)
            {
                LockInfos lock_infos;
                lock_infos.emplace_back(std::move(lock_info));
                throw LockException(std::move(lock_infos));
            }
        }
    }

    /// Read raw KVs from region cache.
    RegionDataReadInfoList data_list_read;
    {
        // Shortcut for empty region.
        if (!scanner->hasNext())
            return {BlockOption{}, OK};
        // Tiny optimization for queries that need only handle, tso, delmark.
        bool need_value = column_names_to_read.size() != 3;
        do
        {
            data_list_read.emplace_back(scanner->next(need_value));
        } while (scanner->hasNext());
    }

    /// Read region data as block.
    auto [block, ok] = readRegionBlock(table_info, columns, column_names_to_read, data_list_read, start_ts, true);
    if (!ok)
        // TODO: Enrich exception message.
        throw Exception("Read region " + std::to_string(region->id()) + " of table " + std::to_string(table_info.id) + " failed",
            ErrorCodes::LOGICAL_ERROR);

    return {block, OK};
}

} // namespace DB
