#include <common/logger_useful.h>
#include <functional>

#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/PartitionDataMover.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>

#include <DataStreams/BlocksListBlockInputStream.h>

namespace DB
{

std::tuple<BlockInputStreamPtr, RegionTable::RegionReadStatus, size_t> RegionTable::getBlockInputStreamByRegion(
    TableID table_id,
    const RegionID region_id,
    const RegionVersion region_version,
    const TiDB::TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & ordered_columns,
    bool learner_read,
    bool resolve_locks,
    UInt64 start_ts,
    std::vector<TiKVKey> * keys)
{
    auto & kvstore = context.getTMTContext().kvstore;

    auto region = kvstore->getRegion(region_id);
    if (!region)
        return {nullptr, NOT_FOUND, 0};

    if (region_version != InvalidRegionVersion && region->version() != region_version)
        return {nullptr, VERSION_ERROR, 0};

    if (learner_read)
        region->wait_index(region->learner_read());

    auto schema_fetcher = [&](TableID) {
        return std::make_tuple<const TiDB::TableInfo *, const ColumnsDescription *, const Names *>(&table_info, &columns, &ordered_columns);
    };

    {
        auto scanner = region->createCommittedScanRemover(table_id);
        {
            Region::LockInfoPtr lock_info = nullptr;
            if (resolve_locks)
                lock_info = scanner->getLockInfo(table_id, start_ts);
            if (lock_info)
            {
                Region::LockInfos lock_infos;
                lock_infos.emplace_back(std::move(lock_info));
                throw LockException(std::move(lock_infos));
            }
        }

        auto next_table_id = scanner->hasNext();
        if (next_table_id == InvalidTableID)
            return {nullptr, OK, 0};

        const auto [table_info, columns, ordered_columns] = schema_fetcher(next_table_id);
        auto block = RegionBlockRead(*table_info, *columns, *ordered_columns, scanner, keys);

        size_t tol = block.rows();

        BlocksList blocks;
        blocks.emplace_back(std::move(block));

        return {std::make_shared<BlocksListBlockInputStream>(std::move(blocks)), OK, tol};
    }
}

} // namespace DB