#include <Core/Block.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>
#include <common/logger_useful.h>

namespace DB
{

RegionTable::BlockOption RegionTable::getBlockInputStreamByRegion(TableID table_id,
    RegionPtr region,
    const TiDB::TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & ordered_columns,
    RegionDataReadInfoList & data_list_for_remove)
{
    if (!region)
        throw Exception("Region is nullptr, should not happen", ErrorCodes::LOGICAL_ERROR);

    bool need_value = true;

    if (ordered_columns.size() == 3)
        need_value = false;

    auto start_time = Clock::now();

    {
        auto scanner = region->createCommittedScanner(table_id);

        if (region->isPendingRemove())
            return BlockOption{};

        if (!scanner->hasNext())
            return BlockOption{};

        do
        {
            data_list_for_remove.emplace_back(scanner->next(need_value));
        } while (scanner->hasNext());
    }

    const auto scan_cost = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_time).count();
    start_time = Clock::now();

    auto block = RegionBlockRead(table_info, columns, ordered_columns, data_list_for_remove);

    auto compute_cost = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_time).count();

    LOG_TRACE(log,
        region->toString(false) << " read " << data_list_for_remove.size() << " rows, cost [scan " << scan_cost << ", compute "
                                << compute_cost << "] ms");
    return block;
}

std::tuple<RegionTable::BlockOption, RegionTable::RegionReadStatus> RegionTable::getBlockInputStreamByRegion(TableID table_id,
    RegionPtr region,
    const RegionVersion region_version,
    const RegionVersion conf_version,
    const TiDB::TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & ordered_columns,
    bool resolve_locks,
    Timestamp start_ts,
    DB::HandleRange<HandleID> & handle_range)
{
    if (!region)
        throw Exception("Region is nullptr, should not happen", ErrorCodes::LOGICAL_ERROR);

    RegionDataReadInfoList data_list;
    bool need_value = true;

    if (ordered_columns.size() == 3)
        need_value = false;

    {
        auto scanner = region->createCommittedScanner(table_id);

        if (region->isPendingRemove())
            return {BlockOption{}, PENDING_REMOVE};

        const auto & [version, conf_ver, key_range] = region->dumpVersionRangeByTable();
        if (version != region_version || conf_ver != conf_version)
            return {BlockOption{}, VERSION_ERROR};

        handle_range = TiKVRange::getHandleRangeByTable(key_range, table_id);

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

        if (!scanner->hasNext())
            return {BlockOption{}, OK};

        do
        {
            data_list.emplace_back(scanner->next(need_value));
        } while (scanner->hasNext());
    }

    auto block = RegionBlockRead(table_info, columns, ordered_columns, data_list);

    return {std::move(block), OK};
}

} // namespace DB
