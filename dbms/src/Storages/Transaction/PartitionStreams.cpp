#include <Core/Block.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

namespace DB
{

using BlockOption = std::optional<Block>;

std::tuple<BlockOption, RegionTable::RegionReadStatus> RegionTable::getBlockInputStreamByRegion(TMTContext & tmt,
    TableID table_id,
    const RegionID region_id,
    const TiDB::TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & ordered_columns,
    RegionDataReadInfoList & data_list_for_remove)
{
    return getBlockInputStreamByRegion(table_id,
        tmt.kvstore->getRegion(region_id),
        InvalidRegionVersion,
        InvalidRegionVersion,
        table_info,
        columns,
        ordered_columns,
        false,
        false,
        0,
        &data_list_for_remove);
}

std::tuple<BlockOption, RegionTable::RegionReadStatus> RegionTable::getBlockInputStreamByRegion(TableID table_id,
    RegionPtr region,
    const RegionVersion region_version,
    const RegionVersion conf_version,
    const TiDB::TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & ordered_columns,
    bool learner_read,
    bool resolve_locks,
    Timestamp start_ts,
    RegionDataReadInfoList * data_list_for_remove)
{
    if (!region)
        return {BlockOption{}, NOT_FOUND};

    if (learner_read)
        region->waitIndex(region->learnerRead());

    auto schema_fetcher = [&](TableID) {
        return std::make_tuple<const TiDB::TableInfo *, const ColumnsDescription *, const Names *>(&table_info, &columns, &ordered_columns);
    };

    {
        RegionDataReadInfoList data_list;

        {
            auto scanner = region->createCommittedScanner(table_id);

            if (region->isPendingRemove())
                return {BlockOption{}, PENDING_REMOVE};

            if (region_version != InvalidRegionVersion && (region->version() != region_version || region->confVer() != conf_version))
                return {BlockOption{}, VERSION_ERROR};

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
                data_list.emplace_back(scanner->next());
            } while (scanner->hasNext());
        }

        const auto [table_info, columns, ordered_columns] = schema_fetcher(table_id);
        auto block = RegionBlockRead(*table_info, *columns, *ordered_columns, data_list);

        if (data_list_for_remove)
            *data_list_for_remove = std::move(data_list);

        return {block, OK};
    }
}

} // namespace DB
