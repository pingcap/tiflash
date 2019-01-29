#pragma once

#include <Common/typeid_cast.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnsNumber.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class RegionConcatedScanRemover
{
public:
    RegionConcatedScanRemover(Regions && regions_, TableID table_id, bool remove_on_read_, bool resolve_locks_, UInt64 start_ts_)
        : regions(std::move(regions_)), expected_table_id(table_id), remove_on_read(remove_on_read_), resolve_locks(resolve_locks_), start_ts(start_ts_)
    {
        if (regions.empty())
            throw Exception("empty regions", ErrorCodes::LOGICAL_ERROR);

        std::sort(regions.begin(), regions.end(), [](const RegionPtr & r1, const RegionPtr & r2) {
            return r1->getRange() < r2->getRange();
        });

        curr_region_index = 0;
        curr_region_id = regions[curr_region_index]->id();
        curr_scanner = regions[0]->createCommittedScanRemover(InvalidTableID);

        if (!remove_on_read && resolve_locks) curr_lock_info = curr_scanner->getLockInfo(expected_table_id, start_ts);
    }

    // return {InvalidTableID, InvalidRegionID} when no more data
    std::tuple<TableID, RegionID, Region::LockInfoPtr> hasNext()
    {
        if (curr_lock_info)
        {
            // lock seen, will ignore data in the rest regions, and find next lock.
            ever_seen_lock = true;
            auto lock_info = std::move(curr_lock_info);

            while ((++curr_region_index) < regions.size())
            {
                curr_scanner = regions[curr_region_index]->createCommittedScanRemover(expected_table_id);
                curr_region_id = regions[curr_region_index]->id();
                if ((curr_lock_info = curr_scanner->getLockInfo(expected_table_id, start_ts)) != nullptr)
                    break;
            }

            return std::make_tuple(expected_table_id, curr_region_id, std::move(lock_info));
        }
        else if (ever_seen_lock)
        {
            // Ever seen lock and no other lock left, finish.
            return std::make_tuple(InvalidTableID, InvalidRegionID, nullptr);
        }

        auto table_id = curr_scanner->hasNext();

        if (table_id != InvalidTableID)
        {
            if (curr_table_id == InvalidTableID)
                curr_table_id = table_id;
            if (remove_on_read && table_id != curr_table_id)
                curr_scanner->remove(curr_table_id);

            curr_table_id = table_id;
            return std::make_tuple(table_id, curr_region_id, nullptr);
        }
        else
        {
            if (remove_on_read && curr_table_id != InvalidTableID)
                curr_scanner->remove(curr_table_id);

            // end of current region, go to next.
            while ((++curr_region_index) < regions.size())
            {
                curr_scanner = regions[curr_region_index]->createCommittedScanRemover(expected_table_id);
                curr_region_id = regions[curr_region_index]->id();

                // check lock before checking data.
                if (resolve_locks && (curr_lock_info = curr_scanner->getLockInfo(expected_table_id, start_ts)) != nullptr)
                    return std::make_tuple(expected_table_id, curr_region_id, nullptr);

                if ((curr_table_id = curr_scanner->hasNext()) != InvalidTableID)
                    return std::make_tuple(curr_table_id, curr_region_id, nullptr);
            }
            return std::make_tuple(InvalidTableID, InvalidRegionID, nullptr);
        }
    }

    auto next() { return curr_scanner->next(); }

private:
    Regions regions;
    TableID expected_table_id;
    bool remove_on_read;
    bool resolve_locks;
    UInt64 start_ts;

    size_t curr_region_index = 0;
    RegionID curr_region_id = InvalidRegionID;
    TableID curr_table_id = InvalidTableID;

    using ScannerPtr = std::unique_ptr<Region::CommittedScanRemover>;

    ScannerPtr curr_scanner;

    Region::LockInfoPtr curr_lock_info = nullptr;
    bool ever_seen_lock = false;
};

// FIXME: remove_on_read is not safe because cache will be remove before data flush into storage.
// Please design another mechanism to gracefully remove cache after flush.
class RegionBlockReader
{
public:
    using SchemaFetcher = std::function<std::tuple<const TiDB::TableInfo *, const ColumnsDescription *, const Names *>(TableID)>;

    RegionBlockReader(SchemaFetcher schema_fetcher_, Regions && regions, TableID table_id, bool remove_on_read, bool resolve_locks, UInt64 start_ts)
        : schema_fetcher(schema_fetcher_), scanner(std::move(regions), table_id, remove_on_read, resolve_locks, start_ts)
    {
    }

    std::tuple<Block, TableID, RegionID, Region::LockInfoPtr> next()
    {
        auto [table_id, region_id, lock_info] = scanner.hasNext();
        if (lock_info)
        {
            return {Block(), table_id, region_id, std::move(lock_info)};
        }
        else if (table_id != InvalidTableID)
        {
            const auto [table_info, columns, ordered_columns] = schema_fetcher(table_id);
            auto block = read(region_id, *table_info, *columns, *ordered_columns, scanner);
            return {block, table_id, region_id, nullptr};
        }
        else
        {
            return {Block(), InvalidTableID, InvalidRegionID, nullptr};
        }
    }

private:
    Block read(RegionID my_region_id, const TiDB::TableInfo & table_info,
        const ColumnsDescription & columns, const Names & ordered_columns_, RegionConcatedScanRemover & scanner);

private:
    SchemaFetcher schema_fetcher;
    RegionConcatedScanRemover scanner;
};

} // namespace DB
