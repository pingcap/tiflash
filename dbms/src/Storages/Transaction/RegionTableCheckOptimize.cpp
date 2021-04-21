#include <Core/TMTPKType.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/TMTDataPartProperty.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/CHTableHandle.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>

#include <DataStreams/PKColumnIterator.hpp>

namespace DB
{
namespace ErrorCodes
{
extern const int TABLE_IS_DROPPED;
}

template <typename TargetType>
std::tuple<size_t, size_t> shouldOptimizeTable(const MergeTreeData & data)
{
    size_t selected_marks = 0, overlapped_marks = 0;
    auto data_parts = data.getDataParts();

    if (data_parts.empty())
        return {0, 0};

    using PartMap = std::unordered_map<const MergeTreeDataPart *, size_t>;
    std::map<TargetType, PartMap> mmp;

    for (auto it = data_parts.begin(); it != data_parts.end();)
    {
        if ((*it)->isEmpty())
            it = data_parts.erase(it);
        else
            it++;
    }

    for (const auto & part : data_parts)
    {
        const auto & tmt_property = *part->tmt_property;
        mmp.emplace(tmt_property.min_pk.get<TargetType>(), PartMap{});
        mmp.emplace(tmt_property.max_pk.get<TargetType>(), PartMap{});
    }

    mmp.emplace(std::numeric_limits<TargetType>::max(), PartMap{});

    for (const auto & part : data_parts)
    {
        const auto & tmt_property = *part->tmt_property;
        const auto pk_index = part->index[0].get();
        size_t marks_count = pk_index->size();

        auto it_begin = mmp.find(tmt_property.min_pk.get<TargetType>());
        auto it_end = mmp.find(tmt_property.max_pk.get<TargetType>());

        for (auto it = it_begin; it != it_end; ++it)
        {
            auto it_next = it;
            ++it_next;

            TargetType left = it->first, right = it_next->first;

            {
                size_t pos_start
                    = std::lower_bound(PKColumnIterator(0, pk_index), PKColumnIterator(marks_count, pk_index), left, PkCmp<TargetType>).pos;
                size_t pos_end
                    = std::upper_bound(PKColumnIterator(0, pk_index), PKColumnIterator(marks_count, pk_index), right, PkCmp<TargetType>)
                          .pos;
                if (pos_end > pos_start)
                    it->second[part.get()] = pos_end - pos_start;
            }
        }
    }

    std::vector<size_t> cnt_list;
    for (auto it = mmp.begin(); it != mmp.end(); ++it)
    {
        const auto & part_info = it->second;
        if (part_info.empty())
            continue;

        if (part_info.size() == 1)
        {
            selected_marks += part_info.begin()->second;
            continue;
        }

        cnt_list.clear();
        for (const auto & info : part_info)
            cnt_list.push_back(info.second);
        std::sort(cnt_list.begin(), cnt_list.end());
        selected_marks += *(cnt_list.end() - 1);
        overlapped_marks += *(cnt_list.end() - 2);
    }

    return {selected_marks, overlapped_marks};
}

bool shouldOptimizeTable(const TableID table_id, TMTContext & tmt, Logger * log, const double threshold)
{
    const auto storage = tmt.getStorages().get(table_id);
    if (!storage)
        return false;
    try
    {
        auto lock = storage->lockStructureForShare(RWLock::NO_QUERY);
    }
    catch (DB::Exception & e)
    {
        // We can ignore if storage is dropped.
        if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
            return false;
        else
            throw;
    }

    if (storage->engineType() != TiDB::StorageEngine::TMT)
        return false;

    // Only for TMT
    auto tmt_storage = std::dynamic_pointer_cast<StorageMergeTree>(storage);
    auto & data = tmt_storage->getData();
    const bool pk_is_uint64 = getTMTPKType(*data.primary_key_data_types[0]) == TMTPKType::UINT64;
    auto [selected_marks, overlapped_marks] = pk_is_uint64 ? shouldOptimizeTable<UInt64>(data) : shouldOptimizeTable<Int64>(data);

    LOG_INFO(log,
        __FUNCTION__ << ": `" << storage->getDatabaseName() << "`.`" << storage->getTableName() << "` table id " << table_id
                     << " overlapped marks " << overlapped_marks << ", selected marks " << selected_marks << ", threshold " << threshold);

    if (selected_marks && overlapped_marks > selected_marks * threshold)
        return true;
    return false;
}

void RegionTable::checkTableOptimize()
{
    static const Seconds TABLE_CHECK_PERIOD(5 * 60); // 5m
    {
        std::lock_guard<std::mutex> lock(table_checker.mutex);
        if (table_checker.is_checking)
            return;
        if (Clock::now() - table_checker.last_check_time < TABLE_CHECK_PERIOD)
            return;
        table_checker.is_checking = true;
    }

    std::vector<TableID> table_to_check;
    {
        std::lock_guard<std::mutex> lock(mutex);
        for (const auto & table : tables)
        {
            if (!table.second.regions.empty())
                table_to_check.emplace_back(table.first);
        }
    }

    for (const auto table_id : table_to_check)
        checkTableOptimize(table_id, table_checker.OVERLAP_THRESHOLD);

    {
        std::lock_guard<std::mutex> lock(table_checker.mutex);
        table_checker.is_checking = false;
        table_checker.last_check_time = Clock::now();
    }
}

void RegionTable::checkTableOptimize(DB::TableID table_id, const double threshold)
{
    auto & tmt = context->getTMTContext();
    bool should_optimize = false;
    try
    {
        should_optimize = shouldOptimizeTable(table_id, tmt, log, threshold);
    }
    catch (...)
    {
    }

    if (should_optimize)
    {
        LOG_INFO(log, "table " << table_id << " need to be optimized");
        std::lock_guard<std::mutex> lock(mutex);
        table_to_optimize.insert(table_id);
    }
}

void RegionTable::setTableCheckerThreshold(double overlap_threshold) { table_checker.OVERLAP_THRESHOLD = overlap_threshold; }

} // namespace DB
