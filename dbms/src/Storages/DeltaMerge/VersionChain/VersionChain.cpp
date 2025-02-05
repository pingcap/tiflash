// Copyright 2024 PingCAP, Inc.
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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/VersionChain/VersionChain.h>

#include <ranges>

namespace DB::DM
{

template <ExtraHandleType HandleType>
std::shared_ptr<const std::vector<RowID>> VersionChain<HandleType>::replaySnapshot(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot)
{
    // Check Stable: stable always has one DMFile.
    if (dmfile_or_delete_range_list.empty())
    {
        const auto & dmfiles = snapshot.stable->getDMFiles();
        RUNTIME_CHECK(dmfiles.size() == 1, dmfiles.size());
        dmfile_or_delete_range_list.push_back(
            DMFileHandleIndex<HandleType>{dm_context, dmfiles[0], /*start_row_id*/ 0, /*rowkey_range*/ std::nullopt});
    }

    const auto & stable = *(snapshot.stable);
    const UInt32 stable_rows = stable.getDMFilesRows();
    const auto & delta = *(snapshot.delta);
    const UInt32 delta_rows = delta.getRows();
    const UInt32 delta_delete_ranges = delta.getDeletes();
    std::lock_guard lock(mtx);
    if (delta_rows + delta_delete_ranges <= replayed_rows_and_deletes)
    {
        RUNTIME_CHECK(base_versions->size() >= delta_rows, base_versions->size(), delta_rows);
        return base_versions;
    }

    // Copy for write
    base_versions = std::make_shared<std::vector<RowID>>(*base_versions);
    const auto cfs = delta.getColumnFiles();
    const auto & data_provider = delta.getDataProvider();

    UInt32 skipped_rows_and_deletes = 0;
    auto pos = cfs.begin();
    for (; pos != cfs.end(); ++pos)
    {
        auto skip_n = (*pos)->isDeleteRange() ? (*pos)->getDeletes() : (*pos)->getRows();
        if (skip_n + skipped_rows_and_deletes > replayed_rows_and_deletes)
            break;
        skipped_rows_and_deletes += skip_n;
    }
    // `pos` points to the first ColumnFile that has records not been replayed.
    // `offset` points to the first records that has not been replayed in `pos`.
    auto offset = replayed_rows_and_deletes - skipped_rows_and_deletes;
    // Only ColumnFileInMemory or ColumnFileTiny can be half replayed.
    RUNTIME_CHECK(offset == 0 || (*pos)->isInMemoryFile() || (*pos)->isTinyFile(), offset, (*pos)->toString());

    const bool calculate_read_packs = (cfs.end() - pos == 1) && ((*pos)->isInMemoryFile() || (*pos)->isTinyFile())
        && dmfile_or_delete_range_list.size() == 1;
    const auto initial_replayed_rows_and_deletes = replayed_rows_and_deletes;
    SCOPE_EXIT({ cleanHandleColumn(); });

    auto delta_reader = DeltaValueReader(
        dm_context,
        snapshot.delta,
        getHandleColumnDefinesPtr<HandleType>(),
        /*range*/ {},
        ReadTag::MVCC);

    for (; pos != cfs.end(); ++pos)
    {
        const auto & cf = *pos;

        if (cf->isInMemoryFile() || cf->isTinyFile())
        {
            replayed_rows_and_deletes
                += replayBlock(dm_context, data_provider, *cf, offset, stable_rows, calculate_read_packs, delta_reader);
            offset = 0;
        }
        else if (const auto * cf_delete_range = cf->tryToDeleteRange(); cf_delete_range)
        {
            replayed_rows_and_deletes += replayDeleteRange(*cf_delete_range, delta_reader, stable_rows);
        }
        else if (const auto * cf_big = cf->tryToBigFile(); cf_big)
        {
            replayed_rows_and_deletes += replayColumnFileBig(dm_context, *cf_big, stable_rows);
        }
        else
        {
            RUNTIME_CHECK_MSG(false, "Unknow column file: {}", cf->toString());
        }
    }

    RUNTIME_CHECK(
        replayed_rows_and_deletes == delta_rows + delta_delete_ranges,
        replayed_rows_and_deletes,
        delta_rows,
        delta_delete_ranges);
    RUNTIME_CHECK(base_versions->size() == delta_rows, base_versions->size(), delta_rows);

    LOG_INFO(
        snapshot.log,
        "replays {} rows. initial_replayed_rows_and_deletes={}, delta_rows={}, delta_delete_ranges={}, "
        "replayed_rows_and_deletes={}.",
        replayed_rows_and_deletes - initial_replayed_rows_and_deletes,
        initial_replayed_rows_and_deletes,
        delta_rows,
        delta_delete_ranges,
        replayed_rows_and_deletes);

    return base_versions;
}

template <ExtraHandleType HandleType>
UInt32 VersionChain<HandleType>::replayBlock(
    const DMContext & dm_context,
    const IColumnFileDataProviderPtr & data_provider,
    const ColumnFile & cf,
    const UInt32 offset,
    const UInt32 stable_rows,
    const bool calculate_read_packs,
    DeltaValueReader & delta_reader)
{
    assert(cf.isInMemoryFile() || cf.isTinyFile());

    auto cf_reader = cf.getReader(dm_context, data_provider, getHandleColumnDefinesPtr<HandleType>(), ReadTag::MVCC);
    auto block = cf_reader->readNextBlock();
    RUNTIME_CHECK_MSG(
        cf.getRows() == block.rows(),
        "ColumnFile<{}> returns {} rows. Read all rows in one block is required!",
        cf.toString(),
        block.rows());

    const auto & column = *(block.begin()->column);
    RUNTIME_CHECK(column.size() > offset, column.size(), offset);

    const auto handle_col = ColumnView<HandleType>(*(block.begin()->column));
    auto itr = handle_col.begin() + offset;

    if (calculate_read_packs)
        calculateReadPacks(itr, handle_col.end());

    for (; itr != handle_col.end(); ++itr)
    {
        const auto h = *itr;
        const RowID curr_row_id = base_versions->size() + stable_rows;
        if (auto t = new_handle_to_row_ids.find(h, delta_reader, stable_rows); t)
        {
            base_versions->push_back(*t);
            continue;
        }
        if (auto row_id = findBaseVersionFromDMFileOrDeleteRangeList(dm_context, h); row_id)
        {
            base_versions->push_back(*row_id);
            continue;
        }

        new_handle_to_row_ids.insert(h, curr_row_id);
        base_versions->push_back(NotExistRowID);
    }
    return column.size() - offset;
}

template <ExtraHandleType HandleType>
UInt32 VersionChain<HandleType>::replayColumnFileBig(
    const DMContext & dm_context,
    const ColumnFileBig & cf_big,
    const UInt32 stable_rows)
{
    const UInt32 rows = cf_big.getRows();
    const UInt32 start_row_id = base_versions->size() + stable_rows;
    base_versions->insert(base_versions->end(), rows, NotExistRowID);

    dmfile_or_delete_range_list.push_back(
        DMFileHandleIndex<HandleType>{dm_context, cf_big.getFile(), start_row_id, cf_big.getRange()});
    return rows;
}

template <ExtraHandleType HandleType>
UInt32 VersionChain<HandleType>::replayDeleteRange(
    const ColumnFileDeleteRange & cf_delete_range,
    DeltaValueReader & delta_reader,
    const UInt32 stable_rows)
{
    new_handle_to_row_ids.deleteRange(cf_delete_range.getDeleteRange(), delta_reader, stable_rows);
    dmfile_or_delete_range_list.push_back(cf_delete_range.getDeleteRange());
    return cf_delete_range.getDeletes();
}

template <ExtraHandleType HandleType>
template <HandleRefType HandleRef>
std::optional<RowID> VersionChain<HandleType>::findBaseVersionFromDMFileOrDeleteRangeList(
    const DMContext & dm_context,
    HandleRef h)
{
    // From from new to old
    for (auto & dmfile_or_delete_range : dmfile_or_delete_range_list | std::views::reverse)
    {
        if (auto * dmfile_index = std::get_if<DMFileHandleIndex<HandleType>>(&dmfile_or_delete_range); dmfile_index)
        {
            if (auto row_id = dmfile_index->getBaseVersion(dm_context, h); row_id)
                return row_id;
        }
        else if (auto * delete_range = std::get_if<RowKeyRange>(&dmfile_or_delete_range); delete_range)
        {
            if (inRowKeyRange(*delete_range, h))
                return {};
        }
    }
    return {};
}

template <ExtraHandleType HandleType>
template <typename Iterator>
void VersionChain<HandleType>::calculateReadPacks(Iterator begin, Iterator end)
{
    assert(dmfile_or_delete_range_list.size() == 1);
    auto & dmfile_index = std::get<DMFileHandleIndex<HandleType>>(dmfile_or_delete_range_list.front());
    dmfile_index.calculateReadPacks(begin, end);
}

template <ExtraHandleType HandleType>
void VersionChain<HandleType>::cleanHandleColumn()
{
    for (auto & dmfile_or_delete_range : dmfile_or_delete_range_list)
    {
        if (auto * dmfile_index = std::get_if<DMFileHandleIndex<HandleType>>(&dmfile_or_delete_range); dmfile_index)
            dmfile_index->cleanHandleColumn();
    }
}

template class VersionChain<Int64>;
template class VersionChain<String>;

} // namespace DB::DM
