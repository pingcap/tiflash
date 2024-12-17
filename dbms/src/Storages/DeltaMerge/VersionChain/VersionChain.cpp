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

namespace DB::DM
{

template <Int64OrString Handle>
std::shared_ptr<const std::vector<RowID>> VersionChain<Handle>::replaySnapshot(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot)
{
    // Check Stable
    if (dmfile_or_delete_range_list->empty())
    {
        const auto & dmfiles = snapshot.stable->getDMFiles();
        RUNTIME_CHECK(dmfiles.size() == 1, dmfiles.size());
        dmfile_or_delete_range_list->push_back(
            DMFileHandleIndex<Handle>{dm_context, dmfiles[0], /*start_row_id*/ 0, std::nullopt});
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

    // `pos` points to the first ColumnFile that has data not been replayed
    auto offset = replayed_rows_and_deletes - skipped_rows_and_deletes;
    // Only ColumnFileInMemory or ColumnFileTiny can be half replayed
    RUNTIME_CHECK(offset == 0 || (*pos)->isInMemoryFile() || (*pos)->isTinyFile(), offset, (*pos)->toString());

    const bool calculate_read_packs = (cfs.end() - pos == 1) && ((*pos)->isInMemoryFile() || (*pos)->isTinyFile())
        && dmfile_or_delete_range_list->size() == 1;
    const auto initial_replayed_rows_and_deletes = replayed_rows_and_deletes;
    SCOPE_EXIT({ cleanHandleColumn(); });
    for (; pos != cfs.end(); ++pos)
    {
        const auto & cf = *pos;

        if (cf->isInMemoryFile() || cf->isTinyFile())
        {
            replayed_rows_and_deletes
                += replayBlock(dm_context, data_provider, *cf, offset, stable_rows, calculate_read_packs);
            offset = 0;
        }
        else if (const auto * cf_delete_range = cf->tryToDeleteRange(); cf_delete_range)
        {
            replayed_rows_and_deletes += replayDeleteRange(*cf_delete_range);
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

template <Int64OrString Handle>
UInt32 VersionChain<Handle>::replayBlock(
    const DMContext & dm_context,
    const IColumnFileDataProviderPtr & data_provider,
    const ColumnFile & cf,
    const UInt32 offset,
    const UInt32 stable_rows,
    const bool calculate_read_packs)
{
    assert(cf.isInMemoryFile() || cf.isTinyFile());

    auto cf_reader = cf.getReader(dm_context, data_provider, getHandleColumnDefinesPtr<Handle>(), ReadTag::MVCC);
    auto block = cf_reader->readNextBlock();
    RUNTIME_CHECK_MSG(!cf_reader->readNextBlock(), "{}: read all rows in one block is required!", cf.toString());
    const auto * handle_col = toColumnVectorDataPtr<Int64>(block.begin()->column);
    RUNTIME_CHECK_MSG(handle_col != nullptr, "TODO: support common handle");
    RUNTIME_CHECK(handle_col->size() > offset, handle_col->size(), offset);
    const std::span<const Handle> handles{
        handle_col->data() + offset, handle_col->size() - offset};

    if (calculate_read_packs)
        calculateReadPacks(handles);

    for (auto h : handles)
    {
        const RowID curr_row_id = base_versions->size() + stable_rows;
        if (auto itr = new_handle_to_row_ids->find(h); itr != new_handle_to_row_ids->end())
        {
            base_versions->push_back(itr->second);
            continue;
        }
        if (auto row_id = findBaseVersionFromDMFileOrDeleteRangeList(h); row_id)
        {
            base_versions->push_back(*row_id);
            continue;
        }

        new_handle_to_row_ids->insert(std::make_pair(h, curr_row_id));
        base_versions->push_back(NotExistRowID);
    }
    return handles.size();
}

template <Int64OrString Handle>
UInt32 VersionChain<Handle>::replayColumnFileBig(
    const DMContext & dm_context,
    const ColumnFileBig & cf_big,
    const UInt32 stable_rows)
{
    const UInt32 rows = cf_big.getRows();
    const UInt32 start_row_id = base_versions->size() + stable_rows;
    base_versions->insert(base_versions->end(), rows, NotExistRowID);

    dmfile_or_delete_range_list->push_back(
        DMFileHandleIndex<Handle>{dm_context, cf_big.getFile(), start_row_id, cf_big.getRange()});
    return rows;
}

template <Int64OrString Handle>
UInt32 VersionChain<Handle>::replayDeleteRange(const ColumnFileDeleteRange & cf_delete_range)
{
    auto [start, end] = convertRowKeyRange<Handle>(cf_delete_range.getDeleteRange());
    auto itr = new_handle_to_row_ids->lower_bound(start);
    const auto end_itr = new_handle_to_row_ids->lower_bound(end);
    std::vector<Handle> erased_handles;
    while (itr != end_itr)
    {
        erased_handles.push_back(itr->first);
        itr = new_handle_to_row_ids->erase(itr);
    }
    dmfile_or_delete_range_list->push_back(cf_delete_range.getDeleteRange());
    return cf_delete_range.getDeletes();
}

template <Int64OrString Handle>
std::optional<RowID> VersionChain<Handle>::findBaseVersionFromDMFileOrDeleteRangeList(Handle h)
{
    for (auto itr = dmfile_or_delete_range_list->rbegin(); itr != dmfile_or_delete_range_list->rend(); ++itr)
    {
        auto & dmfile_or_delete_range = *itr;
        if (auto * dmfile_index = std::get_if<DMFileHandleIndex<Handle>>(&dmfile_or_delete_range); dmfile_index)
        {
            if (auto row_id = dmfile_index->getBaseVersion(h); row_id)
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

template <Int64OrString Handle>
void VersionChain<Handle>::calculateReadPacks(const std::span<const Handle> handles)
{
    assert(dmfile_or_delete_range_list->size() == 1);
    auto & dmfile_index = std::get<DMFileHandleIndex<Handle>>(dmfile_or_delete_range_list->front());
    dmfile_index.calculateReadPacks(handles);
}

template <Int64OrString Handle>
void VersionChain<Handle>::cleanHandleColumn()
{
    for (auto & dmfile_or_delete_range : *dmfile_or_delete_range_list)
    {
        if (auto * dmfile_index = std::get_if<DMFileHandleIndex<Handle>>(&dmfile_or_delete_range); dmfile_index)
            dmfile_index->cleanHandleColumn();
    }
}

template class VersionChain<Int64>;
//template class VersionChain<String>;

} // namespace DB::DM
