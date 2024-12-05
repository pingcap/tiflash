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

#include <Storages/DeltaMerge/VersionChain/VersionChain.h>

namespace DB::DM
{

template <Int64OrString Handle>
VersionChain<Handle>::VersionChain() = default;


template <Int64OrString Handle>
VersionChain<Handle>::VersionChain(const Context & global_context, const Segment & seg)
{
    const auto & dmfiles = seg.getStable()->getDMFiles();
    RUNTIME_CHECK(dmfiles.size() == 1, dmfiles.size());
    dmfile_or_delete_range_list->push_back(DMFileHandleIndex<Handle>{global_context, dmfiles[0]});
}

template <Int64OrString Handle>
std::shared_ptr<const std::vector<RowID>> VersionChain<Handle>::replaySnapshot(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot)
{
    assert(snapshot.delta != nullptr);

    const auto & delta = *(snapshot.delta);
    UInt32 delta_rows = delta.getRows();
    UInt32 delta_delete_ranges = delta.getDeletes();
    assert(delta_rows > 0 || delta_delete_ranges > 0);

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

    const auto initial_replayed_rows_and_deletes = replayed_rows_and_deletes;
    for (; pos != cfs.end(); ++pos)
    {
        const auto & cf = *pos;

        if (cf->isInMemoryFile() || cf->isTinyFile())
        {
            replayed_rows_and_deletes += replayBlock(dm_context, data_provider, *cf, offset);
            offset = 0;
        }
        else if (const auto * cf_delete_range = cf->tryToDeleteRange(); cf_delete_range)
        {
            replayed_rows_and_deletes += replayDeleteRange(*cf_delete_range);
        }
        else if (const auto * cf_big = cf->tryToBigFile(); cf_big)
        {
            replayed_rows_and_deletes += replayColumnFileBig(dm_context, *cf_big);
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
    UInt32 offset)
{
    assert(cf.isInMemoryFile() || cf.isTinyFile());

    auto cf_reader = cf.getReader(dm_context, data_provider, getHandleColumnDefinesPtr<Handle>(), ReadTag::MVCC);
    auto block = cf_reader->readNextBlock();
    RUNTIME_CHECK_MSG(!cf_reader->readNextBlock(), "{}: read all rows in one block is required!", cf.toString());
    auto handles = block.begin()->column;
    const auto * v = toColumnVectorDataPtr<Int64>(handles);
    RUNTIME_CHECK_MSG(v != nullptr, "TODO: support common handle");
    for (auto i = offset; i < v->size(); ++i)
    {
        auto h = (*v)[i];
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

        new_handle_to_row_ids->insert(std::make_pair(h, base_versions->size()));
        base_versions->push_back(NotExistRowID);
    }
    return v->size() - offset;
}

template <Int64OrString Handle>
UInt32 VersionChain<Handle>::replayColumnFileBig(const DMContext & dm_context, const ColumnFileBig & cf_big)
{
    auto rows = cf_big.getRows();
    base_versions->insert(base_versions->end(), rows, NotExistRowID);

    dmfile_or_delete_range_list->push_back(DMFileHandleIndex<Handle>{
        dm_context.global_context,
        cf_big.getFile(),
    });
    return rows;
}

template <Int64OrString Handle>
UInt32 VersionChain<Handle>::replayDeleteRange(const ColumnFileDeleteRange & cf_delete_range)
{
    auto [start, end] = convertRowKeyRange<Handle>(cf_delete_range.getDeleteRange());
    auto itr = new_handle_to_row_ids->lower_bound(start);
    const auto end_itr = new_handle_to_row_ids->lower_bound(end);
    while (itr != end_itr)
    {
        itr = new_handle_to_row_ids->erase(itr);
    }
    dmfile_or_delete_range_list->push_back(cf_delete_range.getDeleteRange());
    return cf_delete_range.getDeletes();
}

template <Int64OrString Handle>
std::optional<RowID> VersionChain<Handle>::findBaseVersionFromDMFileOrDeleteRangeList(Handle h)
{
    for (auto & dmfile_or_delete_range : *dmfile_or_delete_range_list)
    {
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

template class VersionChain<Int64>;
//template class VersionChain<String>;

} // namespace DB::DM
