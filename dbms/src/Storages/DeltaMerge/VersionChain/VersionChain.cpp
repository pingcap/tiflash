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
    if (dmfile_or_delete_range_list.empty())
    {
        // In theory, we can support stable composed of multiple disjoint dmfiles.
        // But it is not necessary for now. For simplicity, assume stable always has one DMFile.
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
    std::lock_guard lock(mtx); // Not run concurrently. Because it can reuse the result of the previous replay.
    if (delta_rows + delta_delete_ranges <= replayed_rows_and_deletes)
    {
        RUNTIME_CHECK(base_versions->size() >= delta_rows, base_versions->size(), delta_rows);
        return base_versions;
    }

    // base_versions may be shared for read, so copy for write here.
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
    SCOPE_EXIT({ cleanHandleColumn(); });

    auto delta_reader = createDeltaValueReaderIfCommonHandle(dm_context, snapshot.delta);

    UInt32 curr_replayed_rows = 0;
    UInt32 curr_replayed_deletes = 0;
    for (; pos != cfs.end(); ++pos)
    {
        const auto & cf = *pos;

        if (cf->isInMemoryFile() || cf->isTinyFile())
        {
            curr_replayed_rows
                += replayBlock(dm_context, data_provider, *cf, offset, stable_rows, calculate_read_packs, delta_reader);
            offset = 0;
        }
        else if (const auto * cf_delete_range = cf->tryToDeleteRange(); cf_delete_range)
        {
            curr_replayed_deletes += replayDeleteRange(*cf_delete_range, delta_reader, stable_rows);
        }
        else if (const auto * cf_big = cf->tryToBigFile(); cf_big)
        {
            curr_replayed_rows += replayColumnFileBig(
                dm_context,
                *cf_big,
                stable_rows,
                stable,
                std::span{cfs.begin(), pos},
                delta_reader);
        }
        else
        {
            RUNTIME_CHECK_MSG(false, "Unknow column file: {}", cf->toString());
        }
    }

    replayed_rows_and_deletes += curr_replayed_rows + curr_replayed_deletes;
    RUNTIME_CHECK(
        replayed_rows_and_deletes == delta_rows + delta_delete_ranges,
        replayed_rows_and_deletes,
        delta_rows,
        delta_delete_ranges);
    RUNTIME_CHECK(base_versions->size() == delta_rows, base_versions->size(), delta_rows);

    LOG_INFO(
        snapshot.log,
        "Snapshot={}, replays {} rows and {} deletes",
        snapshot.detailInfo(),
        curr_replayed_rows,
        curr_replayed_deletes);

    return base_versions;
}

template <ExtraHandleType HandleType>
template <typename Iter>
void VersionChain<HandleType>::replayHandles(
    const DMContext & dm_context,
    Iter begin,
    Iter end,
    const UInt32 stable_rows,
    std::optional<DeltaValueReader> & delta_reader)
{
    for (auto itr = begin; itr != end; ++itr)
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
}

template <ExtraHandleType HandleType>
UInt32 VersionChain<HandleType>::replayBlock(
    const DMContext & dm_context,
    const IColumnFileDataProviderPtr & data_provider,
    const ColumnFile & cf,
    const UInt32 offset,
    const UInt32 stable_rows,
    const bool calculate_read_packs,
    std::optional<DeltaValueReader> & delta_reader)
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

    const auto handle_col = ColumnView<HandleType>(column);
    auto itr = handle_col.begin() + offset;

    if (calculate_read_packs)
        calculateReadPacks(itr, handle_col.end());

    replayHandles(dm_context, itr, handle_col.end(), stable_rows, delta_reader);
    return column.size() - offset;
}

template <ExtraHandleType HandleType>
UInt32 VersionChain<HandleType>::replayColumnFileBig(
    const DMContext & dm_context,
    const ColumnFileBig & cf_big,
    const UInt32 stable_rows,
    const StableValueSpace::Snapshot & stable,
    const std::span<const ColumnFilePtr> preceding_cfs,
    std::optional<DeltaValueReader> & delta_reader)
{
    auto cf_big_min_max = loadDMFileHandleRange<HandleType>(dm_context.global_context, *(cf_big.getFile()));
    if (!cf_big_min_max) // DMFile is empty.
        return 0;

    HandleRefType cf_big_min = cf_big_min_max->first;
    HandleRefType cf_big_max = cf_big_min_max->second;

    // If a ColumnFileBig is apply snapshot or ingest sst, there is no data locally that intersects with it.
    auto is_apply_snapshot = [&]() {
        if (dmfile_or_delete_range_list.empty())
            return false;
        if (auto * delete_range = std::get_if<RowKeyRange>(&dmfile_or_delete_range_list.back()); delete_range)
            return inRowKeyRange(*delete_range, cf_big_min) && inRowKeyRange(*delete_range, cf_big_max);
        return false;
    };

    auto is_dmfile_intersect = [&](const DMFile & file) {
        auto file_min_max = loadDMFileHandleRange<HandleType>(dm_context.global_context, file);
        if (!file_min_max)
            return false;
        const auto & [file_min, file_max] = *file_min_max;
        return cf_big_min <= file_max && file_min <= cf_big_max;
    };

    auto is_ingest_sst = [&]() {
        for (const auto & file : stable.getDMFiles())
            if (is_dmfile_intersect(*file))
                return false;

        for (const auto & cf : preceding_cfs)
        {
            if (const auto * t = cf->tryToBigFile(); t)
            {
                if (is_dmfile_intersect(*(t->getFile())))
                    return false;
            }
            else
                return false;
        }
        return true;
    };

    if (likely(is_apply_snapshot() || is_ingest_sst()))
    {
        const UInt32 rows = cf_big.getRows();
        const UInt32 start_row_id = base_versions->size() + stable_rows;
        base_versions->insert(base_versions->end(), rows, NotExistRowID);

        dmfile_or_delete_range_list.push_back(
            DMFileHandleIndex<HandleType>{dm_context, cf_big.getFile(), start_row_id, cf_big.getRange()});
        return rows;
    }

    // Replay dmfile as normal write. At present, only testing may take this path.
    LOG_WARNING(
        Logger::get(dm_context.tracing_id),
        "ColumnFileBig={} is neither apply snapshot nor ingest sst",
        cf_big.toString());
    auto cf_reader = cf_big.getReader(
        dm_context,
        /*data_provider*/ nullptr,
        getHandleColumnDefinesPtr<HandleType>(),
        ReadTag::MVCC);
    UInt32 read_rows = 0;
    while (true)
    {
        auto block = cf_reader->readNextBlock();
        if (!block)
            break;
        read_rows += block.rows();
        const auto handle_col = ColumnView<HandleType>(*(block.begin()->column));
        replayHandles(dm_context, handle_col.begin(), handle_col.end(), stable_rows, delta_reader);
    }
    RUNTIME_CHECK(read_rows == cf_big.getRows(), read_rows, cf_big.getRows());
    return read_rows;
}

template <ExtraHandleType HandleType>
UInt32 VersionChain<HandleType>::replayDeleteRange(
    const ColumnFileDeleteRange & cf_delete_range,
    std::optional<DeltaValueReader> & delta_reader,
    const UInt32 stable_rows)
{
    new_handle_to_row_ids.deleteRange(cf_delete_range.getDeleteRange(), delta_reader, stable_rows);
    dmfile_or_delete_range_list.push_back(cf_delete_range.getDeleteRange());
    return cf_delete_range.getDeletes();
}

template <ExtraHandleType HandleType>
std::optional<RowID> VersionChain<HandleType>::findBaseVersionFromDMFileOrDeleteRangeList(
    const DMContext & dm_context,
    HandleRefType h)
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
template <typename Iter>
void VersionChain<HandleType>::calculateReadPacks(Iter begin, Iter end)
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

template <ExtraHandleType HandleType>
std::optional<DeltaValueReader> VersionChain<HandleType>::createDeltaValueReaderIfCommonHandle(
    const DMContext & dm_context,
    const DeltaSnapshotPtr & delta_snap)
{
    if constexpr (std::is_same_v<HandleType, String>)
        return DeltaValueReader{
            dm_context,
            delta_snap,
            getHandleColumnDefinesPtr<HandleType>(),
            /*range*/ {},
            ReadTag::MVCC};
    else
        return std::nullopt;
}

template class VersionChain<Int64>;
template class VersionChain<String>;

} // namespace DB::DM
