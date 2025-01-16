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
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/VersionChain/RowKeyFilter.h>

namespace DB::DM
{
namespace
{
template <ExtraHandleType HandleType>
UInt32 buildRowKeyFilterVector(
    const ColumnView<HandleType> & handles,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const UInt32 start_row_id,
    IColumn::Filter & filter)
{
    for (auto itr = handles.begin(); itr != handles.end(); ++itr)
    {
        auto in_range = [h = *itr](const RowKeyRange & range) {
            return inRowKeyRange(range, h);
        };
        // IN delete_ranges or NOT IN read_ranges
        if (std::any_of(delete_ranges.begin(), delete_ranges.end(), in_range)
            || std::none_of(read_ranges.begin(), read_ranges.end(), in_range))
        {
            filter[itr - handles.begin() + start_row_id] = 0;
        }
    }
    return handles.end() - handles.begin();
}

template <ExtraHandleType HandleType>
UInt32 buildRowKeyFilterBlock(
    const DMContext & dm_context,
    const IColumnFileDataProviderPtr & data_provider,
    const ColumnFile & cf,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const UInt32 start_row_id,
    IColumn::Filter & filter)
{
    assert(cf.isInMemoryFile() || cf.isTinyFile());
    const UInt32 rows = cf.getRows();
    assert(rows > 0);

    auto cf_reader = cf.getReader(dm_context, data_provider, getHandleColumnDefinesPtr<HandleType>(), ReadTag::MVCC);
    auto block = cf_reader->readNextBlock();
    RUNTIME_CHECK_MSG(
        rows == block.rows(),
        "ColumnFile<{}> returns {} rows. Read all rows in one block is required!",
        cf.toString(),
        block.rows());

    const auto handles = ColumnView<HandleType>(*(block.begin()->column));
    return buildRowKeyFilterVector<HandleType>(handles, delete_ranges, read_ranges, start_row_id, filter);
}

template <ExtraHandleType HandleType>
UInt32 buildRowKeyFilterDMFile(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const std::optional<RowKeyRange> & segment_range,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResultPtr & stable_filter_res,
    const UInt32 start_row_id,
    IColumn::Filter & filter)
{
    auto [valid_handle_res, valid_start_pack_id] = getClippedRSResultsByRanges(dm_context, dmfile, segment_range);
    if (unlikely(valid_handle_res.empty()))
        return 0;

    // Filter out these packs that don't need to read.
    if (stable_filter_res)
    {
        RUNTIME_CHECK(valid_start_pack_id == 0, valid_start_pack_id);
        const auto & pack_res = stable_filter_res->getPackRes();
        RUNTIME_CHECK(pack_res.size() == valid_handle_res.size(), pack_res.size(), valid_handle_res.size());
        for (UInt32 i = 0; i < valid_handle_res.size(); ++i)
            if (!pack_res[i].isUse())
                valid_handle_res[i] = RSResult::None;
    }

    // RSResult of read_ranges.
    const auto & read_ranges_handle_res
        = stable_filter_res ? stable_filter_res->getHandleRes() : getRSResultsByRanges(dm_context, dmfile, read_ranges);
    for (UInt32 i = 0; i < valid_handle_res.size(); ++i)
        valid_handle_res[i] = valid_handle_res[i] && read_ranges_handle_res[valid_start_pack_id + i];

    // RSResult of delete_ranges.
    if (!delete_ranges.empty())
    {
        const auto delete_ranges_handle_res = getRSResultsByRanges(dm_context, dmfile, delete_ranges);
        for (UInt32 i = 0; i < valid_handle_res.size(); ++i)
            valid_handle_res[i] = valid_handle_res[i] && !delete_ranges_handle_res[valid_start_pack_id + i];
    }

    auto need_read_packs = std::make_shared<IdSet>();
    UInt32 need_read_rows = 0;
    std::unordered_map<UInt32, UInt32> need_read_pack_to_start_row_ids;

    const auto & pack_stats = dmfile->getPackStats();
    UInt32 processed_rows = 0;
    for (UInt32 i = 0; i < valid_handle_res.size(); ++i)
    {
        const auto pack_id = valid_start_pack_id + i;
        if (!valid_handle_res[i].isUse())
        {
            std::fill_n(filter.begin() + start_row_id + processed_rows, pack_stats[pack_id].rows, false);
        }
        else if (!valid_handle_res[i].allMatch())
        {
            need_read_packs->insert(pack_id);
            need_read_pack_to_start_row_ids.emplace(pack_id, start_row_id + processed_rows);
            need_read_rows += pack_stats[pack_id].rows;
        }
        processed_rows += pack_stats[pack_id].rows;
    }

    if (need_read_rows == 0)
        return processed_rows;

    DMFileBlockInputStreamBuilder builder(dm_context.global_context);
    builder.onlyReadOnePackEveryTime().setReadPacks(need_read_packs).setReadTag(ReadTag::MVCC);
    auto stream
        = builder.build(dmfile, {getHandleColumnDefine<HandleType>()}, /*rowkey_ranges*/ {}, dm_context.scan_context);
    UInt32 read_rows = 0;
    for (auto pack_id : *need_read_packs)
    {
        auto block = stream->read();
        RUNTIME_CHECK(block.rows() == pack_stats[pack_id].rows, block.rows(), pack_stats[pack_id].rows);
        const auto handles = ColumnView<HandleType>(*(block.begin()->column));
        const auto itr = need_read_pack_to_start_row_ids.find(pack_id);
        RUNTIME_CHECK(itr != need_read_pack_to_start_row_ids.end(), need_read_pack_to_start_row_ids, pack_id);
        read_rows += buildRowKeyFilterVector(
            handles,
            delete_ranges,
            read_ranges,
            /*start_row_id*/ itr->second,
            filter);
    }
    RUNTIME_CHECK(read_rows == need_read_rows, read_rows, need_read_rows);
    return processed_rows;
}

template <ExtraHandleType HandleType>
UInt32 buildRowKeyFilterColumnFileBig(
    const DMContext & dm_context,
    const ColumnFileBig & cf_big,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const UInt32 start_row_id,
    IColumn::Filter & filter)
{
    if (cf_big.getRows() == 0)
        return 0;
    return buildRowKeyFilterDMFile<HandleType>(
        dm_context,
        cf_big.getFile(),
        cf_big.getRange(),
        delete_ranges,
        Segment::shrinkRowKeyRanges(cf_big.getRange(), read_ranges),
        /*stable_filter_res*/ nullptr,
        start_row_id,
        filter);
}

template <ExtraHandleType HandleType>
UInt32 buildRowKeyFilterStable(
    const DMContext & dm_context,
    const StableValueSpace::Snapshot & stable,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResultPtr & stable_filter_res,
    IColumn::Filter & filter)
{
    const auto & dmfiles = stable.getDMFiles();
    RUNTIME_CHECK(dmfiles.size() == 1, dmfiles.size());
    const auto & dmfile = dmfiles[0];
    if (unlikely(dmfile->getPacks() == 0))
        return 0;
    return buildRowKeyFilterDMFile<HandleType>(
        dm_context,
        dmfile,
        /*segment_range*/ std::nullopt,
        delete_ranges,
        read_ranges,
        stable_filter_res,
        /*start_row_id*/ 0,
        filter);
}

} // namespace

template <ExtraHandleType HandleType>
void buildRowKeyFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResultPtr & stable_filter_res,
    IColumn::Filter & filter)
{
    const auto & delta = *(snapshot.delta);
    const auto & stable = *(snapshot.stable);
    const UInt32 delta_rows = delta.getRows();
    const UInt32 stable_rows = stable.getDMFilesRows();
    const UInt32 total_rows = delta_rows + stable_rows;
    RUNTIME_CHECK(filter.size() == total_rows, filter.size(), total_rows);
    const auto cfs = delta.getColumnFiles();
    const auto & data_provider = delta.getDataProvider();

    RowKeyRanges delete_ranges;
    UInt32 read_rows = 0;
    // Read ColumnFiles from new to old for handling delete ranges
    for (auto itr = cfs.rbegin(); itr != cfs.rend(); ++itr)
    {
        const auto & cf = *itr;
        if (const auto * cf_delete_range = cf->tryToDeleteRange(); cf_delete_range)
        {
            delete_ranges.push_back(cf_delete_range->getDeleteRange());
            continue;
        }

        const UInt32 cf_rows = cf->getRows();
        RUNTIME_CHECK(delta_rows >= read_rows + cf_rows, delta_rows, read_rows, cf_rows);
        const UInt32 start_row_id = total_rows - read_rows - cf_rows;
        read_rows += cf_rows;

        // TODO: add min-max value in tiny file to optimize rowkey filter.
        if (cf->isInMemoryFile() || cf->isTinyFile())
        {
            const auto n = buildRowKeyFilterBlock<HandleType>(
                dm_context,
                data_provider,
                *cf,
                delete_ranges,
                read_ranges,
                start_row_id,
                filter);
            RUNTIME_CHECK(cf_rows == n, cf_rows, n);
            continue;
        }
        else if (const auto * cf_big = cf->tryToBigFile(); cf_big)
        {
            const auto n = buildRowKeyFilterColumnFileBig<HandleType>(
                dm_context,
                *cf_big,
                delete_ranges,
                read_ranges,
                start_row_id,
                filter);
            RUNTIME_CHECK(cf_rows == n, cf_rows, n);
            continue;
        }
        RUNTIME_CHECK_MSG(false, "{}: unknow ColumnFile type", cf->toString());
    }
    RUNTIME_CHECK(read_rows == delta_rows, read_rows, delta_rows);

    const auto n = buildRowKeyFilterStable<HandleType>(
        dm_context,
        stable,
        delete_ranges,
        read_ranges,
        stable_filter_res,
        filter);
    RUNTIME_CHECK(n == stable_rows, n, stable_rows);
}

template void buildRowKeyFilter<Int64>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResultPtr & filter_res,
    IColumn::Filter & filter);

template void buildRowKeyFilter<String>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResultPtr & filter_res,
    IColumn::Filter & filter);
} // namespace DB::DM
