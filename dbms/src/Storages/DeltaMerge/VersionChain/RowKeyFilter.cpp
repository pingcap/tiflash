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

#include <ranges>

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
    BitmapFilter & filter)
{
    UInt32 filtered_out_rows = 0;
    for (UInt32 i = 0; i < handles.size(); ++i)
    {
        auto in_range = [h = handles[i]](const RowKeyRange & range) {
            return inRowKeyRange(range, h);
        };
        // IN delete_ranges or NOT IN read_ranges
        if (std::any_of(delete_ranges.begin(), delete_ranges.end(), in_range)
            || std::none_of(read_ranges.begin(), read_ranges.end(), in_range))
        {
            filter[i + start_row_id] = 0;
            ++filtered_out_rows;
        }
    }
    return filtered_out_rows;
}

template <ExtraHandleType HandleType>
UInt32 buildRowKeyFilterBlock(
    const DMContext & dm_context,
    const IColumnFileDataProviderPtr & data_provider,
    const ColumnFile & cf,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const UInt32 start_row_id,
    BitmapFilter & filter)
{
    assert(cf.isInMemoryFile() || cf.isTinyFile());
    const UInt32 rows = cf.getRows();
    if (rows == 0)
        return 0;

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
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const UInt32 start_pack_id,
    const RSResults & handle_res, // read_ranges && !delete_ranges
    const RSResults & pack_res, // read_ranges && !delete_ranges && (rs_filter if stable)
    const UInt32 start_row_id,
    BitmapFilter & filter)
{
    auto need_read_packs = std::make_shared<IdSet>();
    std::unordered_map<UInt32, UInt32> start_row_id_of_need_read_packs; // pack_id -> start_row_id

    const auto & pack_stats = dmfile->getPackStats();
    UInt32 processed_rows = 0;
    UInt32 filtered_out_rows = 0;
    for (UInt32 i = 0; i < handle_res.size(); ++i)
    {
        const auto pack_id = start_pack_id + i;
        const auto pack_start_row_id = start_row_id + processed_rows;
        processed_rows += pack_stats[pack_id].rows;
        if (!pack_res[i].isUse())
        {
            filtered_out_rows += pack_stats[pack_id].rows;
            filter.set(pack_start_row_id, pack_stats[pack_id].rows, false);
        }
        else if (!handle_res[i].allMatch())
        {
            need_read_packs->insert(pack_id);
            start_row_id_of_need_read_packs.emplace(pack_id, pack_start_row_id);
        }
        // else handle_res[i].allMatch() do nothing
    }

    if (need_read_packs->empty())
        return filtered_out_rows;

    DMFileBlockInputStreamBuilder builder(dm_context.global_context);
    builder.onlyReadOnePackEveryTime().setReadPacks(need_read_packs).setReadTag(ReadTag::MVCC);
    auto stream
        = builder.build(dmfile, {getHandleColumnDefine<HandleType>()}, /*rowkey_ranges*/ {}, dm_context.scan_context);

    for (auto pack_id : *need_read_packs)
    {
        auto block = stream->read();
        RUNTIME_CHECK(block.rows() == pack_stats[pack_id].rows, block.rows(), pack_stats[pack_id].rows);
        const auto handles = ColumnView<HandleType>(*(block.begin()->column));
        const auto itr = start_row_id_of_need_read_packs.find(pack_id);
        RUNTIME_CHECK(itr != start_row_id_of_need_read_packs.end(), start_row_id_of_need_read_packs, pack_id);
        filtered_out_rows += buildRowKeyFilterVector(
            handles,
            delete_ranges,
            read_ranges,
            /*start_row_id*/ itr->second,
            filter);
    }
    return filtered_out_rows;
}

template <ExtraHandleType HandleType>
UInt32 buildRowKeyFilterColumnFileBig(
    const DMContext & dm_context,
    const ColumnFileBig & cf_big,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const UInt32 start_row_id,
    BitmapFilter & filter)
{
    auto [valid_handle_res, valid_start_pack_id]
        = getClippedRSResultsByRanges(dm_context, cf_big.getFile(), cf_big.getRange());
    if (unlikely(valid_handle_res.empty()))
        return 0;

    if (!delete_ranges.empty())
    {
        const auto delete_ranges_handle_res = getRSResultsByRanges(dm_context, cf_big.getFile(), delete_ranges);
        for (UInt32 i = 0; i < valid_handle_res.size(); ++i)
            valid_handle_res[i] = valid_handle_res[i] && !delete_ranges_handle_res[valid_start_pack_id + i];
    }

    auto real_read_ranges = Segment::shrinkRowKeyRanges(cf_big.getRange(), read_ranges);
    {
        auto real_read_ranges_handle_res = getRSResultsByRanges(dm_context, cf_big.getFile(), real_read_ranges);
        for (UInt32 i = 0; i < valid_handle_res.size(); ++i)
            valid_handle_res[i] = valid_handle_res[i] && real_read_ranges_handle_res[valid_start_pack_id + i];
    }

    return buildRowKeyFilterDMFile<HandleType>(
        dm_context,
        cf_big.getFile(),
        delete_ranges,
        real_read_ranges,
        valid_start_pack_id,
        valid_handle_res,
        valid_handle_res,
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
    BitmapFilter & filter)
{
    const auto & dmfiles = stable.getDMFiles();
    RUNTIME_CHECK(dmfiles.size() == 1, dmfiles.size());
    const auto & dmfile = dmfiles[0];
    if (unlikely(dmfile->getPacks() == 0))
        return 0;

    auto handle_res = stable_filter_res->getHandleRes();
    auto pack_res = stable_filter_res->getPackRes();
    if (!delete_ranges.empty())
    {
        const auto delete_ranges_handle_res = getRSResultsByRanges(dm_context, dmfile, delete_ranges);
        for (UInt32 i = 0; i < handle_res.size(); ++i)
        {
            handle_res[i] = handle_res[i] && !delete_ranges_handle_res[i];
            pack_res[i] = pack_res[i] && !delete_ranges_handle_res[i];
        }
    }

    return buildRowKeyFilterDMFile<HandleType>(
        dm_context,
        dmfile,
        delete_ranges,
        read_ranges,
        /*start_pack_id*/ 0,
        handle_res,
        pack_res,
        /*start_row_id*/ 0,
        filter);
}

} // namespace

template <ExtraHandleType HandleType>
UInt32 buildRowKeyFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResultPtr & stable_filter_res,
    BitmapFilter & filter)
{
    const auto & delta = *(snapshot.delta);
    const auto & stable = *(snapshot.stable);
    const UInt32 delta_rows = delta.getRows();
    const UInt32 stable_rows = stable.getDMFilesRows();
    const UInt32 total_rows = delta_rows + stable_rows;
    const auto cfs = delta.getColumnFiles();
    const auto & data_provider = delta.getDataProvider();
    RUNTIME_CHECK(filter.size() == total_rows, filter.size(), total_rows);

    RowKeyRanges delete_ranges;
    UInt32 read_rows = 0;
    UInt32 filtered_out_rows = 0;
    // Read ColumnFiles from new to old for handling delete ranges
    for (const auto & cf : cfs | std::views::reverse)
    {
        if (const auto * cf_delete_range = cf->tryToDeleteRange(); cf_delete_range)
        {
            delete_ranges.push_back(cf_delete_range->getDeleteRange());
            continue;
        }

        const UInt32 cf_rows = cf->getRows();
        RUNTIME_CHECK(delta_rows >= read_rows + cf_rows, delta_rows, read_rows, cf_rows);
        const UInt32 start_row_id = total_rows - read_rows - cf_rows;
        read_rows += cf_rows;

        // TODO: add min-max value in tiny/memory column file to optimize rowkey filter.
        if (cf->isInMemoryFile() || cf->isTinyFile())
        {
            filtered_out_rows += buildRowKeyFilterBlock<HandleType>(
                dm_context,
                data_provider,
                *cf,
                delete_ranges,
                read_ranges,
                start_row_id,
                filter);
            continue;
        }

        if (const auto * cf_big = cf->tryToBigFile(); cf_big)
        {
            filtered_out_rows += buildRowKeyFilterColumnFileBig<HandleType>(
                dm_context,
                *cf_big,
                delete_ranges,
                read_ranges,
                start_row_id,
                filter);
            continue;
        }
        RUNTIME_CHECK_MSG(false, "{}: unknow ColumnFile type", cf->toString());
    }
    RUNTIME_CHECK(read_rows == delta_rows, read_rows, delta_rows);

    filtered_out_rows += buildRowKeyFilterStable<HandleType>(
        dm_context,
        stable,
        delete_ranges,
        read_ranges,
        stable_filter_res,
        filter);
    return filtered_out_rows;
}

template UInt32 buildRowKeyFilter<Int64>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResultPtr & filter_res,
    BitmapFilter & filter);

template UInt32 buildRowKeyFilter<String>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResultPtr & filter_res,
    BitmapFilter & filter);
} // namespace DB::DM
