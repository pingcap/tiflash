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

// TODO: shrinking read_range by segment_range
template <Int64OrString Handle>
UInt32 buildRowKeyFilterVector(
    const PaddedPODArray<Handle> & handles,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const UInt32 start_row_id,
    std::vector<UInt8> & filter)
{
    for (UInt32 i = 0; i < handles.size(); ++i)
    {
        auto in_range = [h = handles[i]](const RowKeyRange & range) {
            return inRowKeyRange(range, h);
        };
        if (std::any_of(delete_ranges.begin(), delete_ranges.end(), in_range)
            || std::none_of(read_ranges.begin(), read_ranges.end(), in_range))
        {
            filter[start_row_id + i] = 0;
        }
    }
    return handles.size();
}

template <Int64OrString Handle>
UInt32 buildRowKeyFilterBlock(
    const DMContext & dm_context,
    const IColumnFileDataProviderPtr & data_provider,
    const ColumnFile & cf,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const UInt32 start_row_id,
    std::vector<UInt8> & filter)
{
    assert(cf.isInMemoryFile() || cf.isTinyFile());

    if (cf.getRows() == 0)
        return 0;

    auto cf_reader = cf.getReader(dm_context, data_provider, getHandleColumnDefinesPtr<Handle>(), ReadTag::MVCC);
    auto block = cf_reader->readNextBlock();
    RUNTIME_CHECK_MSG(!cf_reader->readNextBlock(), "{}: MUST read all rows in one block!", cf.toString());
    const auto * handles_ptr = toColumnVectorDataPtr<Int64>(block.begin()->column);
    RUNTIME_CHECK_MSG(handles_ptr != nullptr, "TODO: support common handle");
    const auto & handles = *handles_ptr;
    return buildRowKeyFilterVector<Handle>(handles, delete_ranges, read_ranges, start_row_id, filter);
}

template <Int64OrString Handle>
UInt32 buildRowKeyFilterDMFile(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const std::optional<RowKeyRange> & segment_range,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const RSResults * stable_pack_res,
    const UInt32 start_row_id,
    std::vector<UInt8> & filter)
{
    auto [valid_handle_res, valid_start_pack_id]
        = getDMFilePackFilterResultBySegmentRange(dm_context, dmfile, segment_range);
    if (valid_handle_res.empty())
        return 0;

    if (stable_pack_res)
    {
        const auto & s_pack_res = *stable_pack_res;
        RUNTIME_CHECK(
            stable_pack_res->size() == valid_handle_res.size(),
            stable_pack_res->size(),
            valid_handle_res.size());
        for (UInt32 i = 0; i < valid_handle_res.size(); ++i)
            if (!s_pack_res[i].isUse())
                valid_handle_res[i] = RSResult::None;
    }

    const auto read_ranges_handle_res = getDMFilePackFilterResultByRanges(dm_context, dmfile, read_ranges);
    for (UInt32 i = 0; i < valid_handle_res.size(); ++i)
        valid_handle_res[i] = valid_handle_res[i] && read_ranges_handle_res[valid_start_pack_id + i];

    if (!delete_ranges.empty())
    {
        const auto delete_ranges_handle_res = getDMFilePackFilterResultByRanges(dm_context, dmfile, delete_ranges);
        for (UInt32 i = 0; i < valid_handle_res.size(); ++i)
            valid_handle_res[i] = valid_handle_res[i] && !delete_ranges_handle_res[valid_start_pack_id + i];
    }

    auto read_packs = std::make_shared<IdSet>();
    UInt32 need_read_rows = 0;
    std::unordered_map<UInt32, UInt32> read_pack_to_start_row_ids;

    const auto & pack_stats = dmfile->getPackStats();
    UInt32 rows = 0;
    for (UInt32 i = 0; i < valid_handle_res.size(); ++i)
    {
        const auto pack_id = valid_start_pack_id + i;
        if (!valid_handle_res[i].isUse())
        {
            std::fill_n(filter.begin() + start_row_id + rows, pack_stats[pack_id].rows, false);
        }
        else if (!valid_handle_res[i].allMatch())
        {
            read_packs->insert(pack_id);
            read_pack_to_start_row_ids.emplace(pack_id, start_row_id + rows);
            need_read_rows += pack_stats[pack_id].rows;
        }
        rows += pack_stats[pack_id].rows;
    }

    if (need_read_rows == 0)
        return rows;

    DMFileBlockInputStreamBuilder builder(dm_context.global_context);
    builder.onlyReadOnePackEveryTime().setReadPacks(read_packs).setReadTag(ReadTag::MVCC);
    auto stream = builder.build(dmfile, {getHandleColumnDefine<Handle>()}, {}, dm_context.scan_context);
    UInt32 read_rows = 0;
    for (auto pack_id : *read_packs)
    {
        auto block = stream->read();
        const auto * handles_ptr = toColumnVectorDataPtr<Int64>(block.begin()->column);
        RUNTIME_CHECK_MSG(handles_ptr != nullptr, "TODO: support common handle");
        const auto & handles = *handles_ptr;

        const auto itr = read_pack_to_start_row_ids.find(pack_id);
        RUNTIME_CHECK(itr != read_pack_to_start_row_ids.end(), read_pack_to_start_row_ids, pack_id);
        read_rows += buildRowKeyFilterVector(
            handles,
            delete_ranges,
            read_ranges,
            itr->second, // start_row_id
            filter);
    }
    RUNTIME_CHECK(read_rows == need_read_rows, read_rows, need_read_rows);
    return rows;
}

template <Int64OrString Handle>
UInt32 buildRowKeyFilterColumnFileBig(
    const DMContext & dm_context,
    const ColumnFileBig & cf_big,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const UInt32 start_row_id,
    std::vector<UInt8> & filter)
{
    if (cf_big.getRows() == 0)
        return 0;
    return buildRowKeyFilterDMFile<Handle>(
        dm_context,
        cf_big.getFile(),
        cf_big.getRange(),
        delete_ranges,
        Segment::shrinkRowKeyRanges(cf_big.getRange(), read_ranges),
        nullptr, // stable_pack_res
        start_row_id,
        filter);
}

template <Int64OrString Handle>
UInt32 buildRowKeyFilterStable(
    const DMContext & dm_context,
    const StableValueSpace::Snapshot & stable,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const RSResults & stable_pack_res,
    std::vector<UInt8> & filter)
{
    const auto & dmfiles = stable.getDMFiles();
    RUNTIME_CHECK(dmfiles.size() == 1, dmfiles.size());
    const auto & dmfile = dmfiles[0];
    if (dmfile->getPacks() == 0)
        return 0;
    return buildRowKeyFilterDMFile<Handle>(
        dm_context,
        dmfile,
        std::nullopt, // segment_range
        delete_ranges,
        read_ranges,
        &stable_pack_res,
        0, // start_row_id
        filter);
}

void buildRowKeyFilterDeleteRange(const ColumnFileDeleteRange & cf_delete_range, RowKeyRanges & delete_ranges)
{
    delete_ranges.push_back(cf_delete_range.getDeleteRange());
}
} // namespace

template <Int64OrString Handle>
void buildRowKeyFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const RSResults & stable_pack_res,
    std::vector<UInt8> & filter)
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
    // Read from new cfs to old cfs to handle delete range
    for (auto itr = cfs.rbegin(); itr != cfs.rend(); ++itr)
    {
        const auto & cf = *itr;
        if (const auto * cf_delete_range = cf->tryToDeleteRange(); cf_delete_range)
        {
            buildRowKeyFilterDeleteRange(*cf_delete_range, delete_ranges);
            continue;
        }

        const UInt32 cf_rows = cf->getRows();
        RUNTIME_CHECK(delta_rows >= read_rows + cf_rows, delta_rows, read_rows, cf_rows);
        const UInt32 start_row_id = total_rows - read_rows - cf_rows;
        read_rows += cf_rows;

        // TODO: add min-max value in tiny file to optimize rowkey filter.
        if (cf->isInMemoryFile() || cf->isTinyFile())
        {
            const auto n = buildRowKeyFilterBlock<Handle>(
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
            const auto n = buildRowKeyFilterColumnFileBig<Handle>(
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

    const auto n
        = buildRowKeyFilterStable<Handle>(dm_context, stable, delete_ranges, read_ranges, stable_pack_res, filter);
    RUNTIME_CHECK(n == stable_rows, n, stable_rows);
}

template void buildRowKeyFilter<Int64>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const RSResults & stable_pack_res,
    std::vector<UInt8> & filter);

// TODO: String
} // namespace DB::DM
