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

#include <Storages/DeltaMerge/VersionChain/RowKeyFilter.h>

namespace DB::DM
{
namespace
{
template <Int64OrString Handle>
UInt32 buildRowKeyFilterVector(
    std::span<const Handle> handles,
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
    auto handles_col = block.begin()->column;
    const auto * handles = toColumnVectorDataPtr<Int64>(handles_col);
    RUNTIME_CHECK_MSG(handles != nullptr, "TODO: support common handle");
    return buildRowKeyFilterVector<Handle>(
        std::span{handles->data(), handles->size()},
        delete_ranges,
        read_ranges,
        start_row_id,
        filter);
}

template <Int64OrString Handle>
UInt32 buildRowKeyFilterDMFile(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const RowKeyRanges & segment_ranges,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
    const UInt32 start_row_id,
    std::vector<UInt8> & filter)
{
    auto seg_range_pack_filter = DMFilePackFilter::loadFrom(
        dmfile,
        dm_context.global_context.getMinMaxIndexCache(),
        true,
        segment_ranges,
        EMPTY_RS_OPERATOR,
        {},
        dm_context.global_context.getFileProvider(),
        dm_context.getReadLimiter(),
        dm_context.scan_context,
        dm_context.tracing_id,
        ReadTag::MVCC);
    const auto & seg_range_handle_res = seg_range_pack_filter.getHandleRes();
    const auto valid_start_itr
        = std::find_if(seg_range_handle_res.begin(), seg_range_handle_res.end(), [](RSResult r) { return r.isUse(); });
    RUNTIME_CHECK_MSG(
        valid_start_itr != seg_range_handle_res.end(),
        "dmfile={}, segment_ranges={}, start_row_id={}",
        dmfile->path(),
        toDebugString(segment_ranges),
        start_row_id);
    const auto valid_end_itr
        = std::find_if(valid_start_itr, seg_range_handle_res.end(), [](RSResult r) { return !r.isUse(); });
    const auto valid_start_pack_id = valid_start_itr - seg_range_handle_res.begin();
    const auto valid_pack_count = valid_end_itr - valid_start_itr;
    RSResults valid_pack_handle_res(valid_start_itr, valid_end_itr);

    auto read_ranges_pack_filter = DMFilePackFilter::loadFrom(
        dmfile,
        dm_context.global_context.getMinMaxIndexCache(),
        true,
        read_ranges,
        EMPTY_RS_OPERATOR,
        {},
        dm_context.global_context.getFileProvider(),
        dm_context.getReadLimiter(),
        dm_context.scan_context,
        dm_context.tracing_id,
        ReadTag::MVCC);
    const auto & read_ranges_handle_res = read_ranges_pack_filter.getHandleRes();
    for (auto i = 0; i < valid_pack_count; ++i)
    {
        valid_pack_handle_res[i] = valid_pack_handle_res[i] && read_ranges_handle_res[valid_start_pack_id + i];
    }

    if (!delete_ranges.empty())
    {
        auto delete_ranges_pack_filter = DMFilePackFilter::loadFrom(
            dmfile,
            dm_context.global_context.getMinMaxIndexCache(),
            true,
            delete_ranges,
            EMPTY_RS_OPERATOR,
            {},
            dm_context.global_context.getFileProvider(),
            dm_context.getReadLimiter(),
            dm_context.scan_context,
            dm_context.tracing_id,
            ReadTag::MVCC);
        const auto & delete_ranges_handle_res = delete_ranges_pack_filter.getHandleRes();
        for (auto i = 0; i < valid_pack_count; ++i)
        {
            valid_pack_handle_res[i] = valid_pack_handle_res[i] && !delete_ranges_handle_res[valid_start_pack_id + i];
        }
    }

    auto read_packs = std::make_shared<IdSet>();
    UInt32 need_read_rows = 0;
    std::unordered_map<UInt32, UInt32> read_pack_to_start_row_ids;

    const auto & pack_stats = dmfile->getPackStats();
    UInt32 rows = 0;
    for (auto i = 0; i < valid_pack_count; ++i)
    {
        const auto pack_id = valid_start_pack_id + i;
        if (!valid_pack_handle_res[i].isUse())
        {
            std::fill_n(filter.begin() + start_row_id + rows, pack_stats[pack_id].rows, false);
        }
        else if (!valid_pack_handle_res[i].allMatch())
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
    builder.setRowsThreshold(need_read_rows).setReadPacks(read_packs).setReadTag(ReadTag::MVCC);
    auto stream = builder.build(dmfile, {getHandleColumnDefine<Handle>()}, {}, dm_context.scan_context);

    auto block = stream->read();
    RUNTIME_CHECK(block.rows() == need_read_rows, block.rows(), need_read_rows);
    auto handle_col = block.begin()->column;
    const auto * handles = toColumnVectorDataPtr<Int64>(handle_col);
    RUNTIME_CHECK_MSG(handles != nullptr, "TODO: support common handle");

    UInt32 offset = 0;
    for (auto pack_id : *read_packs)
    {
        const auto itr = read_pack_to_start_row_ids.find(pack_id);
        RUNTIME_CHECK(itr != read_pack_to_start_row_ids.end(), read_pack_to_start_row_ids, pack_id);
        offset += buildRowKeyFilterVector(
            std::span{handles->data() + offset, pack_stats[pack_id].rows},
            delete_ranges,
            read_ranges,
            itr->second,
            filter);
    }
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
        {cf_big.getRange()},
        delete_ranges,
        read_ranges,
        start_row_id,
        filter);
}

template <Int64OrString Handle>
UInt32 buildRowKeyFilterStable(
    const DMContext & dm_context,
    const StableValueSpace::Snapshot & stable,
    const RowKeyRanges & delete_ranges,
    const RowKeyRanges & read_ranges,
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
        {}, // empty ranges means all
        delete_ranges,
        read_ranges,
        0, // start_row_id
        filter);
}

void buildRowKeyFilterDeleteRange(const ColumnFileDeleteRange & cf_delete_range, RowKeyRanges & delete_ranges)
{
    delete_ranges.push_back(cf_delete_range.getDeleteRange());
}
} // namespace

template <Int64OrString Handle>
std::vector<UInt8> buildRowKeyFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges)
{
    const auto & delta = *(snapshot.delta);
    const auto & stable = *(snapshot.stable);
    const UInt32 delta_rows = delta.getRows();
    const UInt32 stable_rows = stable.getDMFilesRows();
    const UInt32 total_rows = delta_rows + stable_rows;
    std::vector<UInt8> filter(total_rows, /*default_value*/ 1);

    auto cfs = delta.getPersistedFileSetSnapshot()->getColumnFiles();
    const auto & memory_cfs = delta.getMemTableSetSnapshot()->getColumnFiles();
    cfs.insert(cfs.end(), memory_cfs.begin(), memory_cfs.end());

    auto storage_snap = std::make_shared<StorageSnapshot>(
        *dm_context.storage_pool,
        dm_context.getReadLimiter(),
        dm_context.tracing_id,
        /*snapshot_read*/ true);
    auto data_from_storage_snap = ColumnFileDataProviderLocalStoragePool::create(storage_snap);

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
                data_from_storage_snap,
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

    const auto n = buildRowKeyFilterStable<Handle>(dm_context, stable, delete_ranges, read_ranges, filter);
    RUNTIME_CHECK(n == stable_rows, n, stable_rows);
    return filter;
}

template std::vector<UInt8> buildRowKeyFilter<Int64>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges);

// TODO: String
} // namespace DB::DM
