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
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>
#include <Storages/DeltaMerge/VersionChain/VersionFilter.h>

namespace DB::DM
{
UInt32 buildVersionFilterBlock(
    const DMContext & dm_context,
    const IColumnFileDataProviderPtr & data_provider,
    const ColumnFile & cf,
    const UInt64 read_ts,
    const std::vector<RowID> & base_ver_snap,
    const UInt32 start_row_id,
    std::vector<UInt8> & filter)
{
    assert(cf.isInMemoryFile() || cf.isTinyFile());
    auto cf_reader = cf.getReader(dm_context, data_provider, getVersionColumnDefinesPtr(), ReadTag::MVCC);
    auto block = cf_reader->readNextBlock();
    RUNTIME_CHECK_MSG(!cf_reader->readNextBlock(), "{}: read all rows in one block is required!", cf.toString());
    auto version_col = block.begin()->column;
    const auto & versions = *toColumnVectorDataPtr<UInt64>(version_col); // Must success.
    // Traverse data from new to old
    for (ssize_t i = versions.size() - 1; i >= 0; --i)
    {
        const UInt32 row_id = start_row_id + i;
        // Cannot see newer tranctions.
        if (versions[row_id] > read_ts)
        {
            filter[row_id] = 0;
            continue;
        }

        // Newer version is chosen and is base version.
        if (!filter[row_id])
        {
            continue;
        }

        const auto base_row_id = base_ver_snap[row_id];
        if (base_row_id == NotExistRowID)
        {
            continue;
        }

        if (!filter[base_row_id])
        {
            filter[row_id] = 0;
        }

        filter[base_row_id] = 0;
    }
    return versions.size();
}

UInt32 buildVersionFilterDMFile(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const RowKeyRanges & segment_ranges,
    const UInt64 read_ts,
    const ssize_t start_row_id,
    std::vector<UInt8> & filter)
{
    auto pack_filter = DMFilePackFilter::loadFrom(
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
    const auto & seg_range_handle_res = pack_filter.getHandleRes();
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

    auto read_packs = std::make_shared<IdSet>();
    UInt32 need_read_rows = 0;
    std::unordered_map<UInt32, UInt32> read_pack_to_start_row_ids;

    const auto & pack_stats = dmfile->getPackStats();
    UInt32 rows = 0;
    for (UInt32 i = 0; i < valid_pack_count; ++i)
    {
        const UInt32 pack_id = valid_start_pack_id + i;
        const UInt32 pack_start_row_id = start_row_id + rows;
        const auto & stat = pack_stats[pack_id];
        if (stat.not_clean || pack_filter.getMaxVersion(pack_id) > read_ts)
        {
            read_packs->insert(pack_id);
            read_pack_to_start_row_ids.emplace(pack_id, pack_start_row_id);
            need_read_rows += stat.rows;
        }
        rows += stat.rows;
    }

    if (need_read_rows == 0)
        return rows;

    // If all packs need to read is clean, we can just read version column.
    // However, the benefits in general scenarios may not be significant.

    DMFileBlockInputStreamBuilder builder(dm_context.global_context);
    builder.setRowsThreshold(need_read_rows).setReadPacks(read_packs).setReadTag(ReadTag::MVCC);
    auto stream = builder.build(
        dmfile,
        {getHandleColumnDefine<Handle>(), getVersionColumnDefine()},
        {},
        dm_context.scan_context);
    auto block = stream->read();
    RUNTIME_CHECK(block.rows() == need_read_rows, block.rows(), need_read_rows);

    auto handle_col = block.getByName(EXTRA_HANDLE_COLUMN_NAME).column;
    const auto * handles_ptr = toColumnVectorDataPtr<Int64>(handle_col);
    RUNTIME_CHECK_MSG(handles_ptr != nullptr, "TODO: support common handle");
    const auto & handles = *handles_ptr;

    auto version_col = block.getByName(VERSION_COLUMN_NAME).column;
    const auto & versions = *toColumnVectorDataPtr<UInt64>(version_col); // Must success.

    UInt32 offset = 0;
    for (auto pack_id : *read_packs)
    {
        const auto itr = read_pack_to_start_row_ids.find(pack_id);
        RUNTIME_CHECK(itr != read_pack_to_start_row_ids.end(), read_pack_to_start_row_ids, pack_id);
        const UInt32 pack_start_row_id = itr->second;

        // Filter invisible versions
        if (pack_filter.getMaxVersion(pack_id) > read_ts)
        {
            for (UInt32 i = 0; i < pack_stats[pack_id].rows; ++i)
            {
                // TODO: benchmark
                // filter[pack_start_row_id + i] = versions[offset + i] <= read_ts;
                if unlikely (versions[offset + i] > read_ts)
                    filter[pack_start_row_id + i] = 0;
            }
        }

        // Filter multiple versions
        if (pack_stats[pack_id].not_clean)
        {
            // [handle_itr, handle_end) is a pack.
            auto handle_itr = handles.begin() + offset;
            const auto handle_end = handle_itr + pack_stats[pack_id].rows;
            for (;;)
            {
                auto itr = std::adjacent_find(handle_itr, handle_end);
                if (itr == handle_end)
                    break;

                // Let `handle_itr` point to next different handle.
                handle_itr = std::find_if(itr, handle_end, [h = *itr](Int64 a) { return h != a; });
                // [itr, handle_itr) are the same handle of different verions.
                auto count = std::distance(itr, handle_itr);

                const UInt32 base_row_id = pack_start_row_id + std::distance(handles.begin(), itr);
                if (!filter[base_row_id])
                {
                    std::fill_n(filter.begin() + base_row_id + 1, count - 1, 0);
                    continue;
                }
                else
                {
                    for (UInt32 i = 1; i < count; ++i)
                    {
                        if (filter[base_row_id + i])
                            filter[base_row_id + i - 1] = 0;
                        else
                            break;
                    }
                }
            }
        }
    }
    return rows;
}

UInt32 buildVersionFilterColumnFileBig(
    const DMContext & dm_context,
    const ColumnFileBig & cf_big,
    const UInt64 read_ts,
    const ssize_t start_row_id,
    std::vector<UInt8> & filter)
{
    auto dmfile = cf_big.getFile();
    auto segment_ranges = RowKeyRanges{cf_big.getRange()};
    return buildVersionFilterDMFile(dm_context, dmfile, segment_ranges, read_ts, start_row_id, filter);
}

UInt32 buildVersionFilterStable(
    const DMContext & dm_context,
    const StableValueSpace::Snapshot & stable,
    const UInt64 read_ts,
    std::vector<UInt8> & filter)
{
    const auto & dmfiles = stable.getDMFiles();
    RUNTIME_CHECK(dmfiles.size() == 1, dmfiles.size());
    return buildVersionFilterDMFile(dm_context, dmfiles[0], {}, read_ts, 0, filter);
}

std::vector<UInt8> buildVersionFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const std::vector<RowID> & base_ver_snap,
    const UInt64 read_ts)
{
    const auto & delta = *(snapshot.delta);
    const auto & stable = *(snapshot.stable);
    const UInt32 delta_rows = delta.getRows();
    const UInt32 stable_rows = stable.getRows();
    const UInt32 total_rows = delta_rows + stable_rows;
    std::vector<UInt8> filter(total_rows, /*default_value*/ 1);

    // Delta MVCC
    auto cfs = delta.getPersistedFileSetSnapshot()->getColumnFiles();
    const auto & memory_cfs = delta.getMemTableSetSnapshot()->getColumnFiles();
    cfs.insert(cfs.end(), memory_cfs.begin(), memory_cfs.end());

    auto storage_snap = std::make_shared<StorageSnapshot>(
        *dm_context.storage_pool,
        dm_context.getReadLimiter(),
        dm_context.tracing_id,
        /*snapshot_read*/ true);
    auto data_from_storage_snap = ColumnFileDataProviderLocalStoragePool::create(storage_snap);

    UInt32 read_rows = 0;

    // Traverse data from new to old
    for (auto itr = cfs.rbegin(); itr != cfs.rend(); ++itr)
    {
        const auto & cf = *itr;
        if (cf->isDeleteRange())
            continue;

        const UInt32 cf_rows = cf->getRows();
        RUNTIME_CHECK(delta_rows >= read_rows + cf_rows, delta_rows, read_rows, cf_rows);
        const UInt32 start_row_id = total_rows - read_rows - cf_rows;
        read_rows += cf_rows;

        // TODO: add clean and max version in tiny file
        if (cf->isInMemoryFile() || cf->isTinyFile())
        {
            const auto n = buildVersionFilterBlock(
                dm_context,
                data_from_storage_snap,
                *cf,
                read_ts,
                base_ver_snap,
                start_row_id,
                filter);
            RUNTIME_CHECK(cf_rows == n, cf_rows, n);
            continue;
        }

        if (const auto * cf_big = cf->tryToBigFile(); cf_big)
        {
            const auto n = buildVersionFilterColumnFileBig(dm_context, *cf_big, read_ts, start_row_id, filter);
            RUNTIME_CHECK(cf_rows == n, cf_rows, n);
            continue;
        }
        RUNTIME_CHECK_MSG(false, "{}: unknow ColumnFile type", cf->toString());
    }
    RUNTIME_CHECK(read_rows == delta_rows, read_rows, delta_rows);
    const auto n = buildVersionFilterStable(dm_context, stable, read_ts, filter);
    RUNTIME_CHECK(n == stable_rows, n, stable_rows);
    return filter;
}
} // namespace DB::DM
