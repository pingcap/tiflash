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
#include <Storages/DeltaMerge/VersionChain/VersionFilter.h>

namespace DB::DM
{
[[nodiscard]] UInt32 buildVersionFilterBlock(
    const DMContext & dm_context,
    const IColumnFileDataProviderPtr & data_provider,
    const ColumnFile & cf,
    const UInt64 read_ts,
    const std::vector<RowID> & base_ver_snap,
    const UInt32 stable_rows,
    const UInt32 start_row_id,
    IColumn::Filter & filter)
{
    assert(cf.isInMemoryFile() || cf.isTinyFile());
    auto cf_reader = cf.getReader(dm_context, data_provider, getVersionColumnDefinesPtr(), ReadTag::MVCC);
    auto block = cf_reader->readNextBlock();
    RUNTIME_CHECK_MSG(
        cf.getRows() == block.rows(),
        "ColumnFile<{}> returns {} rows. Read all rows in one block is required!",
        cf.toString(),
        block.rows());
    if (!block)
        return 0;

    const auto & versions = *toColumnVectorDataPtr<UInt64>(block.begin()->column); // Must success
    // Traverse data from new to old
    for (ssize_t i = versions.size() - 1; i >= 0; --i)
    {
        const UInt32 row_id = start_row_id + i;
        // Already filtered ou, maybe by RowKeyFilter
        if (!filter[row_id])
            continue;

        // Invisible
        if (versions[i] > read_ts)
        {
            filter[row_id] = 0;
            continue;
        }

        // Visible
        const auto base_row_id = base_ver_snap[row_id - stable_rows];
        // Newer version has been chosen
        if (base_row_id != NotExistRowID && !filter[base_row_id])
        {
            filter[row_id] = 0;
            continue;
        }
        // Choose this version. If has based version, filter it out.
        if (base_row_id != NotExistRowID)
            filter[base_row_id] = 0;
    }
    return versions.size();
}

template <HandleType Handle>
[[nodiscard]] UInt32 buildVersionFilterDMFile(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const std::optional<RowKeyRange> & segment_range,
    const UInt64 read_ts,
    const ssize_t start_row_id,
    IColumn::Filter & filter)
{
    auto [valid_handle_res, valid_start_pack_id] = getClippedRSResultsByRanges(dm_context, dmfile, segment_range);
    if (valid_handle_res.empty())
        return 0;

    const auto max_versions = loadPackMaxValue<UInt64>(dm_context.global_context, *dmfile, MutSup::version_col_id);

    auto need_read_packs = std::make_shared<IdSet>();
    UInt32 need_read_rows = 0;
    std::unordered_map<UInt32, UInt32> need_read_pack_to_start_row_ids;

    const auto & pack_stats = dmfile->getPackStats();
    UInt32 processed_rows = 0;
    for (UInt32 i = 0; i < valid_handle_res.size(); ++i)
    {
        const UInt32 pack_id = valid_start_pack_id + i;
        const UInt32 pack_start_row_id = start_row_id + processed_rows;
        const auto & stat = pack_stats[pack_id];
        if (stat.not_clean || max_versions[pack_id] > read_ts)
        {
            need_read_packs->insert(pack_id);
            need_read_pack_to_start_row_ids.emplace(pack_id, pack_start_row_id);
            need_read_rows += stat.rows;
        }
        processed_rows += stat.rows;
    }

    if (need_read_rows == 0)
        return processed_rows;

    // If all packs need to read is clean, we can just read version column.
    // However, the benefits in general scenarios may not be significant.
    // For simplicity, read handle column and version column directly.
    DMFileBlockInputStreamBuilder builder(dm_context.global_context);
    builder.onlyReadOnePackEveryTime().setReadPacks(need_read_packs).setReadTag(ReadTag::MVCC);
    auto stream = builder.build(
        dmfile,
        {getHandleColumnDefine<Handle>(), getVersionColumnDefine()},
        {},
        dm_context.scan_context);

    UInt32 read_rows = 0;
    for (auto pack_id : *need_read_packs)
    {
        auto block = stream->read();
        RUNTIME_CHECK(block.rows() == pack_stats[pack_id].rows, block.rows(), pack_stats[pack_id].rows);
        read_rows += block.rows();
        const auto handles = ColumnView<Handle>(*(block.getByPosition(0).column));
        const auto & versions = *toColumnVectorDataPtr<UInt64>(block.getByPosition(1).column); // Must success.
        const auto itr = need_read_pack_to_start_row_ids.find(pack_id);
        RUNTIME_CHECK(itr != need_read_pack_to_start_row_ids.end(), need_read_pack_to_start_row_ids, pack_id);
        const UInt32 pack_start_row_id = itr->second;

        // Filter invisible versions
        if (max_versions[pack_id] > read_ts)
        {
            for (UInt32 i = 0; i < block.rows(); ++i)
                filter[pack_start_row_id + i] = versions[i] <= read_ts;
        }

        // Filter multiple versions
        if (pack_stats[pack_id].not_clean)
        {
            auto handle_itr = handles.begin();
            auto handle_end = handles.end();
            for (;;)
            {
                // Search for the first two consecutive equal elements
                auto itr = std::adjacent_find(handle_itr, handle_end);
                if (itr == handle_end)
                    break;

                // Let `handle_itr` point to next different handle.
                handle_itr = std::find_if(itr, handle_end, [h = *itr](auto a) { return h != a; });
                // [itr, handle_itr) are the same handle of different verions.
                auto count = handle_itr - itr;
                RUNTIME_CHECK(count >= 2, count);

                const UInt32 base_row_id = itr - handles.begin() + pack_start_row_id;
                if (!filter[base_row_id])
                {
                    std::fill_n(filter.begin() + base_row_id + 1, count - 1, 0);
                    continue;
                }
                else
                {
                    // Find the newest but not filtered out version.
                    // If it is invisiable to `read_ts`, it already filtered out before.
                    // So we just get the last not filtered out version here.
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
    RUNTIME_CHECK(read_rows == need_read_rows, read_rows, need_read_rows);
    return processed_rows;
}

template <HandleType Handle>
[[nodiscard]] UInt32 buildVersionFilterColumnFileBig(
    const DMContext & dm_context,
    const ColumnFileBig & cf_big,
    const UInt64 read_ts,
    const ssize_t start_row_id,
    IColumn::Filter & filter)
{
    return buildVersionFilterDMFile<Handle>(
        dm_context,
        cf_big.getFile(),
        cf_big.getRange(),
        read_ts,
        start_row_id,
        filter);
}

template <HandleType Handle>
[[nodiscard]] UInt32 buildVersionFilterStable(
    const DMContext & dm_context,
    const StableValueSpace::Snapshot & stable,
    const UInt64 read_ts,
    IColumn::Filter & filter)
{
    const auto & dmfiles = stable.getDMFiles();
    RUNTIME_CHECK(dmfiles.size() == 1, dmfiles.size());
    return buildVersionFilterDMFile<Handle>(dm_context, dmfiles[0], std::nullopt, read_ts, 0, filter);
}

template <HandleType Handle>
void buildVersionFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const std::vector<RowID> & base_ver_snap,
    const UInt64 read_ts,
    IColumn::Filter & filter)
{
    const auto & delta = *(snapshot.delta);
    const auto & stable = *(snapshot.stable);
    const UInt32 delta_rows = delta.getRows();
    const UInt32 stable_rows = stable.getDMFilesRows();
    const UInt32 total_rows = delta_rows + stable_rows;
    RUNTIME_CHECK(filter.size() == total_rows, filter.size(), total_rows);

    // Delta MVCC
    const auto cfs = delta.getColumnFiles();
    const auto & data_provider = delta.getDataProvider();

    UInt32 read_rows = 0;
    // Read versions from new to old
    for (auto itr = cfs.rbegin(); itr != cfs.rend(); ++itr)
    {
        const auto & cf = *itr;
        if (cf->isDeleteRange()) // Delete range is handled by RowKeyFilter.
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
                data_provider,
                *cf,
                read_ts,
                base_ver_snap,
                stable_rows,
                start_row_id,
                filter);
            RUNTIME_CHECK(cf_rows == n, cf_rows, n);
            continue;
        }

        if (const auto * cf_big = cf->tryToBigFile(); cf_big)
        {
            const auto n = buildVersionFilterColumnFileBig<Handle>(dm_context, *cf_big, read_ts, start_row_id, filter);
            RUNTIME_CHECK(cf_rows == n, cf_rows, n);
            continue;
        }
        RUNTIME_CHECK_MSG(false, "{}: unknow ColumnFile type", cf->toString());
    }
    RUNTIME_CHECK(read_rows == delta_rows, read_rows, delta_rows);
    const auto n = buildVersionFilterStable<Handle>(dm_context, stable, read_ts, filter);
    RUNTIME_CHECK(n == stable_rows, n, stable_rows);
}

template void buildVersionFilter<Int64>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const std::vector<RowID> & base_ver_snap,
    const UInt64 read_ts,
    IColumn::Filter & filter);

template void buildVersionFilter<String>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const std::vector<RowID> & base_ver_snap,
    const UInt64 read_ts,
    IColumn::Filter & filter);
} // namespace DB::DM
