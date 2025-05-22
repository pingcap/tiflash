// Copyright 2025 PingCAP, Inc.
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

#include <ranges>

namespace DB::DM
{
UInt32 buildVersionFilterVector(
    const PaddedPODArray<UInt64> & versions,
    const UInt64 read_ts,
    const std::vector<RowID> & base_ver_snap,
    const UInt32 stable_rows,
    const UInt32 start_row_id,
    BitmapFilter & filter)
{
    UInt32 filtered_out_rows = 0;
    // Traverse data from new to old
    for (ssize_t i = versions.size() - 1; i >= 0; --i)
    {
        const UInt32 row_id = start_row_id + i;
        // Already filtered out, maybe by newer version.
        if (!filter[row_id])
            continue;

        // Invisible
        if (versions[i] > read_ts)
        {
            filter[row_id] = 0;
            ++filtered_out_rows;
            continue;
        }

        // Visible
        const auto base_row_id = base_ver_snap[row_id - stable_rows];
        // base_version is filtered out, there is newer version has been chosen
        if (base_row_id != NotExistRowID && !filter[base_row_id])
        {
            filter[row_id] = 0;
            ++filtered_out_rows;
            continue;
        }
        // Choose this version. If has based version, filter it out.
        if (base_row_id != NotExistRowID)
        {
            ++filtered_out_rows;
            filter[base_row_id] = 0;
        }
    }
    return filtered_out_rows;
}

[[nodiscard]] UInt32 buildVersionFilterBlock(
    const DMContext & dm_context,
    const IColumnFileDataProviderPtr & data_provider,
    const ColumnFile & cf,
    const UInt64 read_ts,
    const std::vector<RowID> & base_ver_snap,
    const UInt32 stable_rows,
    const UInt32 start_row_id,
    BitmapFilter & filter)
{
    assert(cf.isInMemoryFile() || cf.isTinyFile() || cf.isBigFile());
    static const auto version_cds_ptr = std::make_shared<ColumnDefines>(1, getVersionColumnDefine());
    auto cf_reader = cf.getReader(dm_context, data_provider, version_cds_ptr, ReadTag::MVCC);
    UInt32 read_block_count = 0;
    UInt32 read_rows = 0;
    UInt32 filtered_out_rows = 0;
    while (true)
    {
        auto block = cf_reader->readNextBlock();
        if (!block)
            break;

        ++read_block_count;
        read_rows += block.rows();
        const auto & versions = *toColumnVectorDataPtr<UInt64>(block.begin()->column);
        filtered_out_rows
            += buildVersionFilterVector(versions, read_ts, base_ver_snap, stable_rows, start_row_id, filter);
    }

    RUNTIME_CHECK(cf.getRows() == read_rows, cf.toString(), read_rows);

    if (cf.isInMemoryFile() || cf.isTinyFile())
        RUNTIME_CHECK_MSG(
            read_block_count == 1,
            "ColumnFile={} does not read all data in one block: read_block_count={}, read_rows={}",
            cf.toString(),
            read_block_count,
            read_rows);
    return filtered_out_rows;
}

template <ExtraHandleType HandleType>
[[nodiscard]] UInt32 buildVersionFilterDMFile(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const UInt64 read_ts,
    const UInt32 start_pack_id,
    const RSResults & rs_results, // Use to skip packs that are not used.
    const ssize_t start_row_id,
    BitmapFilter & filter,
    const LoggerPtr & log)
{
    const auto max_versions = loadPackMaxValue<UInt64>(dm_context, *dmfile, MutSup::version_col_id);

    auto need_read_packs = std::make_shared<IdSet>();
    std::unordered_map<UInt32, UInt32> start_row_id_of_need_read_packs; // pack_id -> start_row_id

    const auto & pack_stats = dmfile->getPackStats();
    UInt32 processed_rows = 0;
    for (UInt32 i = 0; i < rs_results.size(); ++i)
    {
        const UInt32 pack_id = start_pack_id + i;
        const UInt32 pack_start_row_id = start_row_id + processed_rows;
        const auto & stat = pack_stats[pack_id];
        processed_rows += stat.rows;

        // Packs that filtered out by rs_results is handled by RowKeyFilter.
        // So we just skip these packs here.
        if (!rs_results[i].isUse())
            continue;

        // `not_clean` means there have <multiple versions of the same handle> or <delete mark> in this pack.
        // Delete mark is handled by DeleteMarkFilter, so we don't read delete mark column below.
        // `max_versions[pack_id] > read_ts` means there is a version of this pack that is not visible to `read_ts`.
        if (stat.not_clean || max_versions[pack_id] > read_ts)
        {
            need_read_packs->insert(pack_id);
            start_row_id_of_need_read_packs.emplace(pack_id, pack_start_row_id);
        }
    }

    if (need_read_packs->empty())
        return 0;

    // TODO: If all packs need to read is clean, we can just read version column.
    // However, the benefits in general scenarios may not be significant.
    // For simplicity, read handle column and version column directly.
    DMFileBlockInputStreamBuilder builder(dm_context.global_context);
    builder.onlyReadOnePackEveryTime().setReadPacks(need_read_packs).setReadTag(ReadTag::MVCC);
    auto stream = builder.build(
        dmfile,
        {getHandleColumnDefine<HandleType>(), getVersionColumnDefine()},
        /*rowkey_ranges*/ {},
        dm_context.scan_context);

    UInt32 filtered_out_rows = 0;
    for (auto pack_id : *need_read_packs)
    {
        auto block = stream->read();
        RUNTIME_CHECK(block.rows() == pack_stats[pack_id].rows, block.rows(), pack_stats[pack_id].rows);
        const auto handles = ColumnView<HandleType>(*(block.getByPosition(0).column));
        const auto & versions = *toColumnVectorDataPtr<UInt64>(block.getByPosition(1).column);
        const auto itr = start_row_id_of_need_read_packs.find(pack_id);
        RUNTIME_CHECK(itr != start_row_id_of_need_read_packs.end(), start_row_id_of_need_read_packs, pack_id);
        const UInt32 pack_start_row_id = itr->second;

        if constexpr (std::is_same_v<HandleType, Int64>)
            LOG_INFO(
                log,
                "pack_id={}, max_version={}, read_ts={}, rows={}, handles=[{}, {}]",
                pack_id,
                max_versions[pack_id],
                read_ts,
                block.rows(),
                handles[0],
                handles[handles.size() - 1]);
        // Filter invisible versions
        if (max_versions[pack_id] > read_ts)
        {
            for (UInt32 i = 0; i < block.rows(); ++i)
            {
                if (filter[pack_start_row_id + i] && versions[i] > read_ts)
                {
                    filter[pack_start_row_id + i] = 0;
                    filter.version_filter_by_read_ts[pack_start_row_id + i] = 1;
                    ++filtered_out_rows;
                }
            }
        }

        // Filter multiple versions
        if (pack_stats[pack_id].not_clean)
        {
            auto handle_itr = handles.begin();
            auto handle_end = handles.end();
            for (;;)
            {
                // Search for the first consecutive equal elements
                auto itr = std::adjacent_find(handle_itr, handle_end);
                if (itr == handle_end)
                    break;

                // Let `handle_itr` point to next different handle.
                handle_itr = std::find_if(itr, handle_end, [h = *itr](const auto a) { return h != a; });
                // [itr, handle_itr) are the same handle of different versions.
                const auto count = handle_itr - itr;
                RUNTIME_CHECK(count >= 2, count);
                // `base_row_id` is the row_id of the first version of the same handle.
                // The first version is the oldest version in DMFile.
                const UInt32 base_row_id = itr - handles.begin() + pack_start_row_id;
                // If the first version is filtered out, there are two possible reasons:
                // 1. The newer version in delta has been chosen.
                // 2. It is invisiable to `read_ts`.
                // So we just filter out all versions of the same handle.
                if (!filter[base_row_id])
                {
                    if (!filter.version_filter_by_read_ts[base_row_id])
                    {
                        std::fill_n(filter.version_filter_by_delta.begin() + base_row_id, count, 1);
                    }
                    else
                    {
                        std::fill_n(filter.version_filter_by_read_ts.begin() + base_row_id, count, 1);
                    }

                    filter.set(base_row_id + 1, count - 1, false);
                    filtered_out_rows += count - 1;
                }
                else
                {
                    // Find the newest but not filtered out version.
                    // If it is invisiable to `read_ts`, it is already filtered out before.
                    // So we just get the last not filtered out version here.
                    for (UInt32 i = 1; i < count; ++i)
                    {
                        if (filter[base_row_id + i])
                        {
                            filter.version_filter_by_stable[base_row_id + i - 1] = 1;
                            filter[base_row_id + i - 1] = 0;
                            ++filtered_out_rows;
                        }
                        else
                            break;
                    }
                }
            } // for loop handling not clean pack
        } // if (pack_stats[pack_id].not_clean)
    } // for (auto pack_id : *need_read_packs)
    return filtered_out_rows;
}

template <ExtraHandleType HandleType>
[[nodiscard]] UInt32 buildVersionFilterColumnFileBig(
    const DMContext & dm_context,
    const ColumnFileBig & cf_big,
    const UInt64 read_ts,
    const ssize_t start_row_id,
    BitmapFilter & filter)
{
    auto [valid_handle_res, valid_start_pack_id]
        = getClippedRSResultsByRange(dm_context, cf_big.getFile(), cf_big.getRange());
    if (valid_handle_res.empty())
        return 0;

    return buildVersionFilterDMFile<HandleType>(
        dm_context,
        cf_big.getFile(),
        read_ts,
        valid_start_pack_id,
        valid_handle_res,
        start_row_id,
        filter,
        Logger::get(dm_context.tracing_id));
}

template <ExtraHandleType HandleType>
[[nodiscard]] UInt32 buildVersionFilterStable(
    const DMContext & dm_context,
    const StableValueSpace::Snapshot & stable,
    const UInt64 read_ts,
    const DMFilePackFilterResultPtr & stable_filter_res,
    BitmapFilter & filter,
    const LoggerPtr & log)
{
    const auto & dmfiles = stable.getDMFiles();
    RUNTIME_CHECK(dmfiles.size() == 1, dmfiles.size());
    const auto & dmfile = dmfiles[0];
    const auto & pack_res = stable_filter_res->getPackRes();
    constexpr UInt32 start_pack_id = 0;
    constexpr UInt32 start_row_id = 0;
    return buildVersionFilterDMFile<
        HandleType>(dm_context, dmfile, read_ts, start_pack_id, pack_res, start_row_id, filter, log);
}

template <ExtraHandleType HandleType>
UInt32 buildVersionFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const std::vector<RowID> & base_ver_snap,
    const UInt64 read_ts,
    const DMFilePackFilterResultPtr & stable_filter_res,
    BitmapFilter & filter)
{
    const auto & delta = *(snapshot.delta);
    const auto & stable = *(snapshot.stable);
    const UInt32 delta_rows = delta.getRows();
    const UInt32 stable_rows = stable.getDMFilesRows();
    const UInt32 total_rows = delta_rows + stable_rows;
    assert(filter.size() == total_rows);
    const auto cfs = delta.getColumnFiles();
    const auto & data_provider = delta.getDataProvider();

    UInt32 read_rows = 0;
    UInt32 filtered_out_rows = 0;
    // Read versions from new to old. Assume that the same handle is written in version order.
    // So we can read versions from new to old and filter out the older versions.
    // Raft log apply repeatly is allow, for example, <A1, A2, A3, A2, A3>.
    // But reorder is not allow, for example, <A1, A2, A3, A2, {but A3 not apply again}>.
    for (const auto & cf : cfs | std::views::reverse)
    {
        // Delete range will be handled by RowKeyFilter.
        // TODO: If we add min-max handles in ColumnFile, delete range can help skip some ColumnFiles here.
        if (cf->isDeleteRange())
            continue;

        const UInt32 cf_rows = cf->getRows();
        RUNTIME_CHECK(delta_rows >= read_rows + cf_rows, delta_rows, read_rows, cf_rows);
        const UInt32 start_row_id = total_rows - read_rows - cf_rows;
        read_rows += cf_rows;

        // TODO: add clean and max version in tiny file
        if (cf->isInMemoryFile() || cf->isTinyFile())
        {
            filtered_out_rows += buildVersionFilterBlock(
                dm_context,
                data_provider,
                *cf,
                read_ts,
                base_ver_snap,
                stable_rows,
                start_row_id,
                filter);
            continue;
        }

        if (const auto * cf_big = cf->tryToBigFile(); cf_big)
        {
            const auto offset_in_delta = start_row_id - stable_rows;
            const auto rows = cf_big->getRows();
            const bool has_base_version = std::any_of(
                base_ver_snap.begin() + offset_in_delta,
                base_ver_snap.begin() + offset_in_delta + rows,
                [](RowID row_id) { return row_id != NotExistRowID; });

            // If `​has_base_version` is ​false, it means we only need to handle version filtering ​within the DMFile.
            if (likely(!has_base_version))
            {
                filtered_out_rows
                    += buildVersionFilterColumnFileBig<HandleType>(dm_context, *cf_big, read_ts, start_row_id, filter);
            }
            else
            {
                filtered_out_rows += buildVersionFilterBlock(
                    dm_context,
                    data_provider,
                    *cf,
                    read_ts,
                    base_ver_snap,
                    stable_rows,
                    start_row_id,
                    filter);
            }
            continue;
        }
        RUNTIME_CHECK_MSG(false, "{}: unknow ColumnFile type", cf->toString());
    }
    RUNTIME_CHECK(read_rows == delta_rows, read_rows, delta_rows);
    filtered_out_rows
        += buildVersionFilterStable<HandleType>(dm_context, stable, read_ts, stable_filter_res, filter, snapshot.log);
    return filtered_out_rows;
}

template UInt32 buildVersionFilter<Int64>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const std::vector<RowID> & base_ver_snap,
    const UInt64 read_ts,
    const DMFilePackFilterResultPtr & stable_filter_res,
    BitmapFilter & filter);

template UInt32 buildVersionFilter<String>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const std::vector<RowID> & base_ver_snap,
    const UInt64 read_ts,
    const DMFilePackFilterResultPtr & stable_filter_res,
    BitmapFilter & filter);
} // namespace DB::DM
