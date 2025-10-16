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

#pragma once

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/VersionChain/ColumnView.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>
#include <Storages/MutableSupport.h>

namespace DB::DM
{

namespace tests
{
class DMFileHandleIndexTest;
} // namespace tests

// DMFileHandleIndex encapsulates the handle column of a DMFile
// and provides interface for locating the position of corresponding handle.
template <ExtraHandleType HandleType>
class DMFileHandleIndex
{
public:
    using HandleRefType = typename std::conditional<std::is_same_v<HandleType, Int64>, Int64, std::string_view>::type;

    DMFileHandleIndex(
        const DMContext & dm_context_,
        const DMFilePtr & dmfile_,
        const RowID start_row_id_,
        std::optional<RowKeyRange> rowkey_range_)
        : dmfile(dmfile_)
        , start_row_id(start_row_id_)
        , rowkey_range(std::move(rowkey_range_))
        , clipped_pack_range(getClippedPackRange(dm_context_, dmfile, rowkey_range))
        , clipped_pack_index(loadPackIndex(dm_context_, *dmfile, clipped_pack_range))
        , clipped_pack_offsets(loadPackOffsets(dmfile->getPackStats(), clipped_pack_range))
        , clipped_need_read_packs(clipped_pack_range.count, 1) // read all packs by default
    {
        RUNTIME_CHECK(
            clipped_pack_index.size() == clipped_pack_range.count,
            clipped_pack_index.size(),
            clipped_pack_range.count);
        RUNTIME_CHECK(
            clipped_pack_offsets.size() == clipped_pack_range.count,
            clipped_pack_offsets.size(),
            clipped_pack_range.count);
    }

    // Get the base(oldest) version of the given handle.
    // 1. Get the pack may contain the given handle.
    // 2. Get the base version of the given handle in the given pack.
    std::optional<RowID> getBaseVersion(const DMContext & dm_context, HandleRefType h)
    {
        auto clipped_pack_id = getClippedPackId(h);
        if (!clipped_pack_id)
            return {};
        auto row_id = getBaseVersion(dm_context, h, *clipped_pack_id);
        if (!row_id)
            return {};
        return start_row_id + *row_id;
    }

    // Use to pre-calculate which packs to read.
    template <typename Iterator>
    void calculateReadPacks(Iterator begin, Iterator end)
    {
        std::vector<UInt8> calc_read_packs(clipped_pack_range.count, 0);
        UInt32 calc_read_count = 0;
        for (auto itr = begin; itr != end; ++itr)
        {
            auto clipped_pack_id = getClippedPackId(*itr);
            if (!clipped_pack_id || calc_read_packs[*clipped_pack_id])
                continue;

            calc_read_packs[*clipped_pack_id] = 1;
            ++calc_read_count;

            // Read too many packs, read all for simplicity.
            // TODO: optimize the storage format of handle column to improve the performance.
            if (calc_read_count * 4 >= clipped_pack_range.count)
                return; // return, instead of break, because `clipped_need_read_packs` is read all by default.
        }
        clipped_need_read_packs = std::move(calc_read_packs);
    }

    // Release handle column data to save memory.
    void cleanHandleColumn()
    {
        clipped_handle_packs.clear();
        std::fill(clipped_need_read_packs.begin(), clipped_need_read_packs.end(), 1); // reset to read all packs.
    }

private:
    // Find the pack may contian the given handle.
    std::optional<UInt32> getClippedPackId(HandleRefType h) const
    {
        if (unlikely(rowkey_range && !inRowKeyRange(*rowkey_range, h)))
            return {};

        auto itr = std::lower_bound(clipped_pack_index.begin(), clipped_pack_index.end(), h);
        if (itr == clipped_pack_index.end())
            return {};
        return itr - clipped_pack_index.begin();
    }

    // Find the base(oldest) version of the given handle in the given pack.
    // Note that this function return the row_id without considering the `start_row_id`
    std::optional<RowID> getBaseVersion(const DMContext & dm_context, HandleRefType h, UInt32 clipped_pack_id)
    {
        loadHandleIfNotLoaded(dm_context);
        ColumnView<HandleType> handle_col(*clipped_handle_packs[clipped_pack_id]);
        auto itr = std::lower_bound(handle_col.begin(), handle_col.end(), h);
        if (itr != handle_col.end() && *itr == h)
            return itr - handle_col.begin() + clipped_pack_offsets[clipped_pack_id];
        return {};
    }

    void loadHandleIfNotLoaded(const DMContext & dm_context)
    {
        if (likely(!clipped_handle_packs.empty()))
            return;

        auto read_pack_ids = std::make_shared<IdSet>();
        for (UInt32 i = 0; i < clipped_need_read_packs.size(); ++i)
        {
            if (clipped_need_read_packs[i])
                read_pack_ids->insert(i + clipped_pack_range.start_pack_id);
        }

        auto pack_filter = DMFilePackFilter::loadFrom(
            dm_context,
            dmfile,
            /*set_cache_if_miss*/ true,
            /*rowkey_ranges*/ {}, // Empty means all
            EMPTY_RS_OPERATOR,
            read_pack_ids);

        DMFileReader reader(
            dmfile,
            {getHandleColumnDefine<HandleType>()},
            isCommonHandle<HandleType>(),
            /*block_wait*/ false,
            /*enable_handle_clean_read*/ false,
            /*enable_del_clean_read*/ false,
            /*is_fast_scan*/ false,
            /*max_data_version*/ std::numeric_limits<UInt64>::max(),
            std::move(pack_filter),
            dm_context.global_context.getMarkCache(),
            /*enable_column_cache*/ false,
            /*column_cache*/ nullptr,
            dm_context.global_context.getSettingsRef().max_read_buffer_size,
            dm_context.global_context.getFileProvider(),
            dm_context.global_context.getReadLimiter(),
            DEFAULT_MERGE_BLOCK_SIZE,
            /*read_one_pack_every_time*/ true,
            dm_context.tracing_id,
            /*max_sharing_column_bytes_for_all*/ false,
            dm_context.scan_context,
            ReadTag::Internal);

        const auto & pack_stats = dmfile->getPackStats();
        clipped_handle_packs.resize(clipped_pack_range.count);
        for (UInt32 pack_id : *read_pack_ids)
        {
            auto block = reader.read();
            RUNTIME_CHECK(block.rows() == pack_stats[pack_id].rows, pack_id, block.rows(), pack_stats[pack_id].rows);
            clipped_handle_packs[pack_id - clipped_pack_range.start_pack_id] = block.begin()->column;
        }
    }

    struct ClippedPackRange
    {
        UInt32 start_pack_id;
        UInt32 count;
    };

    static ClippedPackRange getClippedPackRange(
        const DMContext & dm_context,
        const DMFilePtr & dmfile,
        const std::optional<const RowKeyRange> & rowkey_range)
    {
        if (!rowkey_range)
            return ClippedPackRange{.start_pack_id = 0, .count = static_cast<UInt32>(dmfile->getPacks())};

        const auto [handle_res, start_pack_id] = getClippedRSResultsByRange(dm_context, dmfile, rowkey_range);
        return ClippedPackRange{.start_pack_id = start_pack_id, .count = static_cast<UInt32>(handle_res.size())};
    }

    static std::vector<HandleType> loadPackIndex(
        const DMContext & dm_context,
        const DMFile & dmfile,
        ClippedPackRange clipped_pack_range)
    {
        auto max_values = loadPackMaxValue<HandleType>(dm_context, dmfile, MutSup::extra_handle_id);
        return std::vector<HandleType>(
            max_values.begin() + clipped_pack_range.start_pack_id,
            max_values.begin() + clipped_pack_range.start_pack_id + clipped_pack_range.count);
    }

    static std::vector<UInt32> loadPackOffsets(
        const DMFileMeta::PackStats & pack_stats,
        ClippedPackRange clipped_pack_range)
    {
        std::vector<UInt32> pack_offsets(clipped_pack_range.count, 0);
        for (UInt32 clipped_pack_id = 1; clipped_pack_id < pack_offsets.size(); ++clipped_pack_id)
        {
            UInt32 real_pack_id = clipped_pack_id + clipped_pack_range.start_pack_id;
            pack_offsets[clipped_pack_id] = pack_offsets[clipped_pack_id - 1] + pack_stats[real_pack_id - 1].rows;
        }
        return pack_offsets;
    }

    const DMFilePtr dmfile;
    const RowID start_row_id;
    const std::optional<const RowKeyRange> rowkey_range; // Range of ColumnFileBig or nullopt for Stable DMFile

    // Clipped by rowkey_range
    const ClippedPackRange clipped_pack_range;
    const std::vector<HandleType> clipped_pack_index; // max value of each pack
    const std::vector<RowID> clipped_pack_offsets; // row offset of each pack inside this file
    std::vector<ColumnPtr> clipped_handle_packs; // handle column of each pack
    // If clipped_need_read_packs[i] is 1, pack i need to be read.
    // If clipped_need_read_packs is empty, no need to read.
    std::vector<UInt8> clipped_need_read_packs;

    friend class tests::DMFileHandleIndexTest;
};

} // namespace DB::DM
