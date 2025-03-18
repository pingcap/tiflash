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
        , clipped_pack_range(getPackRange(dm_context_))
        , clipped_pack_index(loadPackIndex(dm_context_))
        , clipped_pack_offsets(loadPackOffsets())
        , clipped_handle_packs(clipped_pack_range.count())
        , clipped_need_read_packs(std::vector<UInt8>(clipped_pack_range.count(), 1)) // read all packs by default
    {
        RUNTIME_CHECK(
            clipped_pack_index.size() == clipped_pack_range.count(),
            clipped_pack_index.size(),
            clipped_pack_range.count());
        RUNTIME_CHECK(
            clipped_pack_offsets.size() == clipped_pack_range.count(),
            clipped_pack_offsets.size(),
            clipped_pack_range.count());
    }

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

    template <typename Iterator>
    void calculateReadPacks(Iterator begin, Iterator end)
    {
        std::vector<UInt8> calc_read_packs(clipped_pack_range.count(), 0);
        UInt32 calc_read_count = 0;
        for (auto itr = begin; itr != end; ++itr)
        {
            auto clipped_pack_id = getClippedPackId(*itr);
            if (!clipped_pack_id || calc_read_packs[*clipped_pack_id])
                continue;

            calc_read_packs[*clipped_pack_id] = 1;
            ++calc_read_count;

            // Read too many packs, read all by default
            if (calc_read_count * 4 >= clipped_pack_range.count())
                return; // return, instead of break, because `clipped_need_read_packs` is read all by default.
        }
        clipped_need_read_packs->swap(calc_read_packs);
    }

    void cleanHandleColumn()
    {
        // Reset handle column data to save memory.
        std::fill(clipped_handle_packs.begin(), clipped_handle_packs.end(), nullptr);
        clipped_need_read_packs.emplace(clipped_pack_range.count(), 1);
    }

private:
    std::optional<UInt32> getClippedPackId(HandleRefType h)
    {
        if (unlikely(rowkey_range && !inRowKeyRange(*rowkey_range, h)))
            return {};

        auto itr = std::lower_bound(clipped_pack_index.begin(), clipped_pack_index.end(), h);
        if (itr == clipped_pack_index.end())
            return {};
        return itr - clipped_pack_index.begin();
    }

    std::optional<RowID> getBaseVersion(const DMContext & dm_context, HandleRefType h, UInt32 clipped_pack_id)
    {
        loadHandleIfNotLoaded(dm_context);
        ColumnView<HandleType> handle_col(*clipped_handle_packs[clipped_pack_id]);
        auto itr = std::lower_bound(handle_col.begin(), handle_col.end(), h);
        if (itr != handle_col.end() && *itr == h)
            return itr - handle_col.begin() + clipped_pack_offsets[clipped_pack_id];
        return {};
    }

    std::vector<HandleType> loadPackIndex(const DMContext & dm_context)
    {
        auto max_values = loadPackMaxValue<HandleType>(dm_context.global_context, *dmfile, MutSup::extra_handle_id);
        return std::vector<HandleType>(
            max_values.begin() + clipped_pack_range.start_pack_id,
            max_values.begin() + clipped_pack_range.end_pack_id);
    }

    std::vector<UInt32> loadPackOffsets()
    {
        const auto & pack_stats = dmfile->getPackStats();
        std::vector<UInt32> pack_offsets(clipped_pack_range.count(), 0);
        for (UInt32 clipped_pack_id = 1; clipped_pack_id < pack_offsets.size(); ++clipped_pack_id)
        {
            UInt32 real_pack_id = clipped_pack_id + clipped_pack_range.start_pack_id;
            pack_offsets[clipped_pack_id] = pack_offsets[clipped_pack_id - 1] + pack_stats[real_pack_id - 1].rows;
        }
        return pack_offsets;
    }

    static bool isCommonHandle() { return std::is_same_v<HandleType, String>; }

    void loadHandleIfNotLoaded(const DMContext & dm_context)
    {
        if (likely(!clipped_need_read_packs))
            return;

        auto read_pack_ids = std::make_shared<IdSet>();
        const auto & packs = *clipped_need_read_packs;
        for (UInt32 i = 0; i < packs.size(); ++i)
        {
            if (packs[i])
                read_pack_ids->insert(i + clipped_pack_range.start_pack_id);
        }

        auto pack_filter = DMFilePackFilter::loadFrom(
            dm_context,
            dmfile,
            true, //set_cache_if_miss
            {}, // rowkey_ranges, empty means all
            nullptr, // RSOperatorPtr
            read_pack_ids);

        DMFileReader reader(
            dmfile,
            {getHandleColumnDefine<HandleType>()},
            isCommonHandle(),
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
            ReadTag::MVCC);


        for (UInt32 pack_id : *read_pack_ids)
        {
            auto block = reader.read();
            clipped_handle_packs[pack_id - clipped_pack_range.start_pack_id] = block.begin()->column;
        }
        clipped_need_read_packs.reset();
    }

    struct PackRange
    {
        // [start_pack_id, end_pack_id)
        UInt32 start_pack_id;
        UInt32 end_pack_id;

        UInt32 count() const { return end_pack_id - start_pack_id; }
    };

    PackRange getPackRange(const DMContext & dm_context)
    {
        if (!rowkey_range)
            return PackRange{.start_pack_id = 0, .end_pack_id = static_cast<UInt32>(dmfile->getPacks())};

        const auto [handle_res, start_pack_id] = getClippedRSResultsByRanges(dm_context, dmfile, rowkey_range);
        return PackRange{
            .start_pack_id = start_pack_id,
            .end_pack_id = start_pack_id + static_cast<UInt32>(handle_res.size())};
    }

    const DMFilePtr dmfile;
    const RowID start_row_id;
    const std::optional<const RowKeyRange> rowkey_range; // Range of ColumnFileBig or nullopt for Stable DMFile

    // Clipped by rowkey_range
    const PackRange clipped_pack_range;
    const std::vector<HandleType> clipped_pack_index; // max value of each pack
    const std::vector<UInt32> clipped_pack_offsets; // offset of each pack
    std::vector<ColumnPtr> clipped_handle_packs; // handle column of each pack
    std::optional<std::vector<UInt8>> clipped_need_read_packs;
};

} // namespace DB::DM
