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

#pragma once

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>

namespace DB::DM
{

template <Int64OrString Handle>
class DMFileHandleIndex
{
public:
    DMFileHandleIndex(
        const DMContext & dm_context_,
        const DMFilePtr & dmfile_,
        const RowID start_row_id_,
        std::optional<RowKeyRange> rowkey_range_)
        : global_context(dm_context_.global_context)
        , dmfile(dmfile_)
        , start_row_id(start_row_id_)
        , rowkey_range(std::move(rowkey_range_))
        , pack_range(getPackRange(dm_context_))
        , pack_index(loadPackIndex())
        , pack_entries(loadPackEntries())
        , handle_packs(pack_range.count())
        , need_read_packs(std::vector<UInt8>(pack_range.count(), 1))  // read all packs by default
    {
        RUNTIME_CHECK(pack_index.size() == pack_range.count(), pack_index.size(), pack_range.count());
        RUNTIME_CHECK(pack_entries.size() == pack_range.count(), pack_entries.size(), pack_range.count());
    }

    template <Int64OrStringView HandleView>
    std::optional<RowID> getBaseVersion(HandleView h)
    {
        auto pa = getPackEntry(h);
        if (!pa)
            return {};
        auto row_id = getBaseVersion(h, pa->first, pa->second.offset);
        if (!row_id)
            return {};
        return start_row_id + *row_id;
    }

    void calculateReadPacks(const PaddedPODArray<Handle> & handles)
    {
        std::vector<UInt8> calc_read_packs(pack_range.count(), 0);
        UInt32 calc_read_count = 0;
        for (Handle h : handles)
        {
            auto pa = getPackEntry(h);
            if (!pa || calc_read_packs[pa->first])
                continue;
        
            calc_read_packs[pa->first] = 1;
            ++calc_read_count;
            
            // Read too many packs, read all by default
            if (calc_read_count * 4 >= pack_range.count())
                return;
        }
        need_read_packs->swap(calc_read_packs);
    }

    void cleanHandleColumn()
    {
        // Reset handle column data to save memory.
        std::fill(handle_packs.begin(), handle_packs.end(), nullptr);
        need_read_packs.emplace(pack_range.count(), 1);
    }

private:
    struct PackEntry
    {
        UInt32 offset;
        UInt32 rows;
    };

    template <Int64OrStringView HandleView>
    std::optional<std::pair<UInt32, PackEntry>> getPackEntry(HandleView h)
    {
        if unlikely (rowkey_range && !inRowKeyRange(*rowkey_range, h))
            return {};

        auto itr = std::lower_bound(pack_index.begin(), pack_index.end(), h);
        if (itr == pack_index.end())
            return {};
        return std::make_pair(itr - pack_index.begin(), pack_entries[itr - pack_index.begin()]);
    }

    template <Int64OrStringView HandleView>
    std::optional<RowID> getBaseVersion(HandleView h, UInt32 pack_id, UInt32 offset)
    {
        loadHandleIfNotLoaded();
        const auto & handle_col = handle_packs[pack_id];
        const auto * handles = toColumnVectorDataPtr<Int64>(handle_col);
        RUNTIME_CHECK_MSG(handles != nullptr, "TODO: support common handle");
        auto itr = std::lower_bound(handles->begin(), handles->end(), h);
        if (itr != handles->end() && *itr == h)
        {
            return itr - handles->begin() + offset;
        }
        return {};
    }

    std::vector<Handle> loadPackIndex()
    {
        auto max_values = loadPackMaxValue<Handle>(global_context, *dmfile, EXTRA_HANDLE_COLUMN_ID);
        return std::vector<Handle>(
            max_values.begin() + pack_range.start_pack_id,
            max_values.begin() + pack_range.end_pack_id);
    }

    std::vector<PackEntry> loadPackEntries()
    {
        const auto & pack_stats = dmfile->getPackStats();
        std::vector<PackEntry> pack_entries;
        pack_entries.reserve(pack_range.count());
        UInt32 offset = 0;
        for (UInt32 pack_id = pack_range.start_pack_id; pack_id < pack_range.end_pack_id; ++pack_id)
        {
            const auto & stat = pack_stats[pack_id];
            pack_entries.push_back(PackEntry{.offset = offset, .rows = stat.rows});
            offset += stat.rows;
        }
        return pack_entries;
    }

    static bool isCommonHandle() { return std::is_same_v<Handle, String>; }

    void loadHandleIfNotLoaded()
    {
        if (likely(!need_read_packs))
            return;
        
        auto read_pack_ids = std::make_shared<IdSet>();
        const auto & packs = *need_read_packs;
        for (UInt32 i = 0; i < packs.size(); ++i)
        {
            if (packs[i])
                read_pack_ids->insert(i + pack_range.start_pack_id);
        }

        auto scan_context = std::make_shared<ScanContext>(); // TODO: use dm_context.scan_context
        // TODO: load by segment range.
        auto pack_filter = DMFilePackFilter::loadFrom(
            dmfile,
            global_context.getMinMaxIndexCache(),
            true, //set_cache_if_miss
            {}, // rowkey_ranges, empty means all
            nullptr, // RSOperatorPtr
            read_pack_ids,
            global_context.getFileProvider(),
            global_context.getReadLimiter(),
            scan_context, // TODO:
            __FILE__,
            ReadTag::MVCC);

        DMFileReader reader(
            dmfile,
            {getHandleColumnDefine<Handle>()},
            isCommonHandle(),
            /*enable_handle_clean_read*/ false,
            /*enable_del_clean_read*/ false,
            /*is_fast_scan*/ false,
            /*max_data_version*/ std::numeric_limits<UInt64>::max(),
            std::move(pack_filter),
            global_context.getMarkCache(),
            /*enable_column_cache*/ false,
            /*column_cache*/ nullptr,
            global_context.getSettingsRef().max_read_buffer_size,
            global_context.getFileProvider(),
            global_context.getReadLimiter(),
            DEFAULT_MERGE_BLOCK_SIZE,
            /*read_one_pack_every_time*/ true,
            "DMFileHandleIndex",
            /*max_sharing_column_bytes_for_all*/ false,
            scan_context,
            ReadTag::MVCC);


        for (UInt32 pack_id : *read_pack_ids)
        {
            auto block = reader.read();
            handle_packs[pack_id - pack_range.start_pack_id] = block.begin()->column;
        }
        need_read_packs.reset();
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

        const auto handle_res = getDMFilePackFilterResultByRanges(dm_context, dmfile, {*rowkey_range});
        const auto valid_start_itr
            = std::find_if(handle_res.begin(), handle_res.end(), [](RSResult r) { return r.isUse(); });
        if (valid_start_itr == handle_res.end())
            return PackRange{.start_pack_id = 0, .end_pack_id = 0};

        const auto valid_end_itr
            = std::find_if(valid_start_itr, handle_res.end(), [](RSResult r) { return !r.isUse(); });
        return PackRange{
            .start_pack_id = static_cast<UInt32>(valid_start_itr - handle_res.begin()),
            .end_pack_id = static_cast<UInt32>(valid_end_itr - handle_res.begin())};
    }

    const Context & global_context;
    DMFilePtr dmfile;
    const RowID start_row_id;
    const std::optional<const RowKeyRange> rowkey_range;
    const PackRange pack_range;

    //IdSetPtr read_packs;

    std::vector<Handle> pack_index; // max value of each pack
    std::vector<PackEntry> pack_entries;
    std::vector<ColumnPtr> handle_packs;

    std::optional<std::vector<UInt8>> need_read_packs;
};

} // namespace DB::DM
