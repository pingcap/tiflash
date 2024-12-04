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
    struct PackEntry
    {
        UInt64 offset;
        UInt64 rows;
    };

    DMFileHandleIndex(const Context & global_context_, const DMFilePtr & dmfile_, const RowID start_row_id_)
        : global_context(global_context_)
        , dmfile(dmfile_)
        , start_row_id(start_row_id_)
        , read_packs(std::make_shared<IdSet>())
        , pack_index(loadPackIndex(global_context, *dmfile))
        , pack_entries(loadPackEntries(*dmfile))
        , handle_packs(dmfile->getPacks())
    {}

    // TODO: getBaseVersion one by one.
    // Maybe we can first check pack id of all handles, and process them by pack.

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
    /*
    template <Int64OrStringView HandleView>
    std::optional<RowID> getBaseVersion(HandleView h, UInt64 offset, UInt64 rows)
    {
        loadHandleIfNotLoaded();

        const auto * v = toColumnVectorDataPtr<Int64>(handle_column);
        RUNTIME_CHECK_MSG(v != nullptr, "TODO: support common handle");

        auto begin = v->begin() + offset;
        auto end = begin + rows;
        auto itr = std::lower_bound(begin, end, h);
        if (itr != end && *itr == h)
            return itr - begin + offset;
        return {};
    }
*/
    template <Int64OrStringView HandleView>
    std::optional<std::pair<UInt32, PackEntry>> getPackEntry(HandleView h)
    {
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

    static std::vector<Handle> loadPackIndex(const Context & global_context, const DMFile & dmfile)
    {
        return loadPackMaxValue<Handle>(global_context, dmfile, EXTRA_HANDLE_COLUMN_ID);
    }

    static std::vector<PackEntry> loadPackEntries(const DMFile & dmfile)
    {
        if (dmfile.getPacks() == 0)
            return {};

        const auto & pack_stats = dmfile.getPackStats();
        std::vector<PackEntry> pack_entries;
        pack_entries.reserve(pack_stats.size());
        UInt64 offset = 0;
        for (const auto & stat : pack_stats)
        {
            pack_entries.push_back(PackEntry{.offset = offset, .rows = stat.rows});
            offset += stat.rows;
        }
        return pack_entries;
    }

    static bool isCommonHandle() { return std::is_same_v<Handle, String>; }

    void loadHandleIfNotLoaded()
    {
        if (likely(!read_packs))
            return;

        auto scan_context = std::make_shared<ScanContext>(); // TODO: use dm_context.scan_context
        // TODO: load by segment range.
        auto pack_filter = DMFilePackFilter::loadFrom(
            dmfile,
            global_context.getMinMaxIndexCache(),
            true, //set_cache_if_miss
            {}, // rowkey_ranges, empty means all
            nullptr, // RSOperatorPtr
            read_packs->empty() ? nullptr : read_packs,
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

        if (read_packs->empty())
        {
            for (UInt32 pack_id = 0; pack_id < dmfile->getPacks(); ++pack_id)
            {
                auto block = reader.read();
                handle_packs[pack_id] = block.begin()->column;
            }
        }
        else
        {
            for (UInt32 pack_id : *read_packs)
            {
                auto block = reader.read();
                handle_packs[pack_id] = block.begin()->column;
            }
        }
        read_packs = nullptr;
    }

    void calculateReadPacks(const PaddedPODArray<Handle> & handles)
    {
        std::vector<UInt8> inserted_packs(dmfile->getPacks(), 0);
        for (Handle h : handles)
        {
            auto pa = getPackEntry(h);
            if (!pa || inserted_packs[pa->first])
                continue;

            inserted_packs[pa->first] = 1;
            read_packs->insert(pa->first);

            if (read_packs->size() * 4 >= inserted_packs.size())
            {
                read_packs->clear();
                break;
            }
        }
    }

    void cleanHandleColumn()
    {
        std::fill(handle_packs.begin(), handle_packs.end(), nullptr);
        read_packs = std::make_shared<IdSet>();
    }

private:
    const Context & global_context;
    DMFilePtr dmfile;
    const RowID start_row_id;

    IdSetPtr read_packs;

    std::vector<Handle> pack_index; // max value of each pack
    std::vector<PackEntry> pack_entries;
    std::vector<ColumnPtr> handle_packs;
};

} // namespace DB::DM
