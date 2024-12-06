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

    DMFileHandleIndex(const Context & global_context_, const DMFilePtr & dmfile_)
        : global_context(global_context_)
        , dmfile(dmfile_)
        , pack_index(loadPackIndex(global_context, *dmfile))
        , pack_entries(loadPackEntries(*dmfile))
    {}

    // TODO: getBaseVersion one by one.
    // Maybe we can first check pack id of all handles, and process them by pack.

    template <Int64OrStringView HandleView>
    std::optional<RowID> getBaseVersion(HandleView h)
    {
        auto pack_entry = getPackEntry(h);
        if (!pack_entry)
            return {};
        return getBaseVersion(h, pack_entry->offset, pack_entry->rows);
    }

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

    template <Int64OrStringView HandleView>
    std::optional<PackEntry> getPackEntry(HandleView h)
    {
        auto itr = std::lower_bound(pack_index.begin(), pack_index.end(), h);
        if (itr == pack_index.end())
            return {};
        return pack_entries[itr - pack_index.begin()];
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

    void cleanHandle() { handle_column = nullptr; }

    void loadHandleIfNotLoaded()
    {
        if (likely(handle_column))
            return;

        // TODO: load by segment range.
        DMFileReader reader(
            dmfile,
            {getHandleColumnDefine<Handle>()},
            isCommonHandle(),
            /*enable_handle_clean_read*/ false,
            /*enable_del_clean_read*/ false,
            /*is_fast_scan*/ false,
            /*max_data_version*/ std::numeric_limits<UInt64>::max(),
            DMFilePackFilter::create(pack_entries.size(), RSResult::All),
            global_context.getMarkCache(),
            /*enable_column_cache*/ false,
            /*column_cache*/ nullptr,
            global_context.getSettingsRef().max_read_buffer_size,
            global_context.getFileProvider(),
            global_context.getReadLimiter(),
            pack_entries.back().offset + pack_entries.back().rows, // Read all
            /*read_one_pack_every_time*/ false,
            "DMFileHandleIndex",
            /*max_sharing_column_bytes_for_all*/ false,
            /*scan_context*/ nullptr,
            ReadTag::MVCC);
        auto block = reader.read();
        handle_column = block.begin()->column;
    }

private:
    const Context & global_context;
    DMFilePtr dmfile;

    std::vector<Handle> pack_index; // max value of each pack
    std::vector<PackEntry> pack_entries;
    ColumnPtr handle_column;
};

} // namespace DB::DM
