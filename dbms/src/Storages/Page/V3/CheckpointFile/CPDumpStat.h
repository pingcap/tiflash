// Copyright 2023 PingCAP, Inc.
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

#include <Storages/Page/PageConstants.h>
#include <fmt/format.h>

#include <magic_enum.hpp>

namespace DB::PS::V3
{

struct CPDataDumpStats
{
    bool has_new_data = false;

    size_t incremental_data_bytes = 0;
    size_t compact_data_bytes = 0;

    // The number of keys uploaded in this checkpoint
    std::array<size_t, static_cast<size_t>(StorageType::_MAX_STORAGE_TYPE_)> num_keys{};
    // The number of bytes uploaded in this checkpoint
    std::array<size_t, static_cast<size_t>(StorageType::_MAX_STORAGE_TYPE_)> num_bytes{};

    // The number of bytes this checkpoint is holding. Some of the data are already uploaded
    // in the previous checkpoint data file.
    std::array<size_t, static_cast<size_t>(StorageType::_MAX_STORAGE_TYPE_)> num_existing_bytes{};

    // Total number of records in this checkpoint
    size_t num_records = 0;
    // Number of Pages that already uploaded to S3
    // and is not changed in this checkpoint
    size_t num_pages_unchanged = 0;
    // Number of Pages that already uploaded to S3
    // but picked by compaction in this checkpoint
    size_t num_pages_compact = 0;
    // Number of incremental Pages since last checkpoint
    size_t num_pages_incremental = 0;
    // Number of ExternalPages
    size_t num_ext_pages = 0;
    // Number of RefPages
    size_t num_ref_pages = 0;
    // Number of delete records
    size_t num_delete_records = 0;
    // Number of other records other than Pages/ExternalPages
    size_t num_other_records = 0;
};

void SetMetrics(const CPDataDumpStats & stats);

} // namespace DB::PS::V3

template <>
struct fmt::formatter<DB::PS::V3::CPDataDumpStats>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::PS::V3::CPDataDumpStats & value, FormatContext & ctx) const -> decltype(ctx.out())
    {
        auto it = fmt::format_to(
            ctx.out(),
            "CPDataDumpStats{{"
            "incremental_data_bytes={} compact_data_bytes={}"
            " n_records{{total={}"
            " pages_unchanged={} pages_compact={} pages_incremental={} ext_pages={} ref_pages={}"
            " delete={} other={}}}",
            value.incremental_data_bytes,
            value.compact_data_bytes,
            value.num_records,
            value.num_pages_unchanged,
            value.num_pages_compact,
            value.num_pages_incremental,
            value.num_ext_pages,
            value.num_ref_pages,
            value.num_delete_records,
            value.num_other_records);
        it = fmt::format_to(it, " types[");
        for (size_t i = 0; i < static_cast<size_t>(DB::StorageType::_MAX_STORAGE_TYPE_); ++i)
        {
            if (i != 0)
                it = fmt::format_to(it, " ");
            it = fmt::format_to(
                it,
                "{{type={} keys={} bytes={}}}",
                magic_enum::enum_name(static_cast<DB::StorageType>(i)),
                value.num_keys[i],
                value.num_bytes[i]);
        }
        return fmt::format_to(
            it,
            "]" // end of "keys"
            "}}" // end of "CPDataDumpStats"
        );
    }
};
