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

#include <Core/Defines.h>
#include <Core/Types.h>
#include <Storages/Page/PageConstants.h>
#include <fmt/format.h>

#include <chrono>
#include <unordered_set>
#include <vector>

namespace DB
{
using Clock = std::chrono::system_clock;
using Seconds = std::chrono::seconds;

using PageIdU64 = UInt64;
using PageIdU64s = std::vector<PageIdU64>;
using PageIdU64Set = std::unordered_set<PageIdU64>;
static constexpr PageIdU64 INVALID_PAGE_U64_ID = 0;

using PageIdV3Internal = UInt128;
using PageIdV3Internals = std::vector<PageIdV3Internal>;

inline PageIdV3Internal buildV3Id(NamespaceID n_id, PageIdU64 p_id)
{
    // low bits first
    return PageIdV3Internal(p_id, n_id);
}

using PageFieldOffset = UInt64;
using PageFieldOffsets = std::vector<PageFieldOffset>;
using PageFieldSizes = std::vector<UInt64>;

// {<offset, checksum>}
using PageFieldOffsetChecksums = std::vector<std::pair<PageFieldOffset, UInt64>>;

using PageFileId = UInt64;
using PageFileLevel = UInt32;
using PageFileIdAndLevel = std::pair<PageFileId, PageFileLevel>;
using PageFileIdAndLevels = std::vector<PageFileIdAndLevel>;

using PageSize = UInt64;

/// https://stackoverflow.com/a/13938417
inline size_t alignPage(size_t n)
{
    return (n + PAGE_SIZE_STEP - 1) & ~(PAGE_SIZE_STEP - 1);
}

} // namespace DB

// https://github.com/fmtlib/fmt/blob/master/doc/api.rst#formatting-user-defined-types
template <>
struct fmt::formatter<DB::PageIdV3Internal>
{
    static constexpr auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();
        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const DB::PageIdV3Internal & value, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}.{}", value.high, value.low);
    }
};

template <>
struct fmt::formatter<DB::PageFileIdAndLevel>
{
    static constexpr auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();
        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const DB::PageFileIdAndLevel & id_lvl, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}_{}", id_lvl.first, id_lvl.second);
    }
};
