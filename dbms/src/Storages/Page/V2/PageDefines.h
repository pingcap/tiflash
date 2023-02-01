// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/Page/PageDefinesBase.h>

namespace DB::PS::V2
{
using PageId = PageIdU64;
using PageIds = PageIdU64s;
using PageIdSet = PageIdU64Set;
static constexpr PageId INVALID_PAGE_ID = INVALID_PAGE_U64_ID;

/// https://stackoverflow.com/a/13938417
inline size_t alignPage(size_t n)
{
    return (n + PAGE_SIZE_STEP - 1) & ~(PAGE_SIZE_STEP - 1);
}
} // namespace DB::PS::V2

template <>
struct fmt::formatter<DB::PageFileIdAndLevel>
{
    static constexpr auto parse(format_parse_context & ctx) -> decltype(ctx.begin())
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();
        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const DB::PageFileIdAndLevel & id_lvl, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return format_to(ctx.out(), "{}_{}", id_lvl.first, id_lvl.second);
    }
};
