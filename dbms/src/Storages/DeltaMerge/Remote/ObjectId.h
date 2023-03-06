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

#include <Storages/Transaction/Types.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB::DM::Remote
{

/**
 * Intra-cluster unique ID for a DMFile.
 */
struct DMFileOID
{
    StoreID store_id = 0;
    TableID table_id = 0;
    UInt64 file_id = 0;
};

/**
 * Intra-cluster unique ID for a page.
 */
struct PageOID
{
    StoreID store_id = 0;
    TableID table_id = 0;
    UInt64 page_id = 0;
};

} // namespace DB::DM::Remote

template <>
struct fmt::formatter<DB::DM::Remote::DMFileOID>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::DM::Remote::DMFileOID & value, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return format_to(ctx.out(), "{}_{}_{}", value.store_id, value.table_id, value.file_id);
    }
};

template <>
struct fmt::formatter<DB::DM::Remote::PageOID>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::DM::Remote::PageOID & value, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return format_to(ctx.out(), "{}_{}_{}", value.store_id, value.table_id, value.page_id);
    }
};
