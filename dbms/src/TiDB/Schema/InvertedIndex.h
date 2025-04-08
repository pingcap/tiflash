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

#include <common/types.h>
#include <fmt/format.h>

namespace TiDB
{

// Constructed from table definition.
struct InvertedIndexDefinition
{
    bool is_signed;
    UInt8 type_size;
};

// As this is constructed from TiDB's table definition, we should not
// ever try to modify it anyway.
using InvertedIndexDefinitionPtr = std::shared_ptr<const InvertedIndexDefinition>;
} // namespace TiDB

template <>
struct fmt::formatter<TiDB::InvertedIndexDefinition>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const TiDB::InvertedIndexDefinition & index, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return fmt::format_to(ctx.out(), "{}{}", index.is_signed ? "" : "U", index.type_size * 8);
    }
};

template <>
struct fmt::formatter<TiDB::InvertedIndexDefinitionPtr>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const TiDB::InvertedIndexDefinitionPtr & index, FormatContext & ctx) const -> decltype(ctx.out())
    {
        if (!index)
            return fmt::format_to(ctx.out(), "nullptr");
        return fmt::format_to(ctx.out(), "{}", *index);
    }
};
