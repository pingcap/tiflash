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

#include <Core/Types.h>
#include <fmt/format.h>

#include <memory>

namespace TiDB
{

// Constructed from table definition.
// See TiDB's pkg/parser/model/index_full_text.go
struct FullTextIndexDefinition
{
    String parser_type = "INVALID";
};

// As this is constructed from TiDB's table definition, we should not
// ever try to modify it anyway.
using FullTextIndexDefinitionPtr = std::shared_ptr<const FullTextIndexDefinition>;

} // namespace TiDB

template <>
struct fmt::formatter<TiDB::FullTextIndexDefinition>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const TiDB::FullTextIndexDefinition & vi, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return fmt::format_to(
            ctx.out(), //
            "PARSER_{}",
            vi.parser_type);
    }
};

template <>
struct fmt::formatter<TiDB::FullTextIndexDefinitionPtr>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const TiDB::FullTextIndexDefinitionPtr & vi, FormatContext & ctx) const -> decltype(ctx.out())
    {
        if (!vi)
            return fmt::format_to(ctx.out(), "");
        return fmt::format_to(ctx.out(), "{}", *vi);
    }
};
