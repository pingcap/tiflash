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

#include <Core/Types.h>
#include <fmt/format.h>
#include <tipb/executor.pb.h>

#include <magic_enum.hpp>
#include <memory>

namespace TiDB
{

// Constructed from table definition.
struct VectorIndexDefinition
{
    tipb::VectorIndexKind kind = tipb::VectorIndexKind::INVALID_INDEX_KIND;
    UInt64 dimension = 0;
    tipb::VectorDistanceMetric distance_metric = tipb::VectorDistanceMetric::INVALID_DISTANCE_METRIC;

    // TODO: There are possibly more fields, like efConstruct.
    // Will be added later.
};

// As this is constructed from TiDB's table definition, we should not
// ever try to modify it anyway.
using VectorIndexDefinitionPtr = std::shared_ptr<const VectorIndexDefinition>;

} // namespace TiDB

template <>
struct fmt::formatter<TiDB::VectorIndexDefinition>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const TiDB::VectorIndexDefinition & vi, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return fmt::format_to(
            ctx.out(), //
            "{}:{}",
            tipb::VectorIndexKind_Name(vi.kind),
            tipb::VectorDistanceMetric_Name(vi.distance_metric));
    }
};

template <>
struct fmt::formatter<TiDB::VectorIndexDefinitionPtr>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const TiDB::VectorIndexDefinitionPtr & vi, FormatContext & ctx) const -> decltype(ctx.out())
    {
        if (!vi)
            return fmt::format_to(ctx.out(), "<no_idx>");
        return fmt::format_to(ctx.out(), "{}", *vi);
    }
};
