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

#include <magic_enum.hpp>
#include <memory>

namespace TiDB
{

enum class VectorIndexKind
{
    INVALID = 0,

    // Note: Field names must match TiDB's enum definition.
    HNSW,
};

enum class DistanceMetric
{
    INVALID = 0,

    // Note: Field names must match TiDB's enum definition.
    L1,
    L2,
    COSINE,
    INNER_PRODUCT,
};


struct VectorIndexInfo
{
    VectorIndexKind kind = VectorIndexKind::INVALID;
    UInt64 dimension = 0;
    DistanceMetric distance_metric = DistanceMetric::INVALID;
};

using VectorIndexInfoPtr = std::shared_ptr<VectorIndexInfo>;

} // namespace TiDB

template <>
struct fmt::formatter<TiDB::VectorIndexKind>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const TiDB::VectorIndexKind & v, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return format_to(ctx.out(), "{}", magic_enum::enum_name(v));
    }
};

template <>
struct fmt::formatter<TiDB::DistanceMetric>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const TiDB::DistanceMetric & d, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return format_to(ctx.out(), "{}", magic_enum::enum_name(d));
    }
};

template <>
struct fmt::formatter<TiDB::VectorIndexInfo>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const TiDB::VectorIndexInfo & vi, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return format_to(ctx.out(), "{}:{}", vi.kind, vi.distance_metric);
    }
};

template <>
struct fmt::formatter<TiDB::VectorIndexInfoPtr>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const TiDB::VectorIndexInfoPtr & vi, FormatContext & ctx) const -> decltype(ctx.out())
    {
        if (!vi)
            return format_to(ctx.out(), "<no_idx>");
        return format_to(ctx.out(), "{}", *vi);
    }
};
