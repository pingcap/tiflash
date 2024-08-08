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

#include <common/types.h>
#include <fmt/format.h>

#include <magic_enum.hpp>

namespace DB::DM
{
class RSResult;
}
namespace fmt
{
template <>
struct formatter<DB::DM::RSResult>;
}

namespace DB::DM
{

class RSResult
{
private:
    enum class ValueResult : UInt8
    {
        Some = 1, // Some values meet requirements and NOT has null, need to read and perform filtering
        None = 2, // No value meets requirements and NOT has null, no need to read
        All = 3, // All values meet requirements NOT has null, need to read and no need perform filtering
    };

    static ValueResult logicalNot(ValueResult v) noexcept;
    static ValueResult logicalAnd(ValueResult v0, ValueResult v1) noexcept;
    static ValueResult logicalOr(ValueResult v0, ValueResult v1) noexcept;

    // Deleting or privating constructors, so that cannot create invalid objects.
    // Use the static member variables below.
    RSResult() = delete;
    RSResult(ValueResult v_, bool has_null_)
        : v(v_)
        , has_null(has_null_)
    {}

    friend struct fmt::formatter<DB::DM::RSResult>;

    ValueResult v;
    bool has_null;

public:
    bool isUse() const noexcept { return v != ValueResult::None; }

    bool allMatch() const noexcept { return *this == RSResult::All; }

    void setHasNull() noexcept { has_null = true; }

    RSResult operator!() const noexcept { return RSResult(logicalNot(v), has_null); }

    RSResult operator&&(RSResult r) const noexcept { return RSResult(logicalAnd(v, r.v), has_null || r.has_null); }

    RSResult operator||(RSResult r) const noexcept
    {
        // Because the result of `1 || 1/0/NULL` is always 1.
        if (allMatch() || r.allMatch())
            return RSResult(ValueResult::All, false);
        return RSResult(logicalOr(v, r.v), has_null || r.has_null);
    }

    bool operator==(RSResult r) const noexcept { return v == r.v && has_null == r.has_null; }

    static const RSResult Some;
    static const RSResult None;
    static const RSResult All;
    static const RSResult SomeNull;
    static const RSResult NoneNull;
    static const RSResult AllNull;
};

using RSResults = std::vector<RSResult>;
} // namespace DB::DM

template <>
struct fmt::formatter<DB::DM::RSResult>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::DM::RSResult r, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}{}", magic_enum::enum_name(r.v), r.has_null ? "Null" : "");
    }
};
