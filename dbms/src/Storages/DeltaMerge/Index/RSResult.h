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

#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB::DM
{
struct Attr
{
    String col_name;
    ColId col_id;
    DataTypePtr type;
};
using Attrs = std::vector<Attr>;

enum class RSResult : UInt8
{
    Unknown = 0, // Not checked yet
    Some = 1, // Some values meet requirements, need to read and perform filtering
    None = 2, // No value meets requirements, no need to read
    All = 3, // All values meet requirements, need to read and no need perform filtering
    SomeNull = 4, // Some values meet requirements and has null, need to read and perform filtering
    NoneNull = 5, // No value meets requirements and has null, no need to read
    AllNull = 6, // All values meet requirements and has null, need to read and perform filtering
};
using RSResults = std::vector<RSResult>;

ALWAYS_INLINE inline std::pair<RSResult, bool> removeNull(RSResult v) noexcept
{
    switch (v)
    {
    case RSResult::SomeNull:
        return {RSResult::Some, true};
    case RSResult::NoneNull:
        return {RSResult::None, true};
    case RSResult::AllNull:
        return {RSResult::All, true};
    default:
        return {v, false};
    }
}

ALWAYS_INLINE inline RSResult addNull(RSResult v) noexcept
{
    switch (v)
    {
    case RSResult::Some:
        return RSResult::SomeNull;
    case RSResult::None:
        return RSResult::NoneNull;
    case RSResult::All:
        return RSResult::AllNull;
    default:
        return v;
    }
}

// For safety reasons, the logical operations of RSResult will always keep null if null has occurred before
ALWAYS_INLINE inline RSResult operator!(RSResult v)
{
    switch (v)
    {
    case RSResult::Some:
        return RSResult::Some;
    case RSResult::None:
        return RSResult::All;
    case RSResult::All:
        return RSResult::None;
    case RSResult::SomeNull:
        return RSResult::SomeNull;
    case RSResult::NoneNull:
        return RSResult::AllNull;
    case RSResult::AllNull:
        return RSResult::NoneNull;
    default:
        throw Exception("Unknow RSResult: {}", static_cast<UInt8>(v));
    }
}

ALWAYS_INLINE inline RSResult operator||(RSResult v0, RSResult v1)
{
    RUNTIME_CHECK(v0 != RSResult::Unknown && v1 != RSResult::Unknown);
    auto [t0, has_null0] = removeNull(v0);
    auto [t1, has_null1] = removeNull(v1);
    auto result = RSResult::None;
    if (t0 == RSResult::All || t1 == RSResult::All)
        result = RSResult::All;
    else if (t0 == RSResult::Some || t1 == RSResult::Some)
        result = RSResult::Some;

    return (has_null0 || has_null1) ? addNull(result) : result;
}

ALWAYS_INLINE inline RSResult operator&&(RSResult v0, RSResult v1)
{
    RUNTIME_CHECK(v0 != RSResult::Unknown && v1 != RSResult::Unknown);
    auto [t0, has_null0] = removeNull(v0);
    auto [t1, has_null1] = removeNull(v1);
    auto result = RSResult::Some;
    if (t0 == RSResult::None || t1 == RSResult::None)
        result = RSResult::None;
    if (t0 == RSResult::All && t1 == RSResult::All)
        result = RSResult::All;

    return (has_null0 || has_null1) ? addNull(result) : result;
}

ALWAYS_INLINE inline bool isUse(RSResult res) noexcept
{
    return res != RSResult::None && res != RSResult::NoneNull;
}
} // namespace DB::DM
