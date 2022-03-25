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

#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{
namespace DM
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
    Some = 1, // Suspected (but may be empty or full)
    None = 2, // Empty
    All = 3, // Full
};

static constexpr RSResult Unknown = RSResult::Unknown;
static constexpr RSResult Some = RSResult::Some;
static constexpr RSResult None = RSResult::None;
static constexpr RSResult All = RSResult::All;

inline RSResult operator!(RSResult v)
{
    if (unlikely(v == Unknown))
        throw Exception("Unexpected Unknown");
    if (v == All)
        return None;
    else if (v == None)
        return All;
    return v;
}

inline RSResult operator||(RSResult v0, RSResult v1)
{
    if (unlikely(v0 == Unknown || v1 == Unknown))
        throw Exception("Unexpected Unknown");
    if (v0 == All || v1 == All)
        return All;
    if (v0 == Some || v1 == Some)
        return Some;
    return None;
}

inline RSResult operator&&(RSResult v0, RSResult v1)
{
    if (unlikely(v0 == Unknown || v1 == Unknown))
        throw Exception("Unexpected Unknown");
    if (v0 == None || v1 == None)
        return None;
    if (v0 == All && v1 == All)
        return All;
    return Some;
}

} // namespace DM

} // namespace DB