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

#include <Core/Types.h>

#include <chrono>
#include <unordered_set>

namespace DB
{
using TableID = Int64;
using TableIDSet = std::unordered_set<TableID>;

enum : TableID
{
    InvalidTableID = -1,
};

using DatabaseID = Int64;

using ColumnID = Int64;

enum : ColumnID
{
    // Prevent conflict with TiDB.
    TiDBPkColumnID = -1,
    ExtraTableIDColumnID = -3,
    VersionColumnID = -1024,
    DelMarkColumnID = -1025,
    InvalidColumnID = -10000,
};

using HandleID = Int64;
using Timestamp = UInt64;

using RegionID = UInt64;

enum : RegionID
{
    InvalidRegionID = 0
};

using RegionVersion = UInt64;

enum : RegionVersion
{
    InvalidRegionVersion = std::numeric_limits<RegionVersion>::max()
};

using Clock = std::chrono::system_clock;
using Timepoint = Clock::time_point;
using Duration = Clock::duration;
using Seconds = std::chrono::seconds;

} // namespace DB
