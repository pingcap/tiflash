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

#include <Columns/IColumn.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>

namespace DB::DM
{
struct DMContext;
struct SegmentSnapshot;

template <ExtraHandleType HandleType>
UInt32 buildVersionFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const std::vector<RowID> & base_ver_snap,
    const UInt64 read_ts,
    IColumn::Filter & filter);
} // namespace DB::DM
