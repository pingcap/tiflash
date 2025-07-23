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

namespace DB::DM
{
struct DMContext;
struct SegmentSnapshot;

// Filter out record versions that are deleted: read the delete mark column and update the `filter`.
// Return how many records are filtered out.
UInt32 buildDeleteMarkFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const DMFilePackFilterResultPtr & stable_filter_res,
    BitmapFilter & filter);
} // namespace DB::DM
