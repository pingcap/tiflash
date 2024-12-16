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

#include <Storages/DeltaMerge/ColumnDefine_fwd.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <common/types.h>

#include <memory>

namespace DB::DM
{
struct DMContext;

using RowID = UInt32;
static constexpr RowID NotExistRowID = std::numeric_limits<RowID>::max();
static constexpr RowID UnknownRowID = NotExistRowID - 1;

template <typename T>
concept Int64OrString = std::same_as<T, Int64> || std::same_as<T, String>;

template <typename T>
concept Int64OrStringView = std::same_as<T, Int64> || std::same_as<T, std::string_view>;

template <Int64OrString Handle>
ColumnDefine getHandleColumnDefine()
{
    if constexpr (std::is_same_v<Handle, Int64>)
        return getExtraIntHandleColumnDefine();
    else if constexpr (std::is_same_v<Handle, String>)
        return getExtraStringHandleColumnDefine();
    else
        static_assert(false, "Not support type");
}

// For ColumnFileReader
template <Int64OrString Handle>
ColumnDefinesPtr getHandleColumnDefinesPtr()
{
    static auto cds_ptr = std::make_shared<ColumnDefines>(1, getHandleColumnDefine<Handle>());
    return cds_ptr;
}

inline ColumnDefinesPtr getVersionColumnDefinesPtr()
{
    static auto cds_ptr = std::make_shared<ColumnDefines>(1, getVersionColumnDefine());
    return cds_ptr;
}

inline ColumnDefinesPtr getTagColumnDefinesPtr()
{
    static auto cds_ptr = std::make_shared<ColumnDefines>(1, getTagColumnDefine());
    return cds_ptr;
}

template <Int64OrStringView HandleView>
bool inRowKeyRange(const RowKeyRange & range, HandleView handle)
{
    if constexpr (std::is_same_v<Handle, Int64>)
        return range.start.int_value <= handle && handle < range.end.int_value;
    else
        static_assert(false, "TODO: support common handle");
}

template <Int64OrString Handle>
std::pair<Handle, Handle> convertRowKeyRange(const RowKeyRange & range)
{
    if constexpr (std::is_same_v<Handle, Int64>)
        return {range.start.int_value, range.end.int_value};
    else
        static_assert(false, "TODO: support common handle");
}

RSResults getDMFilePackFilterResultByRanges(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const RowKeyRanges & ranges);

std::pair<RSResults, UInt32> getDMFilePackFilterResultBySegmentRange(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const std::optional<RowKeyRange> & segment_range);

template <typename T>
std::vector<T> loadPackMaxValue(const Context & global_context, const DMFile & dmfile, const ColId col_id);

std::pair<UInt32, UInt32> getDMFilePackRangeBySegmentRange(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const std::optional<RowKeyRange> & segment_range);
} // namespace DB::DM
