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

template <typename T>
concept ExtraHandleType = std::same_as<T, Int64> || std::same_as<T, String>;

template <ExtraHandleType HandleType>
struct ExtraHandleRefType
{
    using type = typename std::conditional<std::is_same_v<HandleType, Int64>, Int64, std::string_view>::type;
};

template <ExtraHandleType HandleType>
constexpr bool isCommonHandle()
{
    return std::is_same_v<HandleType, String>;
}

template <ExtraHandleType HandleType>
ColumnDefine getHandleColumnDefine()
{
    return getExtraHandleColumnDefine(isCommonHandle<HandleType>());
}

// For ColumnFileReader
template <ExtraHandleType HandleType>
ColumnDefinesPtr getHandleColumnDefinesPtr()
{
    static auto cds_ptr = std::make_shared<ColumnDefines>(1, getHandleColumnDefine<HandleType>());
    return cds_ptr;
}

template <typename HandleRefType>
bool inRowKeyRange(const RowKeyRange & range, HandleRefType handle)
{
    if constexpr (std::is_same_v<HandleRefType, Int64>)
        return range.start.int_value <= handle && (handle < range.end.int_value || range.isEndInfinite());
    else if constexpr (std::is_same_v<HandleRefType, std::string_view>)
        return *(range.start.value) <= handle && (handle < *(range.end.value) || range.isEndInfinite());
    else
        static_assert(false, "Only suport Int64 and std::string_view");
}

RSResults getRSResultsByRanges(const DMContext & dm_context, const DMFilePtr & dmfile, const RowKeyRanges & ranges);

// Clip RSResults by removing the leading and trailing RSResult::None.
// Return the clipped RSResults and the pack_id of the first not RSResult::None.
// Because DMFile of ColumnFileBig only takes packs intersect with the `segment_range`.
std::pair<RSResults, UInt32> getClippedRSResultsByRange(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const std::optional<RowKeyRange> & segment_range);

// Load max-value of packs of `col_id` from min-max index.
template <typename T>
std::vector<T> loadPackMaxValue(const DMContext & dm_context, const DMFile & dmfile, const ColId col_id);

// Load min-value and max-value of handle column.
template <ExtraHandleType HandleType>
std::optional<std::pair<HandleType, HandleType>> loadDMFileHandleRange(
    const DMContext & dm_context,
    const DMFile & dmfile);

} // namespace DB::DM
