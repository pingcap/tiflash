// Copyright 2026 PingCAP, Inc.
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

#include <Common/PODArray.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/File/MergedFile.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>
#include <Storages/DeltaMerge/dtpb/dmfile.pb.h>
#include <common/types.h>

#include <optional>
#include <string_view>
#include <unordered_map>

namespace DB::DM
{

/// Pack-mark bit layout shared by ordinary and trim min-max payloads.
/// Ordinary `.idx` only uses bit 0; trim `.trim.idx` may also use bits 1/2.
namespace PackMarkBits
{
inline constexpr UInt8 Null = 0x01;
inline constexpr UInt8 TrimmedLow = 0x02;
inline constexpr UInt8 TrimmedHigh = 0x04;
inline constexpr UInt8 ReservedMask = 0xf8;
inline constexpr UInt8 OrdinaryAllowedMask = Null;
inline constexpr UInt8 TrimAllowedMask = Null | TrimmedLow | TrimmedHigh;
} // namespace PackMarkBits

inline bool hasNullMark(UInt8 pack_mark)
{
    return (pack_mark & PackMarkBits::Null) != 0;
}

inline bool hasTrimmedLowMark(UInt8 pack_mark)
{
    return (pack_mark & PackMarkBits::TrimmedLow) != 0;
}

inline bool hasTrimmedHighMark(UInt8 pack_mark)
{
    return (pack_mark & PackMarkBits::TrimmedHigh) != 0;
}

/// Compositional wrapper around ordinary MinMaxIndex payload for trim indexes.
/// Phase A only defines the data model and validation helpers; rough-check
/// correction for low/high flags lands in Phase C.
struct TrimMinMaxIndex
{
    MinMaxIndexPtr minmax;
};

using TrimMinMaxIndexPtr = std::shared_ptr<TrimMinMaxIndex>;

enum class TrimMinMaxFallbackReason : UInt8
{
    None = 0,
    Disabled,
    NoMeta,
    UnsupportedVersion,
    MetadataMismatch,
    IndexMissing,
    UnsupportedExpression,
    PredicateBoundaryOutsideRange,
};

inline std::string_view trimMinMaxFallbackReasonToString(TrimMinMaxFallbackReason reason)
{
    switch (reason)
    {
    case TrimMinMaxFallbackReason::None:
        return "none";
    case TrimMinMaxFallbackReason::Disabled:
        return "disabled";
    case TrimMinMaxFallbackReason::NoMeta:
        return "no_meta";
    case TrimMinMaxFallbackReason::UnsupportedVersion:
        return "unsupported_version";
    case TrimMinMaxFallbackReason::MetadataMismatch:
        return "metadata_mismatch";
    case TrimMinMaxFallbackReason::IndexMissing:
        return "index_missing";
    case TrimMinMaxFallbackReason::UnsupportedExpression:
        return "unsupported_expression";
    case TrimMinMaxFallbackReason::PredicateBoundaryOutsideRange:
        return "predicate_boundary_outside_range";
    }
    return "unknown";
}

struct TrimMinMaxIndexMeta
{
    UInt32 format_version = 0;
    UInt64 lower_bound = 0;
    UInt64 upper_bound = 0;
    UInt64 pack_count = 0;
    UInt64 file_size = 0;
    UInt64 merged_file_number = 0;
    UInt64 merged_file_offset = 0;
};

namespace TrimMinMax
{
inline constexpr UInt32 FormatVersionV1 = 1;

/// Default half-open effective range [1900-01-01 00:00:00, 2100-01-01 00:00:00).
UInt64 defaultLowerBoundPacked(const IDataType & nested_type);
UInt64 defaultUpperBoundPacked(const IDataType & nested_type);

String encodeBound(UInt64 packed);
std::optional<UInt64> decodeBound(std::string_view bytes);

dtpb::TrimMinMaxIndexProps makeDefaultProps(const IDataType & nested_type, UInt64 pack_count);

/// Structural validation of protobuf props against the column nested type and DMFile pack count.
/// Does not look up MergedSubFileInfo.
TrimMinMaxFallbackReason validateProps(
    const dtpb::TrimMinMaxIndexProps & props,
    const IDataType & nested_type,
    UInt64 expected_pack_count,
    TrimMinMaxIndexMeta * out_meta = nullptr);

/// Full metadata + subfile location check used by Reader selection.
/// On success fills `out_meta` (including MergedSubFileInfo size/location).
TrimMinMaxFallbackReason trySelectTrimMeta(
    bool read_enabled,
    const std::optional<dtpb::TrimMinMaxIndexProps> & props,
    const IDataType & nested_type,
    UInt64 expected_pack_count,
    const std::unordered_map<String, MergedSubFileInfo> & merged_sub_file_infos,
    const String & trim_index_fname,
    TrimMinMaxIndexMeta * out_meta = nullptr);

/// Returns true when a `.trim.idx` MergedSubFileInfo exists but ColumnStat has no trim meta.
bool hasOrphanTrimSubFile(
    const std::optional<dtpb::TrimMinMaxIndexProps> & props,
    const std::unordered_map<String, MergedSubFileInfo> & merged_sub_file_infos,
    const String & trim_index_fname);

/// Reject reserved bits in trim pack marks. Ordinary historical marks are only 0x00/0x01.
bool validateTrimPackMarks(const PaddedPODArray<UInt8> & pack_marks, size_t expected_pack_count);

} // namespace TrimMinMax

} // namespace DB::DM
