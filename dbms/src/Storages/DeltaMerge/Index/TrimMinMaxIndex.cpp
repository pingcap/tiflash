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

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/MyTime.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/Index/TrimMinMaxIndex.h>

namespace DB::DM
{
namespace
{
const IDataType * stripNullable(const IDataType & type)
{
    if (type.isNullable())
        return static_cast<const DataTypeNullable &>(type).getNestedType().get();
    return &type;
}

bool isSupportedTemporalNestedType(const IDataType & nested_type)
{
    return typeid_cast<const DataTypeMyDate *>(&nested_type) || typeid_cast<const DataTypeMyDateTime *>(&nested_type);
}

const PaddedPODArray<UInt64> & getUInt64Data(const IColumn & column)
{
    if (column.isColumnNullable())
    {
        const auto & nullable = static_cast<const ColumnNullable &>(column);
        return static_cast<const ColumnVector<UInt64> &>(nullable.getNestedColumn()).getData();
    }
    return static_cast<const ColumnVector<UInt64> &>(column).getData();
}
} // namespace

namespace TrimMinMax
{
bool isSupportedTemporalType(const IDataType & type)
{
    return isSupportedTemporalNestedType(*stripNullable(type));
}

UInt64 defaultLowerBoundPacked(const IDataType & nested_type)
{
    const auto & type = *stripNullable(nested_type);
    if (typeid_cast<const DataTypeMyDate *>(&type))
        return MyDate(1900, 1, 1).toPackedUInt();
    // DATETIME / TIMESTAMP share MyDateTime packing.
    return MyDateTime(1900, 1, 1, 0, 0, 0, 0).toPackedUInt();
}

UInt64 defaultUpperBoundPacked(const IDataType & nested_type)
{
    const auto & type = *stripNullable(nested_type);
    // Use 2099-12-01 rather than 2100-01-01 so TIMESTAMP / TZ / DST conversions
    // near the common 2100-01-01 sentinel do not push the exclusive upper edge
    // across timezone boundaries into an unexpected calendar day.
    if (typeid_cast<const DataTypeMyDate *>(&type))
        return MyDate(2099, 12, 1).toPackedUInt();
    return MyDateTime(2099, 12, 1, 0, 0, 0, 0).toPackedUInt();
}

String encodeBound(UInt64 packed)
{
    WriteBufferFromOwnString buf;
    writeIntBinary(packed, buf);
    return buf.str();
}

std::optional<UInt64> decodeBound(std::string_view bytes)
{
    if (bytes.size() != sizeof(UInt64))
        return std::nullopt;
    ReadBufferFromMemory buf(bytes.data(), bytes.size());
    UInt64 value = 0;
    readIntBinary(value, buf);
    if (!buf.eof())
        return std::nullopt;
    return value;
}

dtpb::TrimMinMaxIndexProps makeDefaultProps(const IDataType & nested_type, UInt64 pack_count)
{
    dtpb::TrimMinMaxIndexProps props;
    props.set_format_version(FormatVersionV1);
    props.set_lower_bound(encodeBound(defaultLowerBoundPacked(nested_type)));
    props.set_upper_bound(encodeBound(defaultUpperBoundPacked(nested_type)));
    props.set_pack_count(pack_count);
    return props;
}

void addOrdinaryAndTrimPack(
    MinMaxIndex & ordinary,
    MinMaxIndex & trim,
    const IColumn & column,
    const ColumnVector<UInt8> * del_mark,
    UInt64 lower_bound,
    UInt64 upper_bound)
{
    const auto * del_mark_data = del_mark ? &del_mark->getData() : nullptr;
    const UInt8 * null_mark_data = nullptr;
    if (column.isColumnNullable())
        null_mark_data = static_cast<const ColumnNullable &>(column).getNullMapData().data();

    const auto & values = getUInt64Data(column);
    const size_t size = column.size();

    size_t ordinary_min_idx = size;
    size_t ordinary_max_idx = size;
    size_t trim_min_idx = size;
    size_t trim_max_idx = size;
    UInt8 trim_pack_mark = 0;
    bool ordinary_has_null = false;

    for (size_t i = 0; i < size; ++i)
    {
        if (del_mark_data && (*del_mark_data)[i])
            continue;

        if (null_mark_data && null_mark_data[i])
        {
            ordinary_has_null = true;
            trim_pack_mark |= PackMarkBits::Null;
            continue;
        }

        const UInt64 value = values[i];
        if (ordinary_min_idx == size || value < values[ordinary_min_idx])
            ordinary_min_idx = i;
        if (ordinary_max_idx == size || value > values[ordinary_max_idx])
            ordinary_max_idx = i;

        if (value >= lower_bound && value < upper_bound)
        {
            if (trim_min_idx == size || value < values[trim_min_idx])
                trim_min_idx = i;
            if (trim_max_idx == size || value > values[trim_max_idx])
                trim_max_idx = i;
        }
        else if (value < lower_bound)
        {
            trim_pack_mark |= PackMarkBits::TrimmedLow;
        }
        else
        {
            trim_pack_mark |= PackMarkBits::TrimmedHigh;
        }
    }

    const UInt8 ordinary_pack_mark = ordinary_has_null ? PackMarkBits::Null : 0;
    if (ordinary_min_idx != size)
    {
        ordinary.appendPack(
            ordinary_pack_mark,
            /*has_value*/ true,
            PackMarkBits::OrdinaryAllowedMask,
            &column,
            ordinary_min_idx,
            ordinary_max_idx);
    }
    else
    {
        ordinary.appendPack(ordinary_pack_mark, /*has_value*/ false, PackMarkBits::OrdinaryAllowedMask);
    }

    if (trim_min_idx != size)
    {
        trim.appendPack(
            trim_pack_mark,
            /*has_value*/ true,
            PackMarkBits::TrimAllowedMask,
            &column,
            trim_min_idx,
            trim_max_idx);
    }
    else
    {
        trim.appendPack(trim_pack_mark, /*has_value*/ false, PackMarkBits::TrimAllowedMask);
    }
}

TrimMinMaxFallbackReason validateProps(
    const dtpb::TrimMinMaxIndexProps & props,
    const IDataType & nested_type,
    UInt64 expected_pack_count,
    TrimMinMaxIndexMeta * out_meta)
{
    const auto & type = *stripNullable(nested_type);
    if (!isSupportedTemporalNestedType(type))
        return TrimMinMaxFallbackReason::MetadataMismatch;

    if (!props.has_format_version() || props.format_version() != FormatVersionV1)
        return TrimMinMaxFallbackReason::UnsupportedVersion;

    if (!props.has_lower_bound() || !props.has_upper_bound() || !props.has_pack_count())
        return TrimMinMaxFallbackReason::MetadataMismatch;

    auto lower = decodeBound(props.lower_bound());
    auto upper = decodeBound(props.upper_bound());
    if (!lower || !upper || *lower >= *upper)
        return TrimMinMaxFallbackReason::MetadataMismatch;

    if (props.pack_count() != expected_pack_count)
        return TrimMinMaxFallbackReason::MetadataMismatch;

    if (out_meta)
    {
        out_meta->format_version = props.format_version();
        out_meta->lower_bound = *lower;
        out_meta->upper_bound = *upper;
        out_meta->pack_count = props.pack_count();
    }
    return TrimMinMaxFallbackReason::None;
}

TrimMinMaxFallbackReason trySelectTrimMeta(
    bool read_enabled,
    const std::optional<dtpb::TrimMinMaxIndexProps> & props,
    const IDataType & nested_type,
    UInt64 expected_pack_count,
    const std::unordered_map<String, MergedSubFileInfo> & merged_sub_file_infos,
    const String & trim_index_fname,
    TrimMinMaxIndexMeta * out_meta)
{
    if (!read_enabled)
        return TrimMinMaxFallbackReason::Disabled;

    if (!props.has_value())
        return TrimMinMaxFallbackReason::NoMeta;

    TrimMinMaxIndexMeta meta;
    auto reason = validateProps(*props, nested_type, expected_pack_count, &meta);
    if (reason != TrimMinMaxFallbackReason::None)
        return reason;

    auto itr = merged_sub_file_infos.find(trim_index_fname);
    if (itr == merged_sub_file_infos.end())
        return TrimMinMaxFallbackReason::IndexMissing;

    const auto & info = itr->second;
    if (info.size == 0)
        return TrimMinMaxFallbackReason::IndexMissing;

    meta.file_size = info.size;
    meta.merged_file_number = info.number;
    meta.merged_file_offset = info.offset;
    if (out_meta)
        *out_meta = meta;
    return TrimMinMaxFallbackReason::None;
}

bool hasOrphanTrimSubFile(
    const std::optional<dtpb::TrimMinMaxIndexProps> & props,
    const std::unordered_map<String, MergedSubFileInfo> & merged_sub_file_infos,
    const String & trim_index_fname)
{
    if (props.has_value())
        return false;
    auto itr = merged_sub_file_infos.find(trim_index_fname);
    return itr != merged_sub_file_infos.end() && itr->second.size > 0;
}

bool validateTrimPackMarks(const PaddedPODArray<UInt8> & pack_marks, size_t expected_pack_count)
{
    if (pack_marks.size() != expected_pack_count)
        return false;
    for (unsigned char pack_mark : pack_marks)
    {
        if ((pack_mark & PackMarkBits::ReservedMask) != 0)
            return false;
    }
    return true;
}

} // namespace TrimMinMax
} // namespace DB::DM
