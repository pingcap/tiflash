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

#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/isSupportedDataTypeCast.h>
#include <Functions/FunctionHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>
#include <common/types.h>
namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

namespace DM
{
namespace
{
/// some helper functions for casting column data type

template <typename TypeFrom, typename TypeTo>
void insertRangeFromWithNumericTypeCast(
    const ColumnPtr & from_col, //
    const ColumnPtr & null_map,
    const ColumnDefine & read_define,
    MutableColumnPtr & to_col,
    size_t rows_offset,
    size_t rows_limit)
{
    // Caller should ensure that both from_col / to_col
    // * are both integer or float32 -> float64
    // * no nullable wrapper
    // * both signed or unsigned
    static_assert(
        (std::is_integral_v<TypeFrom> && std::is_integral_v<TypeTo>)
        || (std::is_same<TypeFrom, Float32>::value && std::is_same<TypeTo, Float64>::value));
    constexpr bool is_both_signed_or_unsigned = !(std::is_unsigned_v<TypeFrom> ^ std::is_unsigned_v<TypeTo>);
    static_assert(is_both_signed_or_unsigned);
    assert(from_col != nullptr);
    assert(to_col != nullptr);
    assert(from_col->isNumeric());
    assert(to_col->isNumeric());
    assert(!from_col->isColumnNullable());
    assert(!to_col->isColumnNullable());
    assert(!from_col->isColumnConst());
    assert(!to_col->isColumnConst());

    // Something like `insertRangeFrom(from_col, rows_offset, rows_limit)` with static_cast
    const PaddedPODArray<TypeFrom> & from_array = toColumnVectorData<TypeFrom>(from_col);
    PaddedPODArray<TypeTo> * to_array_ptr = toMutableColumnVectorDataPtr<TypeTo>(to_col);
    to_array_ptr->reserve(rows_limit);
    for (size_t i = 0; i < rows_limit; ++i)
    {
        (*to_array_ptr).emplace_back(static_cast<TypeTo>(from_array[rows_offset + i]));
    }

    if (unlikely(null_map))
    {
        /// We are applying cast from nullable to not null, scan to fill "NULL" with default value

        TypeTo default_value = 0; // if read_define.default_value is empty, fill with 0
        if (read_define.default_value.isNull())
        {
            // Do nothing
        }
        else if (read_define.default_value.getType() == Field::Types::Int64)
        {
            default_value = read_define.default_value.safeGet<Int64>();
        }
        else if (read_define.default_value.getType() == Field::Types::UInt64)
        {
            default_value = read_define.default_value.safeGet<UInt64>();
        }
        else if (read_define.default_value.getType() == Field::Types::Float64)
        {
            default_value = read_define.default_value.safeGet<Float64>();
        }
        else
        {
            throw Exception("Invalid column value type", ErrorCodes::BAD_ARGUMENTS);
        }

        const size_t to_offset_before_inserted = to_array_ptr->size() - rows_limit;

        for (size_t i = 0; i < rows_limit; ++i)
        {
            const size_t to_offset = to_offset_before_inserted + i;
            if (null_map->getInt(rows_offset + i) != 0)
            {
                // `from_col[rows_offset + i]` is "NULL", fill `to_col[x]` with default value
                (*to_array_ptr)[to_offset] = static_cast<TypeTo>(default_value);
            }
        }
    }
}


bool castNonNullNumericColumn(
    const DataTypePtr & disk_type_not_null_,
    const ColumnPtr & disk_col_not_null,
    const ColumnDefine & read_define,
    const ColumnPtr & null_map,
    MutableColumnPtr & memory_col_not_null,
    size_t rows_offset,
    size_t rows_limit)
{
    /// Caller should ensure that type is not nullable
    assert(disk_type_not_null_ != nullptr);
    assert(disk_col_not_null != nullptr);
    assert(read_define.type != nullptr);
    assert(memory_col_not_null != nullptr);

    const IDataType * disk_type_not_null = disk_type_not_null_.get();
    const IDataType * read_type_not_null = read_define.type.get();

    /// Caller should ensure nullable is unwrapped
    assert(!disk_type_not_null->isNullable());
    assert(!read_type_not_null->isNullable());

    /// Caller should ensure that dist_type != read_type
    assert(!disk_type_not_null->equals(*read_type_not_null));

    if (checkDataType<DataTypeUInt32>(disk_type_not_null))
    {
        using FromType = UInt32;
        if (checkDataType<DataTypeUInt64>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, UInt64>(
                disk_col_not_null,
                null_map,
                read_define,
                memory_col_not_null,
                rows_offset,
                rows_limit);
            return true;
        }
    }
    else if (checkDataType<DataTypeInt32>(disk_type_not_null))
    {
        using FromType = Int32;
        if (checkDataType<DataTypeInt64>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, Int64>(
                disk_col_not_null,
                null_map,
                read_define,
                memory_col_not_null,
                rows_offset,
                rows_limit);
            return true;
        }
    }
    else if (checkDataType<DataTypeUInt16>(disk_type_not_null))
    {
        using FromType = UInt16;
        if (checkDataType<DataTypeUInt32>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, UInt32>(
                disk_col_not_null,
                null_map,
                read_define,
                memory_col_not_null,
                rows_offset,
                rows_limit);
            return true;
        }
        else if (checkDataType<DataTypeUInt64>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, UInt64>(
                disk_col_not_null,
                null_map,
                read_define,
                memory_col_not_null,
                rows_offset,
                rows_limit);
            return true;
        }
    }
    else if (checkDataType<DataTypeInt16>(disk_type_not_null))
    {
        using FromType = Int16;
        if (checkDataType<DataTypeInt32>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, Int32>(
                disk_col_not_null,
                null_map,
                read_define,
                memory_col_not_null,
                rows_offset,
                rows_limit);
            return true;
        }
        else if (checkDataType<DataTypeInt64>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, Int64>(
                disk_col_not_null,
                null_map,
                read_define,
                memory_col_not_null,
                rows_offset,
                rows_limit);
            return true;
        }
    }
    else if (checkDataType<DataTypeUInt8>(disk_type_not_null))
    {
        using FromType = UInt8;
        if (checkDataType<DataTypeUInt32>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, UInt32>(
                disk_col_not_null,
                null_map,
                read_define,
                memory_col_not_null,
                rows_offset,
                rows_limit);
            return true;
        }
        else if (checkDataType<DataTypeUInt64>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, UInt64>(
                disk_col_not_null,
                null_map,
                read_define,
                memory_col_not_null,
                rows_offset,
                rows_limit);
            return true;
        }
        else if (checkDataType<DataTypeUInt16>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, UInt16>(
                disk_col_not_null,
                null_map,
                read_define,
                memory_col_not_null,
                rows_offset,
                rows_limit);
            return true;
        }
    }
    else if (checkDataType<DataTypeInt8>(disk_type_not_null))
    {
        using FromType = Int8;
        if (checkDataType<DataTypeInt32>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, Int32>(
                disk_col_not_null,
                null_map,
                read_define,
                memory_col_not_null,
                rows_offset,
                rows_limit);
            return true;
        }
        else if (checkDataType<DataTypeInt64>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, Int64>(
                disk_col_not_null,
                null_map,
                read_define,
                memory_col_not_null,
                rows_offset,
                rows_limit);
            return true;
        }
        else if (checkDataType<DataTypeInt16>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, Int16>(
                disk_col_not_null,
                null_map,
                read_define,
                memory_col_not_null,
                rows_offset,
                rows_limit);
            return true;
        }
    }
    else if (checkDataType<DataTypeEnum8>(disk_type_not_null) && checkDataType<DataTypeEnum8>(read_type_not_null))
    {
        memory_col_not_null->insertRangeFrom(*disk_col_not_null, rows_offset, rows_limit);
        return true;
    }
    else if (checkDataType<DataTypeEnum16>(disk_type_not_null) && checkDataType<DataTypeEnum16>(read_type_not_null))
    {
        memory_col_not_null->insertRangeFrom(*disk_col_not_null, rows_offset, rows_limit);
        return true;
    }
    else if (checkDataType<DataTypeFloat32>(disk_type_not_null) && checkDataType<DataTypeFloat64>(read_type_not_null))
    {
        insertRangeFromWithNumericTypeCast<Float32, Float64>(
            disk_col_not_null,
            null_map,
            read_define,
            memory_col_not_null,
            rows_offset,
            rows_limit);
        return true;
    }
    else if (
        checkDataType<DataTypeMyDateTime>(disk_type_not_null) && checkDataType<DataTypeMyDateTime>(read_type_not_null))
    {
        static_assert(
            std::is_same_v<DataTypeMyDateTime::FieldType, UInt64>,
            "Ensure the MyDateTime/MyTime is stored as UInt64");
        insertRangeFromWithNumericTypeCast<UInt64, UInt64>(
            disk_col_not_null,
            null_map,
            read_define,
            memory_col_not_null,
            rows_offset,
            rows_limit);
        return true;
    }
    // else is not support
    return false;
}

} // namespace

void convertColumnByColumnDefine(
    const DataTypePtr & disk_type,
    const ColumnPtr & disk_col,
    const ColumnDefine & read_define,
    MutableColumnPtr memory_col,
    size_t rows_offset,
    size_t rows_limit)
{
    const DataTypePtr & read_type = read_define.type;

    // Unwrap nullable(what)
    ColumnPtr disk_col_not_null;
    MutableColumnPtr memory_col_not_null;
    ColumnPtr null_map;
    DataTypePtr disk_type_not_null = disk_type;
    DataTypePtr read_type_not_null = read_type;
    if (disk_type->isNullable() && read_type->isNullable())
    {
        // nullable -> nullable, copy null map
        const auto & disk_nullable_col = typeid_cast<const ColumnNullable &>(*disk_col);
        const auto & disk_null_map = disk_nullable_col.getNullMapData();
        auto & memory_nullable_col = typeid_cast<ColumnNullable &>(*memory_col);
        auto & memory_null_map = memory_nullable_col.getNullMapData();
        memory_null_map.insert(disk_null_map.begin(), disk_null_map.end());

        disk_col_not_null = disk_nullable_col.getNestedColumnPtr();
        memory_col_not_null = memory_nullable_col.getNestedColumn().getPtr();

        const auto * type_nullable = typeid_cast<const DataTypeNullable *>(disk_type.get());
        disk_type_not_null = type_nullable->getNestedType();
        type_nullable = typeid_cast<const DataTypeNullable *>(read_type.get());
        read_type_not_null = type_nullable->getNestedType();
    }
    else if (!disk_type->isNullable() && read_type->isNullable())
    {
        // not null -> nullable, set null map to all not null
        auto & memory_nullable_col = typeid_cast<ColumnNullable &>(*memory_col);
        auto & nullmap_data = memory_nullable_col.getNullMapData();
        nullmap_data.resize_fill_zero(rows_offset + rows_limit);

        disk_col_not_null = disk_col;
        memory_col_not_null = memory_nullable_col.getNestedColumn().getPtr();

        const auto * type_nullable = typeid_cast<const DataTypeNullable *>(read_type.get());
        read_type_not_null = type_nullable->getNestedType();
    }
    else if (disk_type->isNullable() && !read_type->isNullable())
    {
        // nullable -> not null, fill "NULL" values with default value later
        const auto & disk_nullable_col = typeid_cast<const ColumnNullable &>(*disk_col);
        null_map = disk_nullable_col.getNullMapColumnPtr();
        disk_col_not_null = disk_nullable_col.getNestedColumnPtr();
        memory_col_not_null = std::move(memory_col);

        const auto * type_nullable = typeid_cast<const DataTypeNullable *>(disk_type.get());
        disk_type_not_null = type_nullable->getNestedType();
    }
    else
    {
        // not null -> not null
        disk_col_not_null = disk_col;
        memory_col_not_null = std::move(memory_col);
    }

    assert(memory_col_not_null != nullptr);
    assert(disk_col_not_null != nullptr);
    assert(read_type_not_null != nullptr);
    assert(disk_type_not_null != nullptr);

    ColumnDefine read_define_not_null(read_define);
    read_define_not_null.type = read_type_not_null;
    if (disk_type_not_null->equals(*read_type_not_null))
    {
        if (null_map)
        {
            /// Applying cast from nullable -> not null, scan to fill "NULL" with default value
            for (size_t i = 0; i < rows_limit; ++i)
            {
                if (unlikely(null_map->getInt(i) != 0))
                    memory_col_not_null->insert(read_define.default_value);
                else
                    memory_col_not_null->insertFrom(*disk_col_not_null, i);
            }
        }
        else
        {
            /// Applying cast from not null -> nullable, simply copy from origin column
            memory_col_not_null->insertRangeFrom(*disk_col_not_null, rows_offset, rows_limit);
        }
    }
    else if (!castNonNullNumericColumn(
                 disk_type_not_null,
                 disk_col_not_null,
                 read_define_not_null,
                 null_map,
                 memory_col_not_null,
                 rows_offset,
                 rows_limit))
    {
        throw Exception(
            "Reading mismatch data type pack. Cast and assign from " + disk_type->getName() + " to "
                + read_type->getName() + " is NOT supported!",
            ErrorCodes::NOT_IMPLEMENTED);
    }
}

std::pair<bool, bool> checkColumnTypeCompatibility(const DataTypePtr & source_type, const DataTypePtr & target_type)
{
    if (unlikely(!isSupportedDataTypeCast(source_type, target_type)))
    {
        return std::make_pair(false, false);
    }

    bool need_cast_data = true;
    /// Currently, cast Enum to Enum is allowed only if from_type
    /// is a subset of the target_type, so when cast from source
    /// enum type to target enum type, the data do not need to be
    /// casted.
    bool source_is_null = source_type->isNullable();
    bool target_is_null = source_type->isNullable();
    if (source_is_null && target_is_null)
    {
        need_cast_data
            = !(typeid_cast<const DataTypeNullable *>(source_type.get())->isEnum()
                && typeid_cast<const DataTypeNullable *>(target_type.get())->isEnum());
    }
    else if (!source_is_null && !target_is_null)
    {
        need_cast_data = !(source_type->isEnum() && target_type->isEnum());
    }
    return std::make_pair(true, need_cast_data);
}

ColumnPtr convertColumnByColumnDefineIfNeed(
    const DataTypePtr & from_type,
    ColumnPtr && from_col,
    const ColumnDefine & to_column_define)
{
    // No need to convert
    if (likely(from_type->equals(*to_column_define.type)))
        return std::move(from_col);

    // Check if support
    auto [compatible, need_data_cast] = checkColumnTypeCompatibility(from_type, to_column_define.type);
    if (unlikely(!compatible))
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Reading mismatch data type pack. Cast from {} to {} is NOT supported, column_id={}",
            from_type->getName(),
            to_column_define.type->getName(),
            to_column_define.id);
    }
    if (unlikely(!need_data_cast))
    {
        return std::move(from_col);
    }

    // Cast column's data from DataType in disk to what we need now
    auto to_col = to_column_define.type->createColumn();
    to_col->reserve(from_col->size());
    convertColumnByColumnDefine(from_type, from_col, to_column_define, to_col->getPtr(), 0, from_col->size());
    return to_col;
}

ColumnPtr createColumnWithDefaultValue(const ColumnDefine & column_define, size_t num_rows)
{
    ColumnPtr column;
    // Read default value from `column_define.default_value`
    if (column_define.default_value.isNull())
    {
        column = column_define.type->createColumnConstWithDefaultValue(num_rows);
    }
    else
    {
        column = column_define.type->createColumnConst(num_rows, column_define.default_value);
    }
    column = column->convertToFullColumnIfConst();
    return column;
}

} // namespace DM
} // namespace DB
