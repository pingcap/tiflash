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

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/TiFlashException.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/ArrowColCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionHelpers.h>
#include <IO/Endian.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_EXCEPTION;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

const IColumn * getNestedCol(const IColumn * flash_col)
{
    if (flash_col->isColumnNullable())
        return static_cast<const ColumnNullable *>(flash_col)->getNestedColumnPtr().get();
    else
        return flash_col;
}

template <typename T>
void decimalToVector(T value, std::vector<Int32> & vec, UInt32 scale)
{
    if (value < 0)
    {
        value = -value;
    }
    while (value != 0)
    {
        vec.push_back(static_cast<Int32>(value % 10));
        value = value / 10;
    }
    while (vec.size() < scale)
    {
        vec.push_back(0);
    }
}

template <typename T, bool is_nullable>
bool flashDecimalColToArrowColInternal(
    TiDBColumn & dag_column,
    const IColumn * flash_col_untyped,
    size_t start_index,
    size_t end_index,
    const IDataType * data_type)
{
    const IColumn * nested_col = getNestedCol(flash_col_untyped);
    if (checkColumn<ColumnDecimal<T>>(nested_col) && checkDataType<DataTypeDecimal<T>>(data_type))
    {
        const auto * flash_col = checkAndGetColumn<ColumnDecimal<T>>(nested_col);
        const auto * type = checkAndGetDataType<DataTypeDecimal<T>>(data_type);
        UInt32 scale = type->getScale();
        for (size_t i = start_index; i < end_index; i++)
        {
            if constexpr (is_nullable)
            {
                if (flash_col_untyped->isNullAt(i))
                {
                    dag_column.appendNull();
                    continue;
                }
            }
            const T & dec = flash_col->getElement(i);
            std::vector<Int32> digits;
            digits.reserve(type->getPrec());
            decimalToVector<typename T::NativeType>(dec.value, digits, scale);
            TiDBDecimal ti_decimal(scale, digits, dec.value < 0);
            dag_column.append(ti_decimal);
        }
        return true;
    }
    return false;
}

template <bool is_nullable>
void flashDecimalColToArrowCol(
    TiDBColumn & dag_column,
    const IColumn * flash_col_untyped,
    size_t start_index,
    size_t end_index,
    const IDataType * data_type)
{
    if (!(flashDecimalColToArrowColInternal<Decimal32, is_nullable>(
              dag_column,
              flash_col_untyped,
              start_index,
              end_index,
              data_type)
          || flashDecimalColToArrowColInternal<Decimal64, is_nullable>(
              dag_column,
              flash_col_untyped,
              start_index,
              end_index,
              data_type)
          || flashDecimalColToArrowColInternal<Decimal128, is_nullable>(
              dag_column,
              flash_col_untyped,
              start_index,
              end_index,
              data_type)
          || flashDecimalColToArrowColInternal<Decimal256, is_nullable>(
              dag_column,
              flash_col_untyped,
              start_index,
              end_index,
              data_type)))
        throw TiFlashException(
            "Error while trying to convert flash col to DAG col, column name " + flash_col_untyped->getName(),
            Errors::Coprocessor::Internal);
}

template <typename T, bool is_nullable>
bool flashIntegerColToArrowColInternal(
    TiDBColumn & dag_column,
    const IColumn * flash_col_untyped,
    size_t start_index,
    size_t end_index)
{
    const IColumn * nested_col = getNestedCol(flash_col_untyped);
    if (const auto * flash_col = checkAndGetColumn<ColumnVector<T>>(nested_col))
    {
        constexpr bool is_unsigned = std::is_unsigned_v<T>;
        for (size_t i = start_index; i < end_index; i++)
        {
            if constexpr (is_nullable)
            {
                if (flash_col_untyped->isNullAt(i))
                {
                    dag_column.appendNull();
                    continue;
                }
            }
            if constexpr (is_unsigned)
                dag_column.append(static_cast<UInt64>(flash_col->getElement(i)));
            else
                dag_column.append(static_cast<UInt64>(flash_col->getElement(i)));
        }
        return true;
    }
    return false;
}

template <typename T, bool is_nullable>
void flashDoubleColToArrowCol(
    TiDBColumn & dag_column,
    const IColumn * flash_col_untyped,
    size_t start_index,
    size_t end_index)
{
    const IColumn * nested_col = getNestedCol(flash_col_untyped);
    if (const auto * flash_col = checkAndGetColumn<ColumnVector<T>>(nested_col))
    {
        for (size_t i = start_index; i < end_index; i++)
        {
            if constexpr (is_nullable)
            {
                if (flash_col_untyped->isNullAt(i))
                {
                    dag_column.appendNull();
                    continue;
                }
            }
            dag_column.append(static_cast<T>(flash_col->getElement(i)));
        }
        return;
    }
    throw TiFlashException(
        "Error while trying to convert flash col to DAG col, column name " + flash_col_untyped->getName(),
        Errors::Coprocessor::Internal);
}

template <bool is_nullable>
void flashIntegerColToArrowCol(
    TiDBColumn & dag_column,
    const IColumn * flash_col_untyped,
    size_t start_index,
    size_t end_index)
{
    if (!(flashIntegerColToArrowColInternal<UInt8, is_nullable>(dag_column, flash_col_untyped, start_index, end_index)
          || flashIntegerColToArrowColInternal<UInt16, is_nullable>(
              dag_column,
              flash_col_untyped,
              start_index,
              end_index)
          || flashIntegerColToArrowColInternal<UInt32, is_nullable>(
              dag_column,
              flash_col_untyped,
              start_index,
              end_index)
          || flashIntegerColToArrowColInternal<UInt64, is_nullable>(
              dag_column,
              flash_col_untyped,
              start_index,
              end_index)
          || flashIntegerColToArrowColInternal<Int8, is_nullable>(dag_column, flash_col_untyped, start_index, end_index)
          || flashIntegerColToArrowColInternal<Int16, is_nullable>(
              dag_column,
              flash_col_untyped,
              start_index,
              end_index)
          || flashIntegerColToArrowColInternal<Int32, is_nullable>(
              dag_column,
              flash_col_untyped,
              start_index,
              end_index)
          || flashIntegerColToArrowColInternal<Int64, is_nullable>(
              dag_column,
              flash_col_untyped,
              start_index,
              end_index)))
        throw TiFlashException(
            "Error while trying to convert flash col to DAG col, column name " + flash_col_untyped->getName(),
            Errors::Coprocessor::Internal);
}


template <bool is_nullable>
void flashDateOrDateTimeColToArrowCol(
    TiDBColumn & dag_column,
    const IColumn * flash_col_untyped,
    size_t start_index,
    size_t end_index,
    const tipb::FieldType & field_type)
{
    const IColumn * nested_col = getNestedCol(flash_col_untyped);
    using DateFieldType = DataTypeMyTimeBase::FieldType;
    const auto * flash_col = checkAndGetColumn<ColumnVector<DateFieldType>>(nested_col);
    for (size_t i = start_index; i < end_index; i++)
    {
        if constexpr (is_nullable)
        {
            if (flash_col_untyped->isNullAt(i))
            {
                dag_column.appendNull();
                continue;
            }
        }
        TiDBTime time = TiDBTime(flash_col->getElement(i), field_type);
        dag_column.append(time);
    }
}

template <bool is_nullable>
void flashStringColToArrowCol(
    TiDBColumn & dag_column,
    const IColumn * flash_col_untyped,
    size_t start_index,
    size_t end_index)
{
    const IColumn * nested_col = getNestedCol(flash_col_untyped);
    // columnFixedString is not used so do not check it
    const auto * flash_col = checkAndGetColumn<ColumnString>(nested_col);
    for (size_t i = start_index; i < end_index; i++)
    {
        // todo check if we can convert flash_col to DAG col directly since the internal representation is almost the same
        if constexpr (is_nullable)
        {
            if (flash_col_untyped->isNullAt(i))
            {
                dag_column.appendNull();
                continue;
            }
        }
        dag_column.append(flash_col->getDataAt(i));
    }
}

template <bool is_nullable>
void flashBitColToArrowCol(
    TiDBColumn & dag_column,
    const IColumn * flash_col_untyped,
    size_t start_index,
    size_t end_index,
    const tipb::FieldType & field_type)
{
    const IColumn * nested_col = getNestedCol(flash_col_untyped);
    const auto * flash_col = checkAndGetColumn<ColumnVector<UInt64>>(nested_col);
    for (size_t i = start_index; i < end_index; i++)
    {
        if constexpr (is_nullable)
        {
            if (flash_col_untyped->isNullAt(i))
            {
                dag_column.appendNull();
                continue;
            }
        }
        TiDBBit bit(flash_col->getElement(i), field_type.flen() < 0 ? -1 : (field_type.flen() + 7u) >> 3u);
        dag_column.append(bit);
    }
}

template <bool is_nullable>
void flashEnumColToArrowCol(
    TiDBColumn & dag_column,
    const IColumn * flash_col_untyped,
    size_t start_index,
    size_t end_index,
    const IDataType * data_type)
{
    const IColumn * nested_col = getNestedCol(flash_col_untyped);
    const auto * flash_col = checkAndGetColumn<ColumnVector<DataTypeEnum16::FieldType>>(nested_col);
    const auto * enum_type = checkAndGetDataType<DataTypeEnum16>(data_type);
    size_t enum_value_size = enum_type->getValues().size();
    for (size_t i = start_index; i < end_index; i++)
    {
        if constexpr (is_nullable)
        {
            if (flash_col_untyped->isNullAt(i))
            {
                dag_column.appendNull();
                continue;
            }
        }
        auto enum_value = static_cast<UInt64>(flash_col->getElement(i));
        if (enum_value > enum_value_size)
            throw TiFlashException(
                Errors::Coprocessor::Internal,
                "number of enum value {} overflow enum boundary {}",
                enum_value,
                enum_value_size);

        const auto & enum_name = enum_type->getNameForValue(static_cast<const DataTypeEnum16::FieldType>(enum_value));
        TiDBEnum ti_enum(enum_value, enum_name);
        dag_column.append(ti_enum);
    }
}

void flashColToArrowCol(
    TiDBColumn & dag_column,
    const ColumnWithTypeAndName & flash_col,
    const tipb::FieldType & field_type,
    size_t start_index,
    size_t end_index)
{
    auto column = flash_col.column->isColumnConst() ? flash_col.column->convertToFullColumnIfConst() : flash_col.column;
    const IColumn * col = column.get();
    const IDataType * type = flash_col.type.get();
    const TiDB::ColumnInfo tidb_column_info = TiDB::fieldTypeToColumnInfo(field_type);

    if (type->isNullable() && tidb_column_info.hasNotNullFlag())
    {
        throw TiFlashException(
            "Flash column and TiDB column has different not null flag",
            Errors::Coprocessor::Internal);
    }
    if (type->isNullable())
        type = static_cast<const DataTypeNullable *>(type)->getNestedType().get();

    switch (tidb_column_info.tp)
    {
    case TiDB::TypeTiny:
    case TiDB::TypeShort:
    case TiDB::TypeInt24:
    case TiDB::TypeLong:
    case TiDB::TypeLongLong:
    case TiDB::TypeYear:
    case TiDB::TypeTime:
        if (!type->isInteger() && !type->isMyTime())
            throw TiFlashException(
                "Type un-matched during arrow encode, target col type is integer and source column type is "
                    + type->getName(),
                Errors::Coprocessor::Internal);
        if (type->isUnsignedInteger() != tidb_column_info.hasUnsignedFlag())
            throw TiFlashException(
                "Flash column and TiDB column has different unsigned flag",
                Errors::Coprocessor::Internal);
        if (tidb_column_info.hasNotNullFlag())
            flashIntegerColToArrowCol<false>(dag_column, col, start_index, end_index);
        else
            flashIntegerColToArrowCol<true>(dag_column, col, start_index, end_index);
        break;
    case TiDB::TypeFloat:
        if (!checkDataType<DataTypeFloat32>(type))
            throw TiFlashException(
                "Type un-matched during arrow encode, target col type is float32 and source column type is "
                    + type->getName(),
                Errors::Coprocessor::Internal);
        if (tidb_column_info.hasNotNullFlag())
            flashDoubleColToArrowCol<Float32, false>(dag_column, col, start_index, end_index);
        else
            flashDoubleColToArrowCol<Float32, true>(dag_column, col, start_index, end_index);
        break;
    case TiDB::TypeDouble:
        if (!checkDataType<DataTypeFloat64>(type))
            throw TiFlashException(
                "Type un-matched during arrow encode, target col type is float64 and source column type is "
                    + type->getName(),
                Errors::Coprocessor::Internal);
        if (tidb_column_info.hasNotNullFlag())
            flashDoubleColToArrowCol<Float64, false>(dag_column, col, start_index, end_index);
        else
            flashDoubleColToArrowCol<Float64, true>(dag_column, col, start_index, end_index);
        break;
    case TiDB::TypeDate:
    case TiDB::TypeDatetime:
    case TiDB::TypeTimestamp:
        if (!type->isDateOrDateTime())
            throw TiFlashException(
                "Type un-matched during arrow encode, target col type is datetime and source column type is "
                    + type->getName(),
                Errors::Coprocessor::Internal);
        if (tidb_column_info.hasNotNullFlag())
            flashDateOrDateTimeColToArrowCol<false>(dag_column, col, start_index, end_index, field_type);
        else
            flashDateOrDateTimeColToArrowCol<true>(dag_column, col, start_index, end_index, field_type);
        break;
    case TiDB::TypeNewDecimal:
        if (!type->isDecimal())
            throw TiFlashException(
                "Type un-matched during arrow encode, target col type is datetime and source column type is "
                    + type->getName(),
                Errors::Coprocessor::Internal);
        if (tidb_column_info.hasNotNullFlag())
            flashDecimalColToArrowCol<false>(dag_column, col, start_index, end_index, type);
        else
            flashDecimalColToArrowCol<true>(dag_column, col, start_index, end_index, type);
        break;
    case TiDB::TypeVarchar:
    case TiDB::TypeVarString:
    case TiDB::TypeString:
    case TiDB::TypeBlob:
    case TiDB::TypeLongBlob:
    case TiDB::TypeMediumBlob:
    case TiDB::TypeTinyBlob:
    case TiDB::TypeJSON:
        if (!checkDataType<DataTypeString>(type))
            throw TiFlashException(
                "Type un-matched during arrow encode, target col type is string and source column type is "
                    + type->getName(),
                Errors::Coprocessor::Internal);
        if (tidb_column_info.hasNotNullFlag())
            flashStringColToArrowCol<false>(dag_column, col, start_index, end_index);
        else
            flashStringColToArrowCol<true>(dag_column, col, start_index, end_index);
        break;
    case TiDB::TypeBit:
        if (!checkDataType<DataTypeUInt64>(type))
            throw TiFlashException(
                "Type un-matched during arrow encode, target col type is bit and source column type is "
                    + type->getName(),
                Errors::Coprocessor::Internal);
        if (tidb_column_info.hasNotNullFlag())
            flashBitColToArrowCol<false>(dag_column, col, start_index, end_index, field_type);
        else
            flashBitColToArrowCol<true>(dag_column, col, start_index, end_index, field_type);
        break;
    case TiDB::TypeEnum:
        if (!checkDataType<DataTypeEnum16>(type))
            throw TiFlashException(
                "Type un-matched during arrow encode, target col type is bit and source column type is "
                    + type->getName(),
                Errors::Coprocessor::Internal);
        if (tidb_column_info.hasNotNullFlag())
            flashEnumColToArrowCol<false>(dag_column, col, start_index, end_index, type);
        else
            flashEnumColToArrowCol<true>(dag_column, col, start_index, end_index, type);
        break;
    default:
        throw TiFlashException(
            "Unsupported field type " + field_type.DebugString() + " when try to convert flash col to DAG col",
            Errors::Coprocessor::Internal);
    }
}

bool checkNull(UInt32 i, UInt32 null_count, const std::vector<UInt8> & null_bitmap, const ColumnWithTypeAndName & col)
{
    if (null_count > 0)
    {
        size_t index = i >> 3;
        size_t p = i & 7;
        if (!(null_bitmap[index] & (1 << p)))
        {
            col.column->assumeMutable()->insert(Field());
            return true;
        }
    }
    return false;
}

const char * arrowStringColToFlashCol(
    const char * pos,
    UInt8,
    UInt32 null_count,
    const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> & offsets,
    const ColumnWithTypeAndName & col,
    const ColumnInfo &,
    UInt32 length)
{
    for (UInt32 i = 0; i < length; i++)
    {
        if (checkNull(i, null_count, null_bitmap, col))
            continue;
        const String value = String(pos + offsets[i], pos + offsets[i + 1]);
        col.column->assumeMutable()->insert(Field(value));
    }
    return pos + offsets[length];
}

const char * arrowEnumColToFlashCol(
    const char * pos,
    UInt8,
    UInt32 null_count,
    const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> & offsets,
    const ColumnWithTypeAndName & col,
    const ColumnInfo &,
    UInt32 length)
{
    for (UInt32 i = 0; i < length; i++)
    {
        if (checkNull(i, null_count, null_bitmap, col))
            continue;
        const auto enum_value
            = static_cast<Int64>(toLittleEndian(*(reinterpret_cast<const UInt32 *>(pos + offsets[i]))));
        col.column->assumeMutable()->insert(Field(enum_value));
    }
    return pos + offsets[length];
}

const char * arrowBitColToFlashCol(
    const char * pos,
    UInt8,
    UInt32 null_count,
    const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> & offsets,
    const ColumnWithTypeAndName & col,
    const ColumnInfo &,
    UInt32 length)
{
    for (UInt32 i = 0; i < length; i++)
    {
        if (checkNull(i, null_count, null_bitmap, col))
            continue;
        const String value = String(pos + offsets[i], pos + offsets[i + 1]);
        if (value.length() == 0)
            col.column->assumeMutable()->insert(Field(static_cast<UInt64>(0)));
        UInt64 result = 0;
        for (const auto & c : value)
        {
            result = (result << 8u) | static_cast<UInt8>(c);
        }
        col.column->assumeMutable()->insert(Field(result));
    }
    return pos + offsets[length];
}

template <typename T>
T toCHDecimal(UInt8 digits_int, UInt8 digits_frac, bool negative, const Int32 * word_buf)
{
    static_assert(IsDecimal<T>);

    UInt8 word_int = (digits_int + DIGITS_PER_WORD - 1) / DIGITS_PER_WORD;
    UInt8 word_frac = digits_frac / DIGITS_PER_WORD;
    UInt8 tailing_digit = digits_frac % DIGITS_PER_WORD;

    typename T::NativeType value = 0;
    const int word_max = static_cast<int>(1e9);
    for (int i = 0; i < word_int; i++)
    {
        value = value * word_max + word_buf[i];
    }
    for (int i = 0; i < word_frac; i++)
    {
        value = value * word_max + word_buf[i + word_int];
    }
    if (tailing_digit > 0)
    {
        Int32 tail = word_buf[word_int + word_frac];
        for (int i = 0; i < DIGITS_PER_WORD - tailing_digit; i++)
        {
            tail /= 10;
        }
        for (int i = 0; i < tailing_digit; i++)
        {
            value *= 10;
        }
        value += tail;
    }
    return negative ? -value : value;
}

const char * arrowDecimalColToFlashCol(
    const char * pos,
    UInt8 field_length,
    UInt32 null_count,
    const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> &,
    const ColumnWithTypeAndName & col,
    const ColumnInfo &,
    UInt32 length)
{
    for (UInt32 i = 0; i < length; i++)
    {
        if (checkNull(i, null_count, null_bitmap, col))
        {
            pos += field_length;
            continue;
        }
        UInt8 digits_int = toLittleEndian(*(reinterpret_cast<const UInt8 *>(pos)));
        pos += 1;
        UInt8 digits_frac = toLittleEndian(*(reinterpret_cast<const UInt8 *>(pos)));
        pos += 1;
        //UInt8 result_frac = toLittleEndian(*(reinterpret_cast<const UInt8 *>(pos)));
        pos += 1;
        UInt8 negative = toLittleEndian(*(reinterpret_cast<const UInt8 *>(pos)));
        pos += 1;
        Int32 word_buf[MAX_WORD_BUF_LEN];
        const DataTypePtr decimal_type = col.type->isNullable()
            ? static_cast<const DataTypeNullable *>(col.type.get())->getNestedType()
            : col.type;
        for (int & j : word_buf)
        {
            j = toLittleEndian(*(reinterpret_cast<const Int32 *>(pos)));
            pos += 4;
        }
        if (const auto * type32 = checkDecimal<Decimal32>(*decimal_type))
        {
            auto res = toCHDecimal<Decimal32>(digits_int, digits_frac, negative, word_buf);
            col.column->assumeMutable()->insert(DecimalField<Decimal32>(res, type32->getScale()));
        }
        else if (const auto * type64 = checkDecimal<Decimal64>(*decimal_type))
        {
            auto res = toCHDecimal<Decimal64>(digits_int, digits_frac, negative, word_buf);
            col.column->assumeMutable()->insert(DecimalField<Decimal64>(res, type64->getScale()));
        }
        else if (const auto * type128 = checkDecimal<Decimal128>(*decimal_type))
        {
            auto res = toCHDecimal<Decimal128>(digits_int, digits_frac, negative, word_buf);
            col.column->assumeMutable()->insert(DecimalField<Decimal128>(res, type128->getScale()));
        }
        else if (const auto * type256 = checkDecimal<Decimal256>(*decimal_type))
        {
            auto res = toCHDecimal<Decimal256>(digits_int, digits_frac, negative, word_buf);
            col.column->assumeMutable()->insert(DecimalField<Decimal256>(res, type256->getScale()));
        }
    }
    return pos;
}

const char * arrowDateColToFlashCol(
    const char * pos,
    UInt8 field_length,
    UInt32 null_count,
    const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> &,
    const ColumnWithTypeAndName & col,
    const ColumnInfo &,
    UInt32 length)
{
    for (UInt32 i = 0; i < length; i++)
    {
        if (checkNull(i, null_count, null_bitmap, col))
        {
            pos += field_length;
            continue;
        }
        UInt64 chunk_time = toLittleEndian(*(reinterpret_cast<const UInt64 *>(pos)));
        auto year
            = static_cast<UInt16>((chunk_time & MyTimeBase::YEAR_BIT_FIELD_MASK) >> MyTimeBase::YEAR_BIT_FIELD_OFFSET);
        auto month
            = static_cast<UInt8>((chunk_time & MyTimeBase::MONTH_BIT_FIELD_MASK) >> MyTimeBase::MONTH_BIT_FIELD_OFFSET);
        auto day
            = static_cast<UInt8>((chunk_time & MyTimeBase::DAY_BIT_FIELD_MASK) >> MyTimeBase::DAY_BIT_FIELD_OFFSET);
        auto hour
            = static_cast<UInt16>((chunk_time & MyTimeBase::HOUR_BIT_FIELD_MASK) >> MyTimeBase::HOUR_BIT_FIELD_OFFSET);
        auto minute = static_cast<UInt8>(
            (chunk_time & MyTimeBase::MINUTE_BIT_FIELD_MASK) >> MyTimeBase::MINUTE_BIT_FIELD_OFFSET);
        auto second = static_cast<UInt8>(
            (chunk_time & MyTimeBase::SECOND_BIT_FIELD_MASK) >> MyTimeBase::SECOND_BIT_FIELD_OFFSET);
        auto micro_second = static_cast<UInt32>(
            (chunk_time & MyTimeBase::MICROSECOND_BIT_FIELD_MASK) >> MyTimeBase::MICROSECOND_BIT_FIELD_OFFSET);
        MyDateTime mt(year, month, day, hour, minute, second, micro_second);
        pos += field_length;
        col.column->assumeMutable()->insert(Field(mt.toPackedUInt()));
    }
    return pos;
}

const char * arrowNumColToFlashCol(
    const char * pos,
    UInt8 field_length,
    UInt32 null_count,
    const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> &,
    const ColumnWithTypeAndName & col,
    const ColumnInfo & col_info,
    UInt32 length)
{
    for (UInt32 i = 0; i < length; i++, pos += field_length)
    {
        if (checkNull(i, null_count, null_bitmap, col))
            continue;
        UInt64 u64;
        Int64 i64;
        UInt32 u32;
        Float32 f32;
        Float64 f64;
        switch (col_info.tp)
        {
        case TiDB::TypeTiny:
        case TiDB::TypeShort:
        case TiDB::TypeInt24:
        case TiDB::TypeLong:
        case TiDB::TypeLongLong:
        case TiDB::TypeYear:
            if (col_info.flag & TiDB::ColumnFlagUnsigned)
            {
                u64 = toLittleEndian(*(reinterpret_cast<const UInt64 *>(pos)));
                col.column->assumeMutable()->insert(Field(u64));
            }
            else
            {
                i64 = toLittleEndian(*(reinterpret_cast<const Int64 *>(pos)));
                col.column->assumeMutable()->insert(Field(i64));
            }
            break;
        case TiDB::TypeTime:
            i64 = toLittleEndian(*(reinterpret_cast<const Int64 *>(pos)));
            col.column->assumeMutable()->insert(Field(i64));
            break;
        case TiDB::TypeFloat:
            u32 = toLittleEndian(*(reinterpret_cast<const UInt32 *>(pos)));
            std::memcpy(&f32, &u32, sizeof(Float32));
            col.column->assumeMutable()->insert(Field(static_cast<Float64>(f32)));
            break;
        case TiDB::TypeDouble:
            u64 = toLittleEndian(*(reinterpret_cast<const UInt64 *>(pos)));
            std::memcpy(&f64, &u64, sizeof(Float64));
            col.column->assumeMutable()->insert(Field(f64));
            break;
        default:
            throw TiFlashException("Should not reach here", Errors::Coprocessor::Internal);
        }
    }
    return pos;
}

const char * arrowColToFlashCol(
    const char * pos,
    UInt8 field_length,
    UInt32 null_count,
    const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> & offsets,
    const ColumnWithTypeAndName & flash_col,
    const ColumnInfo & col_info,
    UInt32 length)
{
    switch (col_info.tp)
    {
    case TiDB::TypeTiny:
    case TiDB::TypeShort:
    case TiDB::TypeInt24:
    case TiDB::TypeLong:
    case TiDB::TypeLongLong:
    case TiDB::TypeYear:
    case TiDB::TypeFloat:
    case TiDB::TypeDouble:
    case TiDB::TypeTime:
        return arrowNumColToFlashCol(pos, field_length, null_count, null_bitmap, offsets, flash_col, col_info, length);
    case TiDB::TypeDatetime:
    case TiDB::TypeDate:
    case TiDB::TypeTimestamp:
        return arrowDateColToFlashCol(pos, field_length, null_count, null_bitmap, offsets, flash_col, col_info, length);
    case TiDB::TypeNewDecimal:
        return arrowDecimalColToFlashCol(
            pos,
            field_length,
            null_count,
            null_bitmap,
            offsets,
            flash_col,
            col_info,
            length);
    case TiDB::TypeVarString:
    case TiDB::TypeVarchar:
    case TiDB::TypeBlob:
    case TiDB::TypeString:
    case TiDB::TypeTinyBlob:
    case TiDB::TypeMediumBlob:
    case TiDB::TypeLongBlob:
    case TiDB::TypeJSON:
        return arrowStringColToFlashCol(
            pos,
            field_length,
            null_count,
            null_bitmap,
            offsets,
            flash_col,
            col_info,
            length);
    case TiDB::TypeBit:
        return arrowBitColToFlashCol(pos, field_length, null_count, null_bitmap, offsets, flash_col, col_info, length);
    case TiDB::TypeEnum:
        return arrowEnumColToFlashCol(pos, field_length, null_count, null_bitmap, offsets, flash_col, col_info, length);
    default:
        throw TiFlashException(
            "Not supported yet: field tp = " + std::to_string(col_info.tp),
            Errors::Coprocessor::Unimplemented);
    }
}

} // namespace DB
