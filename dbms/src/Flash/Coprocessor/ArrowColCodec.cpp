#include <Flash/Coprocessor/ArrowColCodec.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>
#include <IO/Endian.h>

namespace DB
{

template <typename T>
void decimalToVector(T dec, std::vector<Int32> & vec, UInt32 scale)
{
    Int256 value = dec.value;
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

template <typename T>
bool flashDecimalColToArrowCol(TiDBColumn & dag_column, const IColumn * flash_col_untyped, const IColumn * null_col,
    const tipb::FieldType & field_type, const IDataType * data_type, size_t start_index, size_t end_index)
{
    if (checkColumn<ColumnDecimal<T>>(flash_col_untyped) && checkDataType<DataTypeDecimal<T>>(data_type)
        && field_type.tp() == TiDB::TypeNewDecimal)
    {
        const ColumnDecimal<T> * flash_col = checkAndGetColumn<ColumnDecimal<T>>(flash_col_untyped);
        const DataTypeDecimal<T> * type = checkAndGetDataType<DataTypeDecimal<T>>(data_type);
        for (size_t i = start_index; i < end_index; i++)
        {
            if (null_col != nullptr && null_col->isNullAt(i))
            {
                dag_column.appendNull();
                continue;
            }
            const T & dec = flash_col->getElement(i);
            UInt32 scale = type->getScale();
            std::vector<Int32> digits;
            decimalToVector<T>(dec, digits, scale);
            TiDBDecimal tiDecimal(scale, digits, dec.value < 0);
            dag_column.appendDecimal(tiDecimal);
        }
        return true;
    }
    return false;
}

template <typename T>
bool flashNumColToArrowCol(TiDBColumn & dag_column, const IColumn * flash_col_untyped, const IColumn * null_col,
    const tipb::FieldType & field_type, size_t start_index, size_t end_index)
{
    if (const ColumnVector<T> * flash_col = checkAndGetColumn<ColumnVector<T>>(flash_col_untyped))
    {
        for (size_t i = start_index; i < end_index; i++)
        {
            if (null_col != nullptr && null_col->isNullAt(i))
            {
                dag_column.appendNull();
                continue;
            }
            switch (field_type.tp())
            {
                case TiDB::TypeTiny:
                case TiDB::TypeShort:
                case TiDB::TypeInt24:
                case TiDB::TypeLong:
                case TiDB::TypeLongLong:
                case TiDB::TypeYear:
                    if (field_type.flag() & TiDB::ColumnFlagUnsigned)
                    {
                        dag_column.appendUInt64(UInt64(flash_col->getElement(i)));
                    }
                    else
                    {
                        dag_column.appendInt64(Int64(flash_col->getElement(i)));
                    }
                    break;
                case TiDB::TypeFloat:
                    dag_column.appendFloat32(Float32(flash_col->getElement(i)));
                    break;
                case TiDB::TypeDouble:
                    dag_column.appendFloat64(Float64(flash_col->getElement(i)));
                    break;
                default:
                    throw Exception("Not supported yet: trying to convert flash col " + flash_col->getName()
                        + " to DAG column of type: " + field_type.DebugString());
            }
        }
        return true;
    }
    // todo maybe the column is a const col??
    return false;
}

bool flashDateOrDateTimeColToArrowCol(TiDBColumn & dag_column, const IColumn * flash_col_untyped, const IColumn * null_col,
    const tipb::FieldType & field_type, const IDataType * data_type, size_t start_index, size_t end_index)
{
    if ((field_type.tp() == TiDB::TypeDate || field_type.tp() == TiDB::TypeDatetime || field_type.tp() == TiDB::TypeTimestamp)
        && (checkDataType<DataTypeMyDate>(data_type) || checkDataType<DataTypeMyDateTime>(data_type)))
    {
        using DateFieldType = DataTypeMyTimeBase::FieldType;
        auto * flash_col = checkAndGetColumn<ColumnVector<DateFieldType>>(flash_col_untyped);
        for (size_t i = start_index; i < end_index; i++)
        {
            if (null_col != nullptr && null_col->isNullAt(i))
            {
                dag_column.appendNull();
                continue;
            }
            TiDBTime time = TiDBTime(flash_col->getElement(i), field_type);
            dag_column.appendTime(time);
        }
        return true;
    }
    return false;
}

bool flashStringColToArrowCol(TiDBColumn & dag_column, const IColumn * flash_col_untyped, const IColumn * null_col,
    const tipb::FieldType & field_type, size_t start_index, size_t end_index)
{
    // columnFixedString is not used so do not check it
    auto * flash_col = checkAndGetColumn<ColumnString>(flash_col_untyped);
    if (flash_col)
    {
        for (size_t i = start_index; i < end_index; i++)
        {
            // todo check if we can convert flash_col to DAG col directly since the internal representation is almost the same
            if (null_col != nullptr && null_col->isNullAt(i))
            {
                dag_column.appendNull();
                continue;
            }
            switch (field_type.tp())
            {
                case TiDB::TypeVarchar:
                case TiDB::TypeVarString:
                case TiDB::TypeString:
                case TiDB::TypeBlob:
                case TiDB::TypeLongBlob:
                case TiDB::TypeMediumBlob:
                case TiDB::TypeTinyBlob:
                    dag_column.appendBytes(flash_col->getDataAt(i));
                    break;
                default:
                    throw Exception("Not supported yet: convert flash col " + flash_col->getName()
                        + " to DAG column of type: " + field_type.DebugString());
            }
        }
        return true;
    }

    return false;
}

void flashColToArrowCol(TiDBColumn & dag_column, const ColumnWithTypeAndName & flash_col, const tipb::FieldType & field_type,
    size_t start_index, size_t end_index)
{
    const IColumn * col = flash_col.column.get();
    const IDataType * type = flash_col.type.get();
    const IColumn * null_col = nullptr;

    if (type->isNullable())
    {
        null_col = col;
        type = dynamic_cast<const DataTypeNullable *>(type)->getNestedType().get();
        col = dynamic_cast<const ColumnNullable *>(col)->getNestedColumnPtr().get();
    }
    const bool is_num = col->isNumeric();
    if (is_num)
    {
        if (!(flashDateOrDateTimeColToArrowCol(dag_column, col, null_col, field_type, type, start_index, end_index)
                || flashNumColToArrowCol<UInt8>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToArrowCol<UInt16>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToArrowCol<UInt32>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToArrowCol<UInt64>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToArrowCol<UInt128>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToArrowCol<Int8>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToArrowCol<Int16>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToArrowCol<Int32>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToArrowCol<Int64>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToArrowCol<Float32>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToArrowCol<Float64>(dag_column, col, null_col, field_type, start_index, end_index)))
        {
            throw Exception("Illegal column " + col->getName() + " when try to convert flash col to DAG col");
        }
    }
    else if (!(flashDecimalColToArrowCol<Decimal32>(dag_column, col, null_col, field_type, type, start_index, end_index)
                 || flashDecimalColToArrowCol<Decimal64>(dag_column, col, null_col, field_type, type, start_index, end_index)
                 || flashDecimalColToArrowCol<Decimal128>(dag_column, col, null_col, field_type, type, start_index, end_index)
                 || flashDecimalColToArrowCol<Decimal256>(dag_column, col, null_col, field_type, type, start_index, end_index)
                 || flashStringColToArrowCol(dag_column, col, null_col, field_type, start_index, end_index)))
    {
        throw Exception("Illegal column " + col->getName() + " when try to convert flash col to DAG col");
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

const char * arrowStringColToFlashCol(const char * pos, UInt8, UInt32 null_count, const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> & offsets, const ColumnWithTypeAndName & col, const ColumnInfo &, UInt32 length)
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

template <typename T>
T toCHDecimal(UInt8 digits_int, UInt8 digits_frac, bool negative, const Int32 * word_buf)
{
    static_assert(IsDecimal<T>);

    UInt8 word_int = (digits_int + DIGITS_PER_WORD - 1) / DIGITS_PER_WORD;
    UInt8 word_frac = digits_frac / DIGITS_PER_WORD;
    UInt8 tailing_digit = digits_frac % DIGITS_PER_WORD;

    typename T::NativeType value = 0;
    const int word_max = int(1e9);
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

const char * arrowDecimalColToFlashCol(const char * pos, UInt8 field_length, UInt32 null_count, const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> &, const ColumnWithTypeAndName & col, const ColumnInfo &, UInt32 length)
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
        const DataTypePtr decimal_type
            = col.type->isNullable() ? dynamic_cast<const DataTypeNullable *>(col.type.get())->getNestedType() : col.type;
        for (int j = 0; j < MAX_WORD_BUF_LEN; j++)
        {
            word_buf[j] = toLittleEndian(*(reinterpret_cast<const Int32 *>(pos)));
            pos += 4;
        }
        if (auto * type = checkDecimal<Decimal32>(*decimal_type))
        {
            auto res = toCHDecimal<Decimal32>(digits_int, digits_frac, negative, word_buf);
            col.column->assumeMutable()->insert(DecimalField<Decimal32>(res, type->getScale()));
        }
        else if (auto * type = checkDecimal<Decimal64>(*decimal_type))
        {
            auto res = toCHDecimal<Decimal64>(digits_int, digits_frac, negative, word_buf);
            col.column->assumeMutable()->insert(DecimalField<Decimal64>(res, type->getScale()));
        }
        else if (auto * type = checkDecimal<Decimal128>(*decimal_type))
        {
            auto res = toCHDecimal<Decimal128>(digits_int, digits_frac, negative, word_buf);
            col.column->assumeMutable()->insert(DecimalField<Decimal128>(res, type->getScale()));
        }
        else if (auto * type = checkDecimal<Decimal256>(*decimal_type))
        {
            auto res = toCHDecimal<Decimal256>(digits_int, digits_frac, negative, word_buf);
            col.column->assumeMutable()->insert(DecimalField<Decimal256>(res, type->getScale()));
        }
    }
    return pos;
}

const char * arrowDateColToFlashCol(const char * pos, UInt8 field_length, UInt32 null_count, const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> &, const ColumnWithTypeAndName & col, const ColumnInfo &, UInt32 length)
{
    for (UInt32 i = 0; i < length; i++)
    {
        if (checkNull(i, null_count, null_bitmap, col))
        {
            pos += field_length;
            continue;
        }
        UInt32 hour = toLittleEndian(*(reinterpret_cast<const UInt32 *>(pos)));
        pos += 4;
        UInt32 micro_second = toLittleEndian(*(reinterpret_cast<const UInt32 *>(pos)));
        pos += 4;
        UInt16 year = toLittleEndian(*(reinterpret_cast<const UInt16 *>(pos)));
        pos += 2;
        UInt8 month = toLittleEndian(*(reinterpret_cast<const UInt8 *>(pos)));
        pos += 1;
        UInt8 day = toLittleEndian(*(reinterpret_cast<const UInt8 *>(pos)));
        pos += 1;
        UInt8 minute = toLittleEndian(*(reinterpret_cast<const UInt8 *>(pos)));
        pos += 1;
        UInt8 second = toLittleEndian(*(reinterpret_cast<const UInt8 *>(pos)));
        pos += 1;
        pos += 2;
        //UInt8 time_type = toLittleEndian(*(reinterpret_cast<const UInt8 *>(pos)));
        pos += 1;
        //UInt8 fsp = toLittleEndian(*(reinterpret_cast<const Int8 *>(pos)));
        pos += 1;
        pos += 2;
        MyDateTime mt(year, month, day, hour, minute, second, micro_second);
        col.column->assumeMutable()->insert(Field(mt.toPackedUInt()));
    }
    return pos;
}

const char * arrowNumColToFlashCol(const char * pos, UInt8 field_length, UInt32 null_count, const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> &, const ColumnWithTypeAndName & col, const ColumnInfo & col_info, UInt32 length)
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
            case TiDB::TypeFloat:
                u32 = toLittleEndian(*(reinterpret_cast<const UInt32 *>(pos)));
                std::memcpy(&f32, &u32, sizeof(Float32));
                col.column->assumeMutable()->insert(Field((Float64)f32));
                break;
            case TiDB::TypeDouble:
                u64 = toLittleEndian(*(reinterpret_cast<const UInt64 *>(pos)));
                std::memcpy(&f64, &u64, sizeof(Float64));
                col.column->assumeMutable()->insert(Field(f64));
                break;
            default:
                throw Exception("Should not reach here", ErrorCodes::LOGICAL_ERROR);
        }
    }
    return pos;
}

const char * arrowColToFlashCol(const char * pos, UInt8 field_length, UInt32 null_count, const std::vector<UInt8> & null_bitmap,
    const std::vector<UInt64> & offsets, const ColumnWithTypeAndName & flash_col, const ColumnInfo & col_info, UInt32 length)
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
            return arrowNumColToFlashCol(pos, field_length, null_count, null_bitmap, offsets, flash_col, col_info, length);
        case TiDB::TypeDatetime:
        case TiDB::TypeDate:
        case TiDB::TypeTimestamp:
            return arrowDateColToFlashCol(pos, field_length, null_count, null_bitmap, offsets, flash_col, col_info, length);
        case TiDB::TypeNewDecimal:
            return arrowDecimalColToFlashCol(pos, field_length, null_count, null_bitmap, offsets, flash_col, col_info, length);
        case TiDB::TypeVarString:
        case TiDB::TypeVarchar:
        case TiDB::TypeBlob:
        case TiDB::TypeString:
        case TiDB::TypeTinyBlob:
        case TiDB::TypeMediumBlob:
        case TiDB::TypeLongBlob:
            return arrowStringColToFlashCol(pos, field_length, null_count, null_bitmap, offsets, flash_col, col_info, length);
        default:
            throw Exception("Not supported yet: field tp = " + std::to_string(col_info.tp));
    }
}

} // namespace DB
