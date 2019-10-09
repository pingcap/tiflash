#include <Flash/Coprocessor/TiDBChunk.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/TiDBDecimal.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

TiDBChunk::TiDBChunk(const std::vector<tipb::FieldType> & field_types)
{
    for (auto & type : field_types)
    {
        switch (type.tp())
        {
            case TiDB::TypeTiny:
            case TiDB::TypeShort:
            case TiDB::TypeInt24:
            case TiDB::TypeLong:
            case TiDB::TypeLongLong:
            case TiDB::TypeYear:
            case TiDB::TypeDouble:
                columns.emplace_back(8);
                break;
            case TiDB::TypeFloat:
                columns.emplace_back(4);
                break;
            case TiDB::TypeDecimal:
            case TiDB::TypeNewDecimal:
                columns.emplace_back(40);
                break;
            case TiDB::TypeDate:
            case TiDB::TypeDatetime:
            case TiDB::TypeNewDate:
            case TiDB::TypeTimestamp:
                columns.emplace_back(20);
                break;
            case TiDB::TypeVarchar:
            case TiDB::TypeVarString:
            case TiDB::TypeString:
            case TiDB::TypeBlob:
            case TiDB::TypeTinyBlob:
            case TiDB::TypeMediumBlob:
            case TiDB::TypeLongBlob:
                columns.emplace_back(VAR_SIZE);
                break;
            default:
                throw Exception("not supported field type in array encode: " + type.DebugString());
        }
    }
}

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
bool flashDecimalColToDAGCol(TiDBColumn & dag_column, const IColumn * flash_col_untyped, const IColumn * null_col,
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
bool flashNumColToDAGCol(TiDBColumn & dag_column, const IColumn * flash_col_untyped, const IColumn * null_col,
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

bool flashDateOrDateTimeColToDAGCol(TiDBColumn & dag_column, const IColumn * flash_col_untyped, const IColumn * null_col,
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

bool flashStringColToDAGCol(TiDBColumn & dag_column, const IColumn * flash_col_untyped, const IColumn * null_col,
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

void flashColToDAGCol(TiDBColumn & dag_column, const ColumnWithTypeAndName & flash_col, const tipb::FieldType & field_type,
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
        if (!(flashDateOrDateTimeColToDAGCol(dag_column, col, null_col, field_type, type, start_index, end_index)
                || flashNumColToDAGCol<UInt8>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToDAGCol<UInt16>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToDAGCol<UInt32>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToDAGCol<UInt64>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToDAGCol<UInt128>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToDAGCol<Int8>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToDAGCol<Int16>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToDAGCol<Int32>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToDAGCol<Int64>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToDAGCol<Float32>(dag_column, col, null_col, field_type, start_index, end_index)
                || flashNumColToDAGCol<Float64>(dag_column, col, null_col, field_type, start_index, end_index)))
        {
            throw Exception("Illegal column " + col->getName() + " when try to convert flash col to DAG col");
        }
    }
    else if (!(flashDecimalColToDAGCol<Decimal32>(dag_column, col, null_col, field_type, type, start_index, end_index)
                 || flashDecimalColToDAGCol<Decimal64>(dag_column, col, null_col, field_type, type, start_index, end_index)
                 || flashDecimalColToDAGCol<Decimal128>(dag_column, col, null_col, field_type, type, start_index, end_index)
                 || flashDecimalColToDAGCol<Decimal256>(dag_column, col, null_col, field_type, type, start_index, end_index)
                 || flashStringColToDAGCol(dag_column, col, null_col, field_type, start_index, end_index)))
    {
        throw Exception("Illegal column " + col->getName() + " when try to convert flash col to DAG col");
    }
}


void TiDBChunk::buildDAGChunkFromBlock(
    const Block & block, const std::vector<tipb::FieldType> & field_types, size_t start_index, size_t end_index)
{
    for (size_t i = 0; i < block.columns(); i++)
    {
        flashColToDAGCol(columns[i], block.getByPosition(i), field_types[i], start_index, end_index);
    }
}

} // namespace DB
