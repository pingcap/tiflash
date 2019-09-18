#include <Storages/DeltaMerge/Index/MinMaxIndex.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{
namespace DM
{
MinMaxIndex::MinMaxIndex(const IDataType & type, const IColumn & column, const ColumnVector<UInt8> & del_mark, size_t offset, size_t limit)
{
    const IColumn * column_ptr = &column;
    if (column.isColumnNullable())
    {
        auto & del_mark_data   = del_mark.getData();
        auto & nullable_column = static_cast<const ColumnNullable &>(column);
        auto & null_mark_data  = nullable_column.getNullMapColumn().getData();
        column_ptr             = &nullable_column.getNestedColumn();

        for (size_t i = offset; i < offset + limit; ++i)
        {
            if (!del_mark_data[i] && null_mark_data[i])
            {
                has_null = true;
                break;
            }
        }
    }

#define DISPATCH(TYPE)                                                                           \
    if (typeid_cast<const DataType##TYPE *>(&type))                                              \
        minmax = std::make_shared<MinMaxValueFixed<TYPE>>(*column_ptr, del_mark, offset, limit); \
    else

    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (typeid_cast<const DataTypeDate *>(&type))
        minmax = std::make_shared<MinMaxValueFixed<typename DataTypeDate::FieldType>>(*column_ptr, del_mark, offset, limit);
    else if (typeid_cast<const DataTypeDateTime *>(&type))
        minmax = std::make_shared<MinMaxValueFixed<typename DataTypeDateTime::FieldType>>(*column_ptr, del_mark, offset, limit);
    else if (typeid_cast<const DataTypeUUID *>(&type))
        minmax = std::make_shared<MinMaxValueFixed<typename DataTypeUUID::FieldType>>(*column_ptr, del_mark, offset, limit);
    else if (typeid_cast<const DataTypeEnum<Int8> *>(&type))
        minmax = std::make_shared<MinMaxValueFixed<Int8>>(*column_ptr, del_mark, offset, limit);
    else if (typeid_cast<const DataTypeEnum<Int16> *>(&type))
        minmax = std::make_shared<MinMaxValueFixed<Int16>>(*column_ptr, del_mark, offset, limit);
    else if (typeid_cast<const DataTypeString *>(&type))
        minmax = std::make_shared<MinMaxValueString>(*column_ptr, del_mark, offset, limit);
    else
        minmax = std::make_shared<MinMaxValueDataGeneric>(*column_ptr, del_mark, offset, limit);
}

void MinMaxIndex::merge(const MinMaxIndex & other)
{
    has_null |= other.has_null;
    minmax->merge(*(other.minmax));
}

void MinMaxIndex::write(const IDataType & type, WriteBuffer & buf)
{
    writePODBinary(has_null, buf);
    minmax->write(type, buf);
}

MinMaxIndexPtr MinMaxIndex::read(const IDataType & type, ReadBuffer & buf)
{
    auto v = std::make_shared<MinMaxIndex>();
    readPODBinary(v->has_null, buf);
#define DISPATCH(TYPE)                                       \
    if (typeid_cast<const DataType##TYPE *>(&type))          \
        v->minmax = MinMaxValueFixed<TYPE>::read(type, buf); \
    else
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (typeid_cast<const DataTypeDate *>(&type))
        v->minmax = MinMaxValueFixed<UInt32>::read(type, buf);
    else if (typeid_cast<const DataTypeDateTime *>(&type))
        v->minmax = MinMaxValueFixed<Int64>::read(type, buf);
    else if (typeid_cast<const DataTypeUUID *>(&type))
        v->minmax = MinMaxValueFixed<UInt128>::read(type, buf);
    else if (typeid_cast<const DataTypeEnum<Int8> *>(&type))
        v->minmax = MinMaxValueFixed<Int8>::read(type, buf);
    else if (typeid_cast<const DataTypeEnum<Int16> *>(&type))
        v->minmax = MinMaxValueFixed<Int16>::read(type, buf);
    else if (typeid_cast<const DataTypeString *>(&type))
        v->minmax = MinMaxValueString::read(type, buf);
    else
        v->minmax = MinMaxValueDataGeneric::read(type, buf);
    return v;
}

RSResult MinMaxIndex::checkEqual(const Field & value, const DataTypePtr & type)
{
    if (has_null || value.isNull())
        return Some;
    return minmax->checkEqual(value, type);
}
RSResult MinMaxIndex::checkGreater(const Field & value, const DataTypePtr & type, int /*nan_direction_hint*/)
{
    if (has_null || value.isNull())
        return Some;
    return minmax->checkGreater(value, type);
}
RSResult MinMaxIndex::checkGreaterEqual(const Field & value, const DataTypePtr & type, int /*nan_direction_hint*/)
{
    if (has_null || value.isNull())
        return Some;
    return minmax->checkGreaterEqual(value, type);
}

} // namespace DM
} // namespace DB