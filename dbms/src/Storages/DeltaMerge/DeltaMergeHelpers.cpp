#include <Functions/FunctionsConversion.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>

namespace DB
{
namespace DM
{
void convertColumn(Block & block, size_t pos, const DataTypePtr & to_type, const Context & context)
{
    const IDataType * to_type_ptr = to_type.get();

    if (checkDataType<DataTypeUInt8>(to_type_ptr))
        DefaultExecutable(FunctionToUInt8::create(context)).execute(block, {pos}, pos);
    else if (checkDataType<DataTypeUInt16>(to_type_ptr))
        DefaultExecutable(FunctionToUInt16::create(context)).execute(block, {pos}, pos);
    else if (checkDataType<DataTypeUInt32>(to_type_ptr))
        DefaultExecutable(FunctionToUInt32::create(context)).execute(block, {pos}, pos);
    else if (checkDataType<DataTypeUInt64>(to_type_ptr))
        DefaultExecutable(FunctionToUInt64::create(context)).execute(block, {pos}, pos);
    else if (checkDataType<DataTypeInt8>(to_type_ptr))
        DefaultExecutable(FunctionToInt8::create(context)).execute(block, {pos}, pos);
    else if (checkDataType<DataTypeInt16>(to_type_ptr))
        DefaultExecutable(FunctionToInt16::create(context)).execute(block, {pos}, pos);
    else if (checkDataType<DataTypeInt32>(to_type_ptr))
        DefaultExecutable(FunctionToInt32::create(context)).execute(block, {pos}, pos);
    else if (checkDataType<DataTypeInt64>(to_type_ptr))
        DefaultExecutable(FunctionToInt64::create(context)).execute(block, {pos}, pos);
    else
        throw Exception("Forgot to support type: " + to_type->getName());
}

void appendIntoHandleColumn(ColumnVector<Handle>::Container & handle_column, const DataTypePtr & type, const ColumnPtr & data)
{
    auto * type_ptr = &(*type);
    size_t size = handle_column.size();

#define APPEND(SHIFT, MARK, DATA_VECTOR)           \
    for (size_t i = 0; i < size; ++i)              \
    {                                              \
        handle_column[i] <<= SHIFT;                \
        handle_column[i] |= MARK & DATA_VECTOR[i]; \
    }

    if (checkDataType<DataTypeUInt8>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<UInt8> &>(*data).getData();
        APPEND(8, 0xFF, data_vector)
    }
    else if (checkDataType<DataTypeUInt16>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<UInt16> &>(*data).getData();
        APPEND(16, 0xFFFF, data_vector)
    }
    else if (checkDataType<DataTypeUInt32>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<UInt32> &>(*data).getData();
        APPEND(32, 0xFFFFFFFF, data_vector)
    }
    else if (checkDataType<DataTypeUInt64>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<UInt64> &>(*data).getData();
        for (size_t i = 0; i < size; ++i)
            handle_column[i] |= data_vector[i];
    }
    else if (checkDataType<DataTypeInt8>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<Int8> &>(*data).getData();
        APPEND(8, 0xFF, data_vector)
    }
    else if (checkDataType<DataTypeInt16>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<Int16> &>(*data).getData();
        APPEND(16, 0xFFFF, data_vector)
    }
    else if (checkDataType<DataTypeInt32>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<Int32> &>(*data).getData();
        APPEND(32, 0xFFFFFFFF, data_vector)
    }
    else if (checkDataType<DataTypeInt64>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<Int64> &>(*data).getData();
        for (size_t i = 0; i < size; ++i)
            handle_column[i] |= data_vector[i];
    }
    else if (checkDataType<DataTypeDateTime>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<typename DataTypeDateTime::FieldType> &>(*data).getData();
        for (size_t i = 0; i < size; ++i)
            handle_column[i] |= data_vector[i];
    }
    else if (checkDataType<DataTypeDate>(type_ptr))
    {
        auto & data_vector = typeid_cast<const ColumnVector<typename DataTypeDate::FieldType> &>(*data).getData();
        APPEND(32, 0xFFFFFFFF, data_vector)
    }
    else
        throw Exception("Forgot to support type: " + type->getName());

#undef APPEND
}

} // namespace DM
} // namespace DB
