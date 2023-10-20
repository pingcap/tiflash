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

#include <Functions/FunctionsConversion.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <openssl/sha.h>

namespace DB
{
namespace DM
{
using Digest = UInt256;
Digest hashSchema(const Block & schema)
{
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    unsigned char digest_bytes[32];

    const auto & data = schema.getColumnsWithTypeAndName();
    for (const auto & column_with_type_and_name : data)
    {
        // for type infos, we should use getName() instead of getTypeId(),
        // because for all nullable types, getTypeId() will always return TypeIndex::Nullable in getTypeId()
        // but getName() will return the real type name, e.g. Nullable(UInt64), Nullable(datetime(6))
        const auto & type = column_with_type_and_name.type->getName();
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(type.c_str()), type.size());

        const auto & name = column_with_type_and_name.name;
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(name.c_str()), name.size());

        const auto & column_id = column_with_type_and_name.column_id;
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(&column_id), sizeof(column_id));

        const auto & default_value = column_with_type_and_name.default_value.toString();
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(default_value.c_str()), default_value.size());
    }

    SHA256_Final(digest_bytes, &ctx);
    return *(reinterpret_cast<Digest *>(&digest_bytes));
}

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

void appendIntoHandleColumn(
    ColumnVector<Handle>::Container & handle_column,
    const DataTypePtr & type,
    const ColumnPtr & data)
{
    const auto * type_ptr = &(*type);
    size_t size = handle_column.size();

#define APPEND(SHIFT, MARK, DATA_VECTOR)               \
    for (size_t i = 0; i < size; ++i)                  \
    {                                                  \
        handle_column[i] <<= (SHIFT);                  \
        handle_column[i] |= (MARK) & (DATA_VECTOR)[i]; \
    }

    if (checkDataType<DataTypeUInt8>(type_ptr))
    {
        const auto & data_vector = typeid_cast<const ColumnVector<UInt8> &>(*data).getData();
        APPEND(8, 0xFF, data_vector)
    }
    else if (checkDataType<DataTypeUInt16>(type_ptr))
    {
        const auto & data_vector = typeid_cast<const ColumnVector<UInt16> &>(*data).getData();
        APPEND(16, 0xFFFF, data_vector)
    }
    else if (checkDataType<DataTypeUInt32>(type_ptr))
    {
        const auto & data_vector = typeid_cast<const ColumnVector<UInt32> &>(*data).getData();
        APPEND(32, 0xFFFFFFFF, data_vector)
    }
    else if (checkDataType<DataTypeUInt64>(type_ptr))
    {
        const auto & data_vector = typeid_cast<const ColumnVector<UInt64> &>(*data).getData();
        for (size_t i = 0; i < size; ++i)
            handle_column[i] |= data_vector[i];
    }
    else if (checkDataType<DataTypeInt8>(type_ptr))
    {
        const auto & data_vector = typeid_cast<const ColumnVector<Int8> &>(*data).getData();
        APPEND(8, 0xFF, data_vector)
    }
    else if (checkDataType<DataTypeInt16>(type_ptr))
    {
        const auto & data_vector = typeid_cast<const ColumnVector<Int16> &>(*data).getData();
        APPEND(16, 0xFFFF, data_vector)
    }
    else if (checkDataType<DataTypeInt32>(type_ptr))
    {
        const auto & data_vector = typeid_cast<const ColumnVector<Int32> &>(*data).getData();
        APPEND(32, 0xFFFFFFFF, data_vector)
    }
    else if (checkDataType<DataTypeInt64>(type_ptr))
    {
        const auto & data_vector = typeid_cast<const ColumnVector<Int64> &>(*data).getData();
        for (size_t i = 0; i < size; ++i)
            handle_column[i] |= data_vector[i];
    }
    else if (checkDataType<DataTypeDateTime>(type_ptr))
    {
        const auto & data_vector
            = typeid_cast<const ColumnVector<typename DataTypeDateTime::FieldType> &>(*data).getData();
        for (size_t i = 0; i < size; ++i)
            handle_column[i] |= data_vector[i];
    }
    else if (checkDataType<DataTypeDate>(type_ptr))
    {
        const auto & data_vector = typeid_cast<const ColumnVector<typename DataTypeDate::FieldType> &>(*data).getData();
        APPEND(32, 0xFFFFFFFF, data_vector)
    }
    else
        throw Exception("Forgot to support type: " + type->getName());

#undef APPEND
}

} // namespace DM
} // namespace DB
