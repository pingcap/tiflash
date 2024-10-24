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
#include <Common/Exception.h>
#include <Common/RandomData.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <TestUtils/ColumnGenerator.h>

#include <magic_enum.hpp>

namespace DB::tests
{
ColumnWithTypeAndName ColumnGenerator::generateNullMapColumn(const ColumnGeneratorOpts & opts)
{
    DataTypePtr type = DataTypeFactory::instance().get(opts.type_name);
    assert(type != nullptr && type->getTypeId() == TypeIndex::UInt8);
    auto col = type->createColumn();
    col->reserve(opts.size);
    for (size_t i = 0; i < opts.size; ++i)
        genBool(col);
    return {std::move(col), type, opts.name};
}

ColumnWithTypeAndName ColumnGenerator::generate(const ColumnGeneratorOpts & opts)
{
    RUNTIME_CHECK(opts.distribution == DataDistribution::RANDOM);
    DataTypePtr type;
    if (opts.type_name == "Decimal")
        type = createDecimalType();
    else
        type = DataTypeFactory::instance().get(opts.type_name);

    if (type->isNullable())
    {
        auto nested_column_generator_opts = opts;
        nested_column_generator_opts.type_name = removeNullable(type)->getName();
        auto null_map_column_generator_opts = opts;
        null_map_column_generator_opts.type_name = "UInt8";
        return {
            ColumnNullable::create(
                generate(nested_column_generator_opts).column,
                generateNullMapColumn(null_map_column_generator_opts).column),
            type,
            opts.name};
    }

    auto col = type->createColumn();
    col->reserve(opts.size);

    auto type_id = type->getTypeId();

    switch (type_id)
    {
    case TypeIndex::UInt8:
        if (opts.gen_bool)
        {
            for (size_t i = 0; i < opts.size; ++i)
                genBool<false>(col);
        }
        else
        {
            for (size_t i = 0; i < opts.size; ++i)
                genUInt<UInt8>(col);
        }
        break;
    case TypeIndex::UInt16:
        for (size_t i = 0; i < opts.size; ++i)
            genUInt<UInt16>(col);
        break;
    case TypeIndex::UInt32:
        for (size_t i = 0; i < opts.size; ++i)
            genUInt<UInt32>(col);
        break;
    case TypeIndex::UInt64:
        for (size_t i = 0; i < opts.size; ++i)
            genUInt<UInt64>(col);
        break;
    case TypeIndex::Int8:
        for (size_t i = 0; i < opts.size; ++i)
            genInt<Int8>(col);
        break;
    case TypeIndex::Int16:
        for (size_t i = 0; i < opts.size; ++i)
            genInt<Int16>(col);
        break;
    case TypeIndex::Int32:
        for (size_t i = 0; i < opts.size; ++i)
            genInt<Int32>(col);
        break;
    case TypeIndex::Int64:
        for (size_t i = 0; i < opts.size; ++i)
            genInt<Int64>(col);
        break;
    case TypeIndex::Float32:
    case TypeIndex::Float64:
        for (size_t i = 0; i < opts.size; ++i)
            genFloat(col);
        break;
    case TypeIndex::String:
    {
        auto int_rand_gen = std::uniform_int_distribution<Int64>(0, opts.string_max_size);
        for (size_t i = 0; i < opts.size; ++i)
            genString(col, int_rand_gen(rand_gen));
        break;
    }
    case TypeIndex::FixedString:
    {
        auto int_rand_gen = std::uniform_int_distribution<Int64>(0, opts.string_max_size);
        for (size_t i = 0; i < opts.size; ++i)
            genString(col, int_rand_gen(rand_gen));
        break;
    }
    case TypeIndex::Decimal32:
    case TypeIndex::Decimal64:
    case TypeIndex::Decimal128:
    case TypeIndex::Decimal256:
        for (size_t i = 0; i < opts.size; ++i)
            genDecimal(col, type);
        break;
    case TypeIndex::MyDate:
        for (size_t i = 0; i < opts.size; ++i)
            genDate(col);
        break;
    case TypeIndex::MyDateTime:
        for (size_t i = 0; i < opts.size; ++i)
            genDateTime(col);
        break;
    case TypeIndex::MyTime:
        for (size_t i = 0; i < opts.size; ++i)
            genDuration(col);
        break;
    case TypeIndex::Enum8:
    case TypeIndex::Enum16:
        for (size_t i = 0; i < opts.size; ++i)
            genEnumValue(col, type);
        break;
    case TypeIndex::Array:
    {
        auto nested_type = typeid_cast<const DataTypeArray *>(type.get())->getNestedType();
        size_t elems_size = opts.array_elems_max_size;
        for (size_t i = 0; i < opts.size; ++i)
        {
            if (opts.array_elems_distribution == DataDistribution::RANDOM)
                elems_size = static_cast<UInt64>(rand_gen()) % opts.array_elems_max_size;
            genVector(col, nested_type, elems_size);
        }
        break;
    }
    default:
        throw DB::Exception(
            ErrorCodes::LOGICAL_ERROR,
            "RandomColumnGenerator invalid type, type_id={}",
            magic_enum::enum_name(type_id));
    }

    return {std::move(col), type, opts.name};
}

DataTypePtr ColumnGenerator::createDecimalType()
{
    static const int max_precision = std::to_string(std::numeric_limits<uint64_t>::max()).size();
    int prec = rand_gen() % max_precision + 1;
    int scale = rand_gen() % prec;
    return DB::createDecimal(prec, scale);
}

template <typename IntegerType>
void ColumnGenerator::genInt(MutableColumnPtr & col)
{
    static_assert(std::is_signed_v<IntegerType>);
    constexpr Int64 min_value = std::numeric_limits<IntegerType>::min();
    constexpr Int64 max_value = std::numeric_limits<IntegerType>::max();
    auto init_value = static_cast<Int64>(rand_gen());
    if (init_value > max_value || init_value < min_value)
    {
        init_value = init_value % max_value;
    }
    Field f = init_value;
    col->insert(f);
}

void ColumnGenerator::genEnumValue(MutableColumnPtr & col, DataTypePtr & enum_type)
{
    size_t value_count = 0;
    const auto & enum8_type = static_cast<const DataTypeEnum8 *>(enum_type.get());
    const auto & enum16_type = static_cast<const DataTypeEnum16 *>(enum_type.get());
    if (enum8_type != nullptr)
        value_count = enum8_type->getValues().size();
    else
        value_count = enum16_type->getValues().size();
    auto value_index = static_cast<Int64>(static_cast<Int64>(rand_gen()) % value_count);
    Int64 enum_value = enum8_type == nullptr ? enum16_type->getValues()[value_index].second
                                             : enum8_type->getValues()[value_index].second;
    col->insert(enum_value);
}

template <typename IntegerType>
void ColumnGenerator::genUInt(MutableColumnPtr & col)
{
    static_assert(std::is_unsigned_v<IntegerType>);
    constexpr UInt64 max_value = std::numeric_limits<IntegerType>::max();
    auto init_value = static_cast<UInt64>(rand_gen());
    if (init_value > max_value)
    {
        init_value = init_value % max_value;
    }
    Field f = init_value;
    col->insert(f);
}

template <bool two_value>
void ColumnGenerator::genBool(MutableColumnPtr & col)
{
    auto res = rand_gen() % 8;
    if constexpr (two_value)
        res = res == 0;
    Field f = static_cast<UInt64>(static_cast<UInt64>(res));
    col->insert(f);
}

void ColumnGenerator::genFloat(MutableColumnPtr & col)
{
    Field f = static_cast<Float64>(real_rand_gen(rand_gen));
    col->insert(f);
}

void ColumnGenerator::genString(MutableColumnPtr & col, UInt64 max_size)
{
    Field f = DB::random::randomString(max_size);
    col->insert(f);
}

void ColumnGenerator::genDate(MutableColumnPtr & col)
{
    Field f = parseMyDateTime(DB::random::randomDate());
    col->insert(f);
}

void ColumnGenerator::genDateTime(MutableColumnPtr & col)
{
    Field f = parseMyDateTime(DB::random::randomDateTime());
    col->insert(f);
}

void ColumnGenerator::genDuration(MutableColumnPtr & col)
{
    Field f = parseMyDuration(DB::random::randomDuration());
    col->insert(f);
}

void ColumnGenerator::genDecimal(MutableColumnPtr & col, DataTypePtr & data_type)
{
    auto prec = getDecimalPrecision(*data_type, 0);
    auto scale = getDecimalScale(*data_type, 0);
    auto s = DB::random::randomDecimal(prec, scale);
    bool negative = rand_gen() % 2 == 0;
    Field f;
    if (parseDecimal(s.data(), s.size(), negative, f))
    {
        col->insert(f);
    }
    else
    {
        throw DB::Exception(
            ErrorCodes::LOGICAL_ERROR,
            "RandomColumnGenerator parseDecimal({}, {}) prec {} scale {} fail",
            s,
            negative,
            prec,
            scale);
    }
}

void ColumnGenerator::genVector(MutableColumnPtr & col, DataTypePtr & nested_type, size_t num_vals)
{
    switch (nested_type->getTypeId())
    {
    case TypeIndex::Float32:
    case TypeIndex::Float64:
    {
        Array arr;
        for (size_t i = 0; i < num_vals; ++i)
        {
            arr.push_back(static_cast<Float64>(real_rand_gen(rand_gen)));
            // arr.push_back(static_cast<Float64>(2.5));
        }
        col->insert(arr);
        break;
    }
    default:
        throw DB::Exception(
            ErrorCodes::LOGICAL_ERROR,
            "RandomColumnGenerator invalid nested type in Array(...), type_id={}",
            magic_enum::enum_name(nested_type->getTypeId()));
    }
}

} // namespace DB::tests
