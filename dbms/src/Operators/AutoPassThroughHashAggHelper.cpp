// Copyright 2024 PingCAP, Inc.
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
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Operators/AutoPassThroughHashAggHelper.h>
#include <common/logger_useful.h>

namespace DB
{
// todo list all situations:
// first_row use this generator:
// 1. For not-null argument: init first_row result as its original value.
// 2. For nullable argument: if original value is not null, init first_row result as its original value, Otherwise init it as Null.
// So copy column is enough for first_row.
ColumnPtr genPassThroughColumnByCopy(
    const DataTypePtr & required_col_type,
    const String & src_col_name,
    const Block & child_block)
{
    // todo maybe add debug log
    auto res = child_block.getByName(src_col_name);
    RUNTIME_CHECK(required_col_type->getTypeId() == res.type->getTypeId());
    return res.column;
}

// For not-null argument: init count result as 1.
// For nullable argument: init count result as 1 if row is not null, otherwise init it as zero.
// NOTE: empty_input_as_null is not consider, because will not use AutoPassHashAgg if group by key is zero.
// todo template nullable
ColumnPtr genPassThroughColumnForCount(const AggregateDescription & desc, const Block & child_block)
{
    RUNTIME_CHECK(desc.arguments.size() <= 1);

    // todo template arg num
    auto count_agg_func_res = ColumnVector<UInt64>::create(child_block.rows(), 1);
    if (desc.arguments.empty())
        return count_agg_func_res;

    const ColumnWithTypeAndName & argument_column = child_block.getByPosition(desc.arguments[0]);
    if (argument_column.type->isNullable())
    {
        const auto * col_nullable = checkAndGetColumn<ColumnNullable>(argument_column.column.get());
        auto nullmap = col_nullable->getNullMapColumnPtr()->cloneFullColumn();
        return ColumnNullable::create(std::move(count_agg_func_res), std::move(nullmap));
    }
    // if (argument_column.type->isNullable())
    // {
    //     const auto * col_nullable = checkAndGetColumn<ColumnNullable>(argument_column.column.get());
    //     auto & datas = count_agg_func_res->getData();
    //     for (size_t i = 0; i < child_block.rows(); ++i)
    //     {
    //         if (col_nullable->isNullAt(i))
    //             datas[i] = 0;
    //     }
    // }
    return count_agg_func_res;
}

// todo FromDecimalType -> FROM_DECIMAL_TYPE?
// For not-null argument: init sum result as its original value.
// For nullable column: if original value is not null, init sum result as its original value. Otherwise init it as Null.
// NOTE: Also needs to handle type cast. For example arg type is decimal(15, 2), result type will be decimal(15+22, 2)
template <typename FromDecimalType, typename ToDecimalType, bool nullable>
ColumnPtr genPassThroughColumnForSumDecimal(const AggregateDescription & desc, const Block & child_block)
{
    RUNTIME_CHECK(desc.arguments.size() == 1);

    const auto & argument_column = child_block.getByPosition(desc.arguments[0]);
    const ColumnDecimal<FromDecimalType> * in_col_ptr = nullptr;
    const ColumnNullable * col_nullable_ptr = nullptr;
    if constexpr (nullable)
    {
        col_nullable_ptr = checkAndGetColumn<ColumnNullable>(argument_column.column.get());
        RUNTIME_CHECK(col_nullable_ptr);
        in_col_ptr = checkAndGetColumn<ColumnDecimal<FromDecimalType>>(col_nullable_ptr->getNestedColumnPtr().get());
    }
    else
    {
        in_col_ptr = checkAndGetColumn<ColumnDecimal<FromDecimalType>>(argument_column.column.get());
    }
    RUNTIME_CHECK(in_col_ptr);

    const typename ColumnDecimal<FromDecimalType>::Container & in_datas = in_col_ptr->getData();
    const auto scale = in_col_ptr->getScale();

    auto out_col = ColumnDecimal<ToDecimalType>::create(0, scale);
    auto & out_datas = out_col->getData();
    out_datas.reserve(in_datas.size());

    for (size_t i = 0; i < in_datas.size(); ++i)
    {
        out_datas.push_back(static_cast<ToDecimalType>(in_datas[i]));
    }

    if constexpr (nullable)
    {
        auto nullmap = col_nullable_ptr->getNullMapColumnPtr()->cloneFullColumn();
        return ColumnNullable::create(std::move(out_col), std::move(nullmap));
    }
    return out_col;
}

// todo macro same with genPassThroughColumnForSumDecimal
template <typename FromNumberType, typename ToNumberType, bool nullable>
ColumnPtr genPassThroughColumnForSumNumber(const AggregateDescription & desc, const Block & child_block)
{
    RUNTIME_CHECK(desc.arguments.size() == 1);

    const auto & argument_column = child_block.getByPosition(desc.arguments[0]);
    const ColumnVector<FromNumberType> * in_col_ptr = nullptr;
    const ColumnNullable * col_nullable_ptr = nullptr;
    if constexpr (nullable)
    {
        col_nullable_ptr = checkAndGetColumn<ColumnNullable>(argument_column.column.get());
        RUNTIME_CHECK(col_nullable_ptr);
        in_col_ptr = checkAndGetColumn<ColumnVector<FromNumberType>>(col_nullable_ptr->getNestedColumnPtr().get());
    }
    else
    {
        in_col_ptr = checkAndGetColumn<ColumnVector<FromNumberType>>(argument_column.column.get());
    }
    RUNTIME_CHECK(in_col_ptr);

    const typename ColumnVector<FromNumberType>::Container & in_datas = in_col_ptr->getData();

    auto out_col = ColumnVector<ToNumberType>::create(0);
    auto & out_datas = out_col->getData();
    out_datas.reserve(in_datas.size());

    for (size_t i = 0; i < in_datas.size(); ++i)
    {
        out_datas.push_back(static_cast<ToNumberType>(in_datas[i]));
    }

    if constexpr (nullable)
    {
        auto nullmap = col_nullable_ptr->getNullMapColumnPtr();
        return ColumnNullable::create(std::move(out_col), std::move(nullmap));
    }
    return out_col;
}

AutoPassThroughColumnGenerator chooseGeneratorForSum(
    const AggregateDescription & agg_desc,
    const ColumnWithTypeAndName & required_column,
    const DataTypePtr & arg_from_type,
    const DataTypePtr & arg_to_type)
{
    DataTypePtr from_type;
    DataTypePtr to_type;
    if (const auto * arg_from_type_ptr = checkAndGetDataType<DataTypeNullable>(arg_from_type.get()))
        from_type = arg_from_type_ptr->getNestedType();
    else
        from_type = arg_from_type;

    if (const auto * arg_to_type_ptr = checkAndGetDataType<DataTypeNullable>(arg_to_type.get()))
        to_type = arg_to_type_ptr->getNestedType();
    else
        to_type = arg_to_type;

    RUNTIME_CHECK(agg_desc.argument_names.size() == 1);

    if ((arg_from_type->isNullable() == arg_to_type->isNullable()) && (from_type->getTypeId() == to_type->getTypeId()))
        return std::bind(
            genPassThroughColumnByCopy,
            required_column.type,
            agg_desc.argument_names[0],
            std::placeholders::_1); // NOLINT

#define FOR_DECIMAL_TYPES_INNER(M_outer_type, M) \
    M(M_outer_type, Decimal32)                   \
    M(M_outer_type, Decimal64)                   \
    M(M_outer_type, Decimal128)                  \
    M(M_outer_type, Decimal256)

#define DISPATCH_DECIMAL_INNER(FROM_NATIVE_TYPE, TO_NATIVE_TYPE)                                \
    do                                                                                          \
    {                                                                                           \
        if (checkDataType<DataTypeDecimal<TO_NATIVE_TYPE>>(to_type.get()))                      \
        {                                                                                       \
            if (arg_from_type->isNullable())                                                    \
                return std::bind(                                                               \
                    genPassThroughColumnForSumDecimal<FROM_NATIVE_TYPE, TO_NATIVE_TYPE, true>,  \
                    agg_desc,                                                                   \
                    std::placeholders::_1);                                                     \
            else                                                                                \
                return std::bind(                                                               \
                    genPassThroughColumnForSumDecimal<FROM_NATIVE_TYPE, TO_NATIVE_TYPE, false>, \
                    agg_desc,                                                                   \
                    std::placeholders::_1);                                                     \
        }                                                                                       \
    } while (0);

#define INNER_Decimal32 FOR_DECIMAL_TYPES_INNER(Decimal32, DISPATCH_DECIMAL_INNER)
#define INNER_Decimal64 FOR_DECIMAL_TYPES_INNER(Decimal64, DISPATCH_DECIMAL_INNER)
#define INNER_Decimal128 FOR_DECIMAL_TYPES_INNER(Decimal128, DISPATCH_DECIMAL_INNER)
#define INNER_Decimal256 FOR_DECIMAL_TYPES_INNER(Decimal256, DISPATCH_DECIMAL_INNER)

#define DISPATCH_DECIMAL_OUTER(FROM_NATIVE_TYPE)                               \
    do                                                                         \
    {                                                                          \
        if (checkDataType<DataTypeDecimal<FROM_NATIVE_TYPE>>(from_type.get())) \
        {                                                                      \
            INNER_##FROM_NATIVE_TYPE                                           \
        }                                                                      \
    } while (0);

#define FOR_DECIMAL_TYPES_OUTER(M) \
    M(Decimal32)                   \
    M(Decimal64)                   \
    M(Decimal128)                  \
    M(Decimal256)

    FOR_DECIMAL_TYPES_OUTER(DISPATCH_DECIMAL_OUTER);

#undef DISPATCH_DECIMAL_INNER
#undef DISPATCH_DECIMAL_OUTER
#undef FOR_DECIMAL_TYPES_INNER
#undef FOR_DECIMAL_TYPES_OUTER

#define FOR_NUMBER_TYPES_INNER(M_outer_type, M) \
    M(M_outer_type, UInt8)                      \
    M(M_outer_type, UInt16)                     \
    M(M_outer_type, UInt32)                     \
    M(M_outer_type, UInt64)                     \
    M(M_outer_type, Int8)                       \
    M(M_outer_type, Int16)                      \
    M(M_outer_type, Int32)                      \
    M(M_outer_type, Int64)                      \
    M(M_outer_type, Float32)                    \
    M(M_outer_type, Float64)

    // todo NUMERIC -> NUMBER
#define DISPATCH_NUMBER_INNER(FROM_NATIVE_TYPE, TO_NATIVE_TYPE)                                \
    do                                                                                         \
    {                                                                                          \
        if (checkDataType<DataTypeNumber<TO_NATIVE_TYPE>>(to_type.get()))                      \
        {                                                                                      \
            if (arg_from_type->isNullable())                                                   \
                return std::bind(                                                              \
                    genPassThroughColumnForSumNumber<FROM_NATIVE_TYPE, TO_NATIVE_TYPE, true>,  \
                    agg_desc,                                                                  \
                    std::placeholders::_1);                                                    \
            else                                                                               \
                return std::bind(                                                              \
                    genPassThroughColumnForSumNumber<FROM_NATIVE_TYPE, TO_NATIVE_TYPE, false>, \
                    agg_desc,                                                                  \
                    std::placeholders::_1);                                                    \
        }                                                                                      \
    } while (0);

#define INNER_UInt8 FOR_NUMBER_TYPES_INNER(UInt8, DISPATCH_NUMBER_INNER)
#define INNER_UInt16 FOR_NUMBER_TYPES_INNER(UInt16, DISPATCH_NUMBER_INNER)
#define INNER_UInt32 FOR_NUMBER_TYPES_INNER(UInt32, DISPATCH_NUMBER_INNER)
#define INNER_UInt64 FOR_NUMBER_TYPES_INNER(UInt64, DISPATCH_NUMBER_INNER)
#define INNER_Int8 FOR_NUMBER_TYPES_INNER(Int8, DISPATCH_NUMBER_INNER)
#define INNER_Int16 FOR_NUMBER_TYPES_INNER(Int16, DISPATCH_NUMBER_INNER)
#define INNER_Int32 FOR_NUMBER_TYPES_INNER(Int32, DISPATCH_NUMBER_INNER)
#define INNER_Int64 FOR_NUMBER_TYPES_INNER(Int64, DISPATCH_NUMBER_INNER)
#define INNER_Float32 FOR_NUMBER_TYPES_INNER(Float32, DISPATCH_NUMBER_INNER)
#define INNER_Float64 FOR_NUMBER_TYPES_INNER(Float64, DISPATCH_NUMBER_INNER)

#define DISPATCH_NUMBER_OUTER(FROM_NATIVE_TYPE)                               \
    do                                                                        \
    {                                                                         \
        if (checkDataType<DataTypeNumber<FROM_NATIVE_TYPE>>(from_type.get())) \
        {                                                                     \
            INNER_##FROM_NATIVE_TYPE                                          \
        }                                                                     \
    } while (0);

#define FOR_NUMBER_TYPES_OUTER(M) \
    M(UInt8)                      \
    M(UInt16)                     \
    M(UInt32)                     \
    M(UInt64)                     \
    M(Int8)                       \
    M(Int16)                      \
    M(Int32)                      \
    M(Int64)                      \
    M(Float32)                    \
    M(Float64)

    FOR_NUMBER_TYPES_OUTER(DISPATCH_NUMBER_OUTER);

#undef DISPATCH_NUMBER_INNER
#undef DISPATCH_NUMBER_OUTER
#undef FOR_NUMBER_TYPES_INNER
#undef FOR_NUMBER_TYPES_OUTER

    throw Exception(fmt::format(
        "choose auto passthrough column generator failed. from type: {}, to type: {}",
        arg_from_type->getName(),
        arg_to_type->getName()));
}

ColumnPtr genPassThroughColumnGeneric(const AggregateDescription & desc, const Block & child_block)
{
    auto new_col = desc.function->getReturnType()->createColumn();
    new_col->reserve(child_block.rows());
    // todo maybe too large?
    Arena arena;
    auto * place = arena.alignedAlloc(desc.function->sizeOfData(), desc.function->alignOfData());

    ColumnRawPtrs argument_columns;
    argument_columns.reserve(child_block.columns());
    for (auto arg_col_idx : desc.arguments)
        argument_columns.push_back(child_block.getByPosition(arg_col_idx).column.get());

    RUNTIME_CHECK(!argument_columns.empty());

    for (size_t row_idx = 0; row_idx < child_block.rows(); ++row_idx)
    {
        desc.function->create(place);
        desc.function->add(place, argument_columns.data(), row_idx, &arena);
        desc.function->insertResultInto(place, *new_col, &arena);
    }
    return new_col;
}

std::vector<AutoPassThroughColumnGenerator> setupAutoPassThroughColumnGenerator(
    const Block & required_header,
    const Block & child_header,
    const AggregateDescriptions & aggregate_descriptions)
{
    std::vector<AutoPassThroughColumnGenerator> results;
    results.reserve(required_header.columns());
    for (size_t col_idx = 0; col_idx < required_header.columns(); ++col_idx)
    {
        const auto & required_column = required_header.getByPosition(col_idx);
        if (child_header.has(required_column.name))
        {
            results.push_back(std::bind(
                genPassThroughColumnByCopy,
                required_column.type,
                required_column.name,
                std::placeholders::_1)); // NOLINT
            continue;
        }

        bool agg_func_col_found = false;
        for (const auto & desc : aggregate_descriptions)
        {
            if (desc.column_name == required_column.name)
            {
                const String func_name = desc.function->getName();
                ColumnPtr new_col;
                // todo col_name has extra space ending
                // todo make sure it's lowercase
                if (func_name.find("sum") != std::string::npos)
                {
                    RUNTIME_CHECK(desc.arguments.size() == 1);
                    RUNTIME_CHECK(required_column.type->getTypeId() == desc.function->getReturnType()->getTypeId());
                    results.push_back(chooseGeneratorForSum(
                        desc,
                        required_column,
                        child_header.getByPosition(desc.arguments[0]).type,
                        desc.function->getReturnType()));
                }
                else if (func_name.find("count") != std::string::npos)
                {
                    // Argument size can be zero fro count(not null column),
                    // because it will be transformed to count() to avoid the cost of convertToFullColumn.
                    RUNTIME_CHECK(desc.arguments.size() <= 1);
                    results.push_back(std::bind(genPassThroughColumnForCount, desc, std::placeholders::_1)); // NOLINT
                }
                else if (func_name.find("first_row") != std::string::npos)
                {
                    RUNTIME_CHECK(desc.argument_names.size() == 1);
                    results.push_back(std::bind(
                        genPassThroughColumnByCopy,
                        required_column.type,
                        desc.argument_names[0],
                        std::placeholders::_1)); // NOLINT
                }
                else
                {
                    // todo other agg funcs
                    results.push_back(std::bind(genPassThroughColumnGeneric, desc, std::placeholders::_1)); // NOLINT
                }
                agg_func_col_found = true;
                break;
            }
        }
        RUNTIME_CHECK_MSG(
            agg_func_col_found,
            "cannot find required column({}) from aggregate descriptions",
            required_column.name);
    }
    return results;
}
} // namespace DB
