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
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Operators/AutoPassThroughHashAggHelper.h>
#include <common/logger_useful.h>

namespace DB
{
// first_row/min/max and sum whose return type is same with argument type will use this generator:
// 1. For not-null argument: init first_row result as its original value.
// 2. For nullable argument: if original value is not null, init first_row result as its original value, Otherwise init it as Null.
// So copy column is enough for first_row.
ColumnPtr genPassThroughColumnByCopy(
    const TypeIndex & required_type_id,
    const String & src_col_name,
    const Block & child_block)
{
    auto res = child_block.getByName(src_col_name);
    RUNTIME_CHECK(required_type_id == res.type->getTypeId());
    return res.column;
}

ColumnPtr genPassThroughColumnForCountNothing(const Block & child_block)
{
    return ColumnVector<UInt64>::create(child_block.rows(), 0);
}

// For not-null argument: init count result as 1.
// For nullable argument: init count result as 1 if row is not null, otherwise init it as zero.
// NOTE: empty_input_as_null is not consider, because will not use AutoPassHashAgg if group by key is zero.
template <bool nullable, bool empty_argument>
ColumnPtr genPassThroughColumnForCount(size_t arg_index, const Block & child_block)
{
    auto count_agg_func_res = ColumnVector<UInt64>::create(child_block.rows(), 1);
    if constexpr (empty_argument || !nullable)
        return count_agg_func_res;

    const ColumnWithTypeAndName & argument_column = child_block.getByPosition(arg_index);
    const auto * col_nullable = checkAndGetColumn<ColumnNullable>(argument_column.column.get());
    RUNTIME_CHECK(col_nullable);
    auto & datas = count_agg_func_res->getData();
    for (size_t i = 0; i < child_block.rows(); ++i)
    {
        if (col_nullable->isNullAt(i))
            datas[i] = 0;
    }
    return count_agg_func_res;
}

ColumnPtr genPassThroughColumnForNothing(const Block & child_block)
{
    auto nullmap = ColumnVector<UInt8>::create(child_block.rows(), 1);
    auto nested_col = ColumnNothing::create(child_block.rows());
    return ColumnNullable::create(std::move(nested_col), std::move(nullmap));
}

// For not-null argument: init sum result as its original value.
// For nullable column: if original value is not null, init sum result as its original value. Otherwise init it as Null.
// NOTE: Also needs to handle type cast. For example arg type is decimal(15, 2), result type will be decimal(15+22, 2)
template <typename FromDecimalType, typename ToDecimalType, bool nullable>
ColumnPtr genPassThroughColumnForSumDecimal(size_t arg_index, const Block & child_block)
{
    const auto & argument_column = child_block.getByPosition(arg_index);
    const ColumnDecimal<FromDecimalType> * in_col_ptr = nullptr;
    const ColumnNullable * col_nullable_ptr = nullptr;
    ColumnPtr nested_col = nullptr;
    if constexpr (nullable)
    {
        col_nullable_ptr = checkAndGetColumn<ColumnNullable>(argument_column.column.get());
        RUNTIME_CHECK(col_nullable_ptr);
        nested_col = col_nullable_ptr->getNestedColumnPtr();
        if (nested_col->isColumnConst())
            nested_col = nested_col->convertToFullColumnIfConst();
        in_col_ptr = checkAndGetColumn<ColumnDecimal<FromDecimalType>>(nested_col.get());
    }
    else
    {
        nested_col = argument_column.column;
        if (argument_column.column->isColumnConst())
            nested_col = nested_col->convertToFullColumnIfConst();
        in_col_ptr = checkAndGetColumn<ColumnDecimal<FromDecimalType>>(nested_col.get());
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

// todo macro
template <typename FromNumberType, typename ToNumberType, bool nullable>
ColumnPtr genPassThroughColumnForSumNumber(size_t arg_index, const Block & child_block)
{
    const auto & argument_column = child_block.getByPosition(arg_index);
    const ColumnVector<FromNumberType> * in_col_ptr = nullptr;
    const ColumnNullable * col_nullable_ptr = nullptr;
    ColumnPtr nested_col = nullptr;
    if constexpr (nullable)
    {
        col_nullable_ptr = checkAndGetColumn<ColumnNullable>(argument_column.column.get());
        RUNTIME_CHECK(col_nullable_ptr);
        nested_col = col_nullable_ptr->getNestedColumnPtr();
        if (nested_col->isColumnConst())
            nested_col = nested_col->convertToFullColumnIfConst();
        in_col_ptr = checkAndGetColumn<ColumnVector<FromNumberType>>(nested_col.get());
    }
    else
    {
        nested_col = argument_column.column;
        if (nested_col->isColumnConst())
            nested_col = nested_col->convertToFullColumnIfConst();
        in_col_ptr = checkAndGetColumn<ColumnVector<FromNumberType>>(nested_col.get());
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

std::pair<DataTypePtr, DataTypePtr> getNestDataType(const DataTypePtr & arg_from_type, const DataTypePtr & arg_to_type)
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
    return {from_type, to_type};
}

AutoPassThroughColumnGenerator chooseGeneratorForSum(
    const AggregateDescription & agg_desc,
    const ColumnWithTypeAndName & required_column,
    const DataTypePtr & arg_from_type,
    const DataTypePtr & arg_to_type)
{
    RUNTIME_CHECK(agg_desc.argument_names.size() == 1 && agg_desc.arguments.size() == 1);
    auto [from_type, to_type] = getNestDataType(arg_from_type, arg_to_type);
    if ((arg_from_type->isNullable() == arg_to_type->isNullable()) && (from_type->getTypeId() == to_type->getTypeId()))
    {
        return [col_typeid = required_column.type->getTypeId(),
                col_name = agg_desc.argument_names[0]](const Block & block) {
            return genPassThroughColumnByCopy(col_typeid, col_name, block);
        };
    }

#define FOR_DECIMAL_TYPES_INNER(M_outer_type, M) \
    M(M_outer_type, Decimal32)                   \
    M(M_outer_type, Decimal64)                   \
    M(M_outer_type, Decimal128)                  \
    M(M_outer_type, Decimal256)

#define DISPATCH_DECIMAL_INNER(FROM_NATIVE_TYPE, TO_NATIVE_TYPE)                                       \
    do                                                                                                 \
    {                                                                                                  \
        if (checkDataType<DataTypeDecimal<TO_NATIVE_TYPE>>(to_type.get()))                             \
        {                                                                                              \
            size_t col_index = agg_desc.arguments[0];                                                  \
            if (arg_from_type->isNullable())                                                           \
                return [col_index](const Block & block) {                                              \
                    return genPassThroughColumnForSumDecimal<FROM_NATIVE_TYPE, TO_NATIVE_TYPE, true>(  \
                        col_index,                                                                     \
                        block);                                                                        \
                };                                                                                     \
            else                                                                                       \
                return [col_index](const Block & block) {                                              \
                    return genPassThroughColumnForSumDecimal<FROM_NATIVE_TYPE, TO_NATIVE_TYPE, false>( \
                        col_index,                                                                     \
                        block);                                                                        \
                };                                                                                     \
        }                                                                                              \
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

#undef INNER_Decimal32
#undef INNER_Decimal64
#undef INNER_Decimal128
#undef INNER_Decimal256
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

#define DISPATCH_NUMBER_INNER(FROM_NATIVE_TYPE, TO_NATIVE_TYPE)                                                        \
    do                                                                                                                 \
    {                                                                                                                  \
        if (checkDataType<DataTypeNumber<TO_NATIVE_TYPE>>(to_type.get()))                                              \
        {                                                                                                              \
            size_t col_index = agg_desc.arguments[0];                                                                  \
            if (arg_from_type->isNullable())                                                                           \
                return [col_index](const Block & block) {                                                              \
                    return genPassThroughColumnForSumNumber<FROM_NATIVE_TYPE, TO_NATIVE_TYPE, true>(col_index, block); \
                };                                                                                                     \
            else                                                                                                       \
                return [col_index](const Block & block) {                                                              \
                    return genPassThroughColumnForSumNumber<FROM_NATIVE_TYPE, TO_NATIVE_TYPE, false>(                  \
                        col_index,                                                                                     \
                        block);                                                                                        \
                };                                                                                                     \
        }                                                                                                              \
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

#undef INNER_UInt8
#undef INNER_UInt16
#undef INNER_UInt32
#undef INNER_UInt64
#undef INNER_Int8
#undef INNER_Int16
#undef INNER_Int32
#undef INNER_Int64
#undef INNER_Float32
#undef INNER_Float64
#undef DISPATCH_NUMBER_INNER
#undef DISPATCH_NUMBER_OUTER
#undef FOR_NUMBER_TYPES_INNER
#undef FOR_NUMBER_TYPES_OUTER

    throw Exception(fmt::format(
        "choose auto passthrough column generator failed. from type: {}, to type: {}",
        arg_from_type->getName(),
        arg_to_type->getName()));
}

AutoPassThroughColumnGenerator chooseGeneratorForCount(
    const AggregateDescription & agg_desc,
    const Block & child_header)
{
    RUNTIME_CHECK(agg_desc.argument_names.size() <= 1 && agg_desc.arguments.size() <= 1);
    // Argument size can be zero fro count(not null column),
    // because it will be transformed to count() to avoid the cost of convertToFullColumn.
    if (agg_desc.arguments.empty())
    {
        return [](const Block & block) {
            return genPassThroughColumnForCount<false, true>(0, block);
        };
    }

    const DataTypePtr & from_type = child_header.getByPosition(agg_desc.arguments[0]).type;
    if (from_type->isNullable() && from_type->onlyNull())
    {
        return [](const Block & block) {
            return genPassThroughColumnForCountNothing(block);
        };
    }

    const size_t col_index = agg_desc.arguments[0];
    const auto & arg_type = child_header.getByPosition(col_index).type;
    if (arg_type->isNullable())
    {
        return [col_index](const Block & block) {
            return genPassThroughColumnForCount<true, false>(col_index, block);
        };
    }

    return [col_index](const Block & block) {
        return genPassThroughColumnForCount<false, false>(col_index, block);
    };
}

ColumnPtr genPassThroughColumnGeneric(const AggregateDescription & desc, const Block & child_block)
{
    auto new_col = desc.function->getReturnType()->createColumn();
    new_col->reserve(child_block.rows());
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
    const AggregateDescriptions & aggregate_descriptions,
    LoggerPtr log)
{
    std::vector<AutoPassThroughColumnGenerator> results;
    results.reserve(required_header.columns());
    for (size_t col_idx = 0; col_idx < required_header.columns(); ++col_idx)
    {
        const auto & required_column = required_header.getByPosition(col_idx);
        if (child_header.has(required_column.name))
        {
            results.push_back(
                [col_typeid = required_column.type->getTypeId(), col_name = required_column.name](const Block & block) {
                    return genPassThroughColumnByCopy(col_typeid, col_name, block);
                });
            continue;
        }

        AutoPassThroughColumnGenerator generator = nullptr;
        for (const auto & desc : aggregate_descriptions)
        {
            if (desc.column_name == required_column.name)
            {
                const String func_name = desc.function->getName();
                if (desc.arguments.size() == 1)
                    LOG_DEBUG(
                        log,
                        "setup agg func result generator. func: {}, from type: {}, to type: {}",
                        func_name,
                        child_header.getByPosition(desc.arguments[0]).type->getName(),
                        desc.function->getReturnType()->getName());
                else
                    LOG_DEBUG(
                        log,
                        "setup agg func result generator. func: {}, to type: {}",
                        func_name,
                        desc.function->getReturnType()->getName());

                ColumnPtr new_col;
                if (func_name == "nothing")
                {
                    generator = [](const Block & block) {
                        return genPassThroughColumnForNothing(block);
                    };
                }
                else if (func_name.find("sum") != std::string::npos)
                {
                    RUNTIME_CHECK(desc.arguments.size() == 1);
                    RUNTIME_CHECK(required_column.type->getTypeId() == desc.function->getReturnType()->getTypeId());
                    generator = chooseGeneratorForSum(
                        desc,
                        required_column,
                        child_header.getByPosition(desc.arguments[0]).type,
                        desc.function->getReturnType());
                }
                else if (func_name.find("count") != std::string::npos)
                {
                    generator = chooseGeneratorForCount(desc, child_header);
                }
                else if (
                    func_name.find("first_row") != std::string::npos || func_name.find("min") != std::string::npos
                    || func_name.find("max") != std::string::npos)
                {
                    RUNTIME_CHECK(desc.argument_names.size() == 1);
                    generator = [col_typeid = required_column.type->getTypeId(),
                                 col_name = desc.argument_names[0]](const Block & block) {
                        return genPassThroughColumnByCopy(col_typeid, col_name, block);
                    };
                }
                else
                {
                    generator = [&](const Block & block) {
                        return genPassThroughColumnGeneric(desc, block);
                    };
                }

                results.push_back(generator);
                break;
            }
        }
        RUNTIME_CHECK_MSG(
            generator,
            "cannot find required column({}) from aggregate descriptions",
            required_column.name);
    }
    return results;
}
} // namespace DB
