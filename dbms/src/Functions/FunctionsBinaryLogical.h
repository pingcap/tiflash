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

#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>

#include <type_traits>

#include "Columns/ColumnVector.h"
#include "Columns/IColumn.h"
#include "Common/FieldVisitors.h"
#include "Functions/FunctionBinaryArithmetic.h"


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Behaviour in presence of NULLs:
  *
  * Functions AND, XOR, NOT use default implementation for NULLs:
  * - if one of arguments is Nullable, they return Nullable result where NULLs are returned when at least one argument was NULL.
  *
  * But function OR is different.
  * It always return non-Nullable result and NULL are equivalent to 0 (false).
  * For example, 1 OR NULL returns 1, not NULL.
  */


struct BinaryAndImpl
{
    static inline void evaluate(bool a, bool b, UInt8 & res) { res=  a && b; }
    static inline void evaluateNullable(bool a, bool a_is_null, bool b, bool b_is_null, UInt8 & res, UInt8 & res_is_null) 
    { 
        res =  a && b; 
    }
};

struct BinaryOrImpl
{
    static inline void evaluate(bool a, bool b, UInt8 & res) { res = a || b; }
    static inline void evaluateNullable(bool a, bool a_is_null, bool b, bool b_is_null, UInt8 & res, UInt8 & res_is_null) 
    { 
        res = a || b; 
    }
};

struct BinaryXorImpl
{
    static inline void evaluate(bool a, bool b, UInt8 & res) { res = a != b; }
    static inline void evaluateNullable(bool a, bool a_is_null, bool b, bool b_is_null, UInt8 & res, UInt8 & res_is_null) 
    { 
        res_is_null = a_is_null || b_is_null;
        res = a != b; 
    }
};

using UInt8Container = ColumnUInt8::Container;
using UInt8ColumnPtrs = std::vector<const ColumnUInt8 *>;


/**
 * The behavior of and and or is the same as
 * https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
 */
template <typename Impl, typename Name, bool defaultImplForNull>
class FunctionBinaryLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBinaryLogical>(); }

public:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForNulls() const override { return defaultImplForNull; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        bool has_nullable_input_column = false;
        for (size_t i = 0; i < 2; ++i)
        {
            has_nullable_input_column |= arguments[i]->isNullable();
            if (!removeNullable(arguments[i])->isNumber() && !arguments[i]->onlyNull())
            {
                throw Exception(
                    "Illegal type (" + arguments[i]->getName() + ") of " + toString(i + 1) + " argument of function "
                        + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        if (has_nullable_input_column)
            return makeNullable(std::make_shared<DataTypeUInt8>());
        else
            return std::make_shared<DataTypeUInt8>();
    }

private:
    template <typename T>
    bool executeConstantVectorNullable(
        const IColumn * column,
        const ConstNullMapPtr column_null_map,
        bool constant_value,
        bool is_constant_null,
        UInt8Container & res,
        UInt8Container & result_null_map)
    {
        auto col = checkAndGetColumn<ColumnVector<T>>(column);
        if (!col)
            return false;
        const auto & vec = col->getData();
        size_t n = res.size();
        if (column_null_map == nullptr)
        {
            for (size_t i = 0; i < n; ++i)
            {
                Impl::evaluateNullable(vec[i], false, constant_value, is_constant_null, res[i], result_null_map[i]);
            }
        }
        else
        {
            for (size_t i = 0; i < n; ++i)
            {
                Impl::evaluateNullable(
                    vec[i],
                    (*column_null_map)[i],
                    constant_value,
                    is_constant_null,
                    res[i],
                    result_null_map[i]);
            }
        }
    }
    template <typename T>
    bool executeConstantVectorNotNull(const IColumn * column, bool constant_value, UInt8Container & res)
    {
        auto col = checkAndGetColumn<ColumnVector<T>>(column);
        if (!col)
            return false;
        const auto & vec = col->getData();
        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
        {
            Impl::evaluate(vec[i], constant_value, res[i]);
        }
    }

    void executeConstantVector(
        Block & block,
        size_t result,
        ColumnPtr & column,
        bool is_constant_null,
        bool constant_value)
    {
        ColumnPtr not_null_column = column;
        ConstNullMapPtr null_map_ptr = nullptr;
        if constexpr (!defaultImplForNull)
        {
            // defaultImplForNull == false means the input column is always not-nullable
            if (column->isColumnNullable())
            {
                const auto & col_nullable = checkAndGetColumn<ColumnNullable>(column.get());
                not_null_column = col_nullable->getNestedColumnPtr();
                null_map_ptr = &col_nullable->getNullMapData();
            }
        }
        auto result_is_nullable = !defaultImplForNull && (is_constant_null || null_map_ptr != nullptr);

        size_t rows = block.rows();
        auto result_vec = ColumnUInt8::create(rows);
        auto & result_vec_data = result_vec->getData();
        if (!result_is_nullable)
        {
            if (!executeConstantVectorNotNull<Int8>(not_null_column.get(), constant_value, result_vec_data)
                && !executeConstantVectorNotNull<Int16>(not_null_column.get(), constant_value, result_vec_data)
                && !executeConstantVectorNotNull<Int32>(not_null_column.get(), constant_value, result_vec_data)
                && !executeConstantVectorNotNull<Int64>(not_null_column.get(), constant_value, result_vec_data)
                && !executeConstantVectorNotNull<UInt8>(not_null_column.get(), constant_value, result_vec_data)
                && !executeConstantVectorNotNull<UInt16>(not_null_column.get(), constant_value, result_vec_data)
                && !executeConstantVectorNotNull<UInt32>(not_null_column.get(), constant_value, result_vec_data)
                && !executeConstantVectorNotNull<UInt64>(not_null_column.get(), constant_value, result_vec_data)
                && !executeConstantVectorNotNull<Float32>(not_null_column.get(), constant_value, result_vec_data)
                && !executeConstantVectorNotNull<Float64>(not_null_column.get(), constant_value, result_vec_data))
                throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
            block.getByPosition(result).column = std::move(result_vec);
        }
        else
        {
            auto result_null_map_vec = ColumnUInt8::create(rows);
            auto & result_null_map_vec_data = result_null_map_vec->getData();
            if (!executeConstantVectorNullable<Int8>(
                    not_null_column.get(),
                    null_map_ptr,
                    constant_value,
                    is_constant_null,
                    result_vec_data,
                    result_null_map_vec_data)
                && !executeConstantVectorNullable<Int16>(
                    not_null_column.get(),
                    null_map_ptr,
                    constant_value,
                    is_constant_null,
                    result_vec_data,
                    result_null_map_vec_data)
                && !executeConstantVectorNullable<Int32>(
                    not_null_column.get(),
                    null_map_ptr,
                    constant_value,
                    is_constant_null,
                    result_vec_data,
                    result_null_map_vec_data)
                && !executeConstantVectorNullable<Int64>(
                    not_null_column.get(),
                    null_map_ptr,
                    constant_value,
                    is_constant_null,
                    result_vec_data,
                    result_null_map_vec_data)
                && !executeConstantVectorNullable<UInt8>(
                    not_null_column.get(),
                    null_map_ptr,
                    constant_value,
                    is_constant_null,
                    result_vec_data,
                    result_null_map_vec_data)
                && !executeConstantVectorNullable<UInt16>(
                    not_null_column.get(),
                    null_map_ptr,
                    constant_value,
                    is_constant_null,
                    result_vec_data,
                    result_null_map_vec_data)
                && !executeConstantVectorNullable<UInt32>(
                    not_null_column.get(),
                    null_map_ptr,
                    constant_value,
                    is_constant_null,
                    result_vec_data,
                    result_null_map_vec_data)
                && !executeConstantVectorNullable<UInt64>(
                    not_null_column.get(),
                    null_map_ptr,
                    constant_value,
                    is_constant_null,
                    result_vec_data,
                    result_null_map_vec_data)
                && !executeConstantVectorNullable<Float32>(
                    not_null_column.get(),
                    null_map_ptr,
                    constant_value,
                    is_constant_null,
                    result_vec_data,
                    result_null_map_vec_data)
                && !executeConstantVectorNullable<Float64>(
                    not_null_column.get(),
                    null_map_ptr,
                    constant_value,
                    is_constant_null,
                    result_vec_data,
                    result_null_map_vec_data))
                throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
            block.getByPosition(result).column
                = ColumnNullable::create(std::move(result_vec), std::move(result_null_map_vec));
        }
    }

    template <typename LeftType, typename RightType>
    bool executeVectorVectorNotNullRight(
        const ColumnVector<LeftType> * column_a,
        IColumn * column_b,
        UInt8Container & res)
    {
        auto col = checkAndGetColumn<ColumnVector<RightType>>(column_b);
        if (!col)
            return false;
        auto & data_a = column_a->getData();
        auto & data_b = col->getData();
        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
            Impl::evaluate(data_a[i], data_b[i], res[i]);
    }

    template <typename LeftType, typename RightType>
    bool executeVectorVectorNullableRight(
        const ColumnVector<LeftType> * column_a,
        ConstNullMapPtr column_a_null_map,
        IColumn * column_b,
        ConstNullMapPtr column_b_null_map,
        UInt8Container & res,
        UInt8Container & res_null_map_data)
    {
        auto col = checkAndGetColumn<ColumnVector<RightType>>(column_b);
        if (!col)
            return false;
        auto & data_a = column_a->getData();
        auto & data_b = col->getData();
        size_t n = res.size();
        if (column_a_null_map == nullptr)
        {
            for (size_t i = 0; i < n; ++i)
                Impl::evaluateNullable(data_a[i], false, data_b[i], column_b_null_map[i], res[i], res_null_map_data[i]);
        }
        else if (column_b_null_map == nullptr)
        {
            for (size_t i = 0; i < n; ++i)
                Impl::evaluateNullable(data_a[i], column_a_null_map[i], data_b[i], false, res[i], res_null_map_data[i]);
        }
        else
        {
            for (size_t i = 0; i < n; ++i)
                Impl::evaluateNullable(
                    data_a[i],
                    column_a_null_map[i],
                    data_b[i],
                    column_b_null_map[i],
                    res[i],
                    res_null_map_data[i]);
        }
    }

    template <typename T>
    bool executeVectorVectorNotNullLeft(IColumn * column_a, IColumn * column_b, UInt8Container & res)
    {
        auto col = checkAndGetColumn<ColumnVector<T>>(column_a);
        if (!col)
            return false;
        if (!executeVectorVectorNotNullRight<T, Int8>(col, column_b, res)
            && !executeVectorVectorNotNullRight<T, Int16>(col, column_b, res)
            && !executeVectorVectorNotNullRight<T, Int32>(col, column_b, res)
            && !executeVectorVectorNotNullRight<T, Int64>(col, column_b, res)
            && !executeVectorVectorNotNullRight<T, UInt8>(col, column_b, res)
            && !executeVectorVectorNotNullRight<T, UInt16>(col, column_b, res)
            && !executeVectorVectorNotNullRight<T, UInt32>(col, column_b, res)
            && !executeVectorVectorNotNullRight<T, UInt64>(col, column_b, res)
            && !executeVectorVectorNotNullRight<T, Float32>(col, column_b, res)
            && !executeVectorVectorNotNullRight<T, Float64>(col, column_b, res))
            throw Exception("Unexpected type of column: " + column_b->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename T>
    bool executeVectorVectorNullableLeft(
        IColumn * column_a,
        ConstNullMapPtr column_a_null_map,
        IColumn * column_b,
        ConstNullMapPtr column_b_null_map,
        UInt8Container & res,
        UInt8Container & res_null_map_data)
    {
        auto col = checkAndGetColumn<ColumnVector<T>>(column_a);
        if (!col)
            return false;
        if (!executeVectorVectorNullableRight<T, Int8>(col, column_a_null_map, column_b, column_b_null_map, res, res_null_map_data)
            && !executeVectorVectorNullableRight<T, Int16>(col, column_a_null_map, column_b, column_b_null_map, res, res_null_map_data)
            && !executeVectorVectorNullableRight<T, Int32>(col, column_a_null_map, column_b, column_b_null_map, res, res_null_map_data)
            && !executeVectorVectorNullableRight<T, Int64>(col, column_a_null_map, column_b, column_b_null_map, res, res_null_map_data)
            && !executeVectorVectorNullableRight<T, UInt8>(col, column_a_null_map, column_b, column_b_null_map, res, res_null_map_data)
            && !executeVectorVectorNullableRight<T, UInt16>(col, column_a_null_map, column_b, column_b_null_map, res, res_null_map_data)
            && !executeVectorVectorNullableRight<T, UInt32>(col, column_a_null_map, column_b, column_b_null_map, res, res_null_map_data)
            && !executeVectorVectorNullableRight<T, UInt64>(col, column_a_null_map, column_b, column_b_null_map, res, res_null_map_data)
            && !executeVectorVectorNullableRight<T, Float32>(col, column_a_null_map, column_b, column_b_null_map, res, res_null_map_data)
            && !executeVectorVectorNullableRight<T, Float64>(col, column_a_null_map, column_b, column_b_null_map, res, res_null_map_data))
            throw Exception("Unexpected type of column: " + column_b->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    void executeVectorVector(Block & block, size_t result, ColumnPtr & column_a, ColumnPtr & column_b)
    {
        ColumnPtr not_null_column_a = column_a;
        ConstNullMapPtr column_a_null_map_ptr = nullptr;
        ColumnPtr not_null_column_b = column_b;
        ConstNullMapPtr column_b_null_map_ptr = nullptr;
        if constexpr (!defaultImplForNull)
        {
            // defaultImplForNull == false means the input column is always not-nullable
            if (column_a->isColumnNullable())
            {
                const auto & col_nullable = checkAndGetColumn<ColumnNullable>(column_a.get());
                not_null_column_a = col_nullable->getNestedColumnPtr();
                column_a_null_map_ptr = &col_nullable->getNullMapData();
            }
            if (column_b->isColumnNullable())
            {
                const auto & col_nullable = checkAndGetColumn<ColumnNullable>(column_b.get());
                not_null_column_b = col_nullable->getNestedColumnPtr();
                column_b_null_map_ptr = &col_nullable->getNullMapData();
            }
        }
        auto result_is_nullable
            = !defaultImplForNull && (column_a_null_map_ptr != nullptr || column_b_null_map_ptr != nullptr);

        size_t rows = block.rows();
        auto result_vec = ColumnUInt8::create(rows);
        auto & result_vec_data = result_vec->getData();
        if (!result_is_nullable)
        {
            if (!executeVectorVectorNotNullLeft<Int8>(column_a.get(), column_b.get(), result_vec_data)
                && !executeVectorVectorNotNullLeft<Int16>(column_a.get(), column_b.get(), result_vec_data)
                && !executeVectorVectorNotNullLeft<Int32>(column_a.get(), column_b.get(), result_vec_data)
                && !executeVectorVectorNotNullLeft<Int64>(column_a.get(), column_b.get(), result_vec_data)
                && !executeVectorVectorNotNullLeft<UInt8>(column_a.get(), column_b.get(), result_vec_data)
                && !executeVectorVectorNotNullLeft<UInt16>(column_a.get(), column_b.get(), result_vec_data)
                && !executeVectorVectorNotNullLeft<UInt32>(column_a.get(), column_b.get(), result_vec_data)
                && !executeVectorVectorNotNullLeft<UInt64>(column_a.get(), column_b.get(), result_vec_data)
                && !executeVectorVectorNotNullLeft<Float32>(column_a.get(), column_b.get(), result_vec_data)
                && !executeVectorVectorNotNullLeft<Float64>(column_a.get(), column_b.get(), result_vec_data))
                throw Exception("Unexpected type of column: " + column_a->getName(), ErrorCodes::ILLEGAL_COLUMN);
            block.getByPosition(result).column = std::move(result_vec);
        }
        else 
        {
            auto result_null_map_vec = ColumnUInt8::create(rows);
            auto & result_null_map_vec_data = result_null_map_vec->getData();
            if (!executeVectorVectorNullableLeft<Int8>(column_a.get(), column_a_null_map_ptr, column_b.get(), column_b_null_map_ptr, result_vec_data, result_null_map_vec_data)
                && !executeVectorVectorNullableLeft<Int16>(column_a.get(), column_a_null_map_ptr, column_b.get(), column_b_null_map_ptr, result_vec_data, result_null_map_vec_data)
                && !executeVectorVectorNullableLeft<Int32>(column_a.get(), column_a_null_map_ptr, column_b.get(), column_b_null_map_ptr, result_vec_data, result_null_map_vec_data)
                && !executeVectorVectorNullableLeft<Int64>(column_a.get(), column_a_null_map_ptr, column_b.get(), column_b_null_map_ptr, result_vec_data, result_null_map_vec_data)
                && !executeVectorVectorNullableLeft<UInt8>(column_a.get(), column_a_null_map_ptr, column_b.get(), column_b_null_map_ptr, result_vec_data, result_null_map_vec_data)
                && !executeVectorVectorNullableLeft<UInt16>(column_a.get(), column_a_null_map_ptr, column_b.get(), column_b_null_map_ptr, result_vec_data, result_null_map_vec_data)
                && !executeVectorVectorNullableLeft<UInt32>(column_a.get(), column_a_null_map_ptr, column_b.get(), column_b_null_map_ptr, result_vec_data, result_null_map_vec_data)
                && !executeVectorVectorNullableLeft<UInt64>(column_a.get(), column_a_null_map_ptr, column_b.get(), column_b_null_map_ptr, result_vec_data, result_null_map_vec_data)
                && !executeVectorVectorNullableLeft<Float32>(column_a.get(), column_a_null_map_ptr, column_b.get(), column_b_null_map_ptr, result_vec_data, result_null_map_vec_data)
                && !executeVectorVectorNullableLeft<Float64>(column_a.get(), column_a_null_map_ptr, column_b.get(), column_b_null_map_ptr, result_vec_data, result_null_map_vec_data))
                throw Exception("Unexpected type of column: " + column_a->getName(), ErrorCodes::ILLEGAL_COLUMN);
            block.getByPosition(result).column
                = ColumnNullable::create(std::move(result_vec), std::move(result_null_map_vec));
        }
    }

public:
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto column_a = block.getByPosition(arguments[0]).column;
        auto column_b = block.getByPosition(arguments[1]).column;
        if (column_b->isColumnConst())
        {
            // make sure that only column_a can be constant column
            column_a = block.getByPosition(arguments[1]).column;
            column_b = block.getByPosition(arguments[0]).column;
        }

        if (column_a->isColumnConst())
        {
            // constantVector
            Field value;
            column_a->get(0, value);
            bool is_constant_null = value.isNull();
            bool constant_value = false;
            if (!is_constant_null)
                constant_value = applyVisitor(FieldVisitorConvertToNumber<bool>(), value);
            executeConstantVector(block, result, column_b, is_constant_null, constant_value);
        }
        else
        {
            if (column_b->isColumnNullable())
            {
                // If only one input column is nullable, make sure that column_a is nullable
                column_a = block.getByPosition(arguments[1]).column;
                column_b = block.getByPosition(arguments[0]).column;
            }
            // vectorVector
            executeVectorVector(block, result, column_a, column_b);
        }
    }
};


// clang-format off
struct NameBinaryAnd { static constexpr auto name = "binary_and"; };
struct NameBinaryOr { static constexpr auto name = "binary_or"; };
struct NameBinaryXor { static constexpr auto name = "binary_xor"; };
// clang-format on

using FunctionBinaryAnd = FunctionBinaryLogical<BinaryAndImpl, NameBinaryAnd, true>;
using FunctionBinaryOr = FunctionBinaryLogical<BinaryOrImpl, NameBinaryOr, true>;
using FunctionBinaryXor = FunctionBinaryLogical<BinaryXorImpl, NameBinaryXor, false>;

} // namespace DB
