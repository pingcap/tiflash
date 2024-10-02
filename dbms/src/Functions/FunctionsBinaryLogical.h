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
    static inline bool isSaturable() { return true; }

    static inline bool resNotNull(const Field & value)
    {
        return !value.isNull() && applyVisitor(FieldVisitorConvertToNumber<bool>(), value) == 0;
    }

    static inline bool resNotNull(UInt8 value, UInt8 is_null) { return !is_null && !value; }

    static inline void adjustForNullValue(UInt8 & value, UInt8 & is_null)
    {
        is_null = false;
        value = false;
    }

    static inline bool isSaturatedValue(bool a) { return !a; }

    static inline bool apply(bool a, bool b) { return a && b; }
};

struct BinaryOrImpl
{
    static inline bool isSaturable() { return true; }

    static inline bool isSaturatedValue(bool a) { return a; }

    static inline bool resNotNull(const Field & value)
    {
        return !value.isNull() && applyVisitor(FieldVisitorConvertToNumber<bool>(), value) == 1;
    }

    static inline bool resNotNull(UInt8 value, UInt8 is_null) { return !is_null && value; }

    static inline void adjustForNullValue(UInt8 & value, UInt8 & is_null)
    {
        is_null = false;
        value = true;
    }

    static inline bool apply(bool a, bool b) { return a || b; }
};

struct BinaryXorImpl
{
    static inline bool isSaturable() { return false; }

    static inline bool isSaturatedValue(bool) { return false; }

    static inline bool resNotNull(const Field &) { return true; }

    static inline bool resNotNull(UInt8, UInt8) { return true; }

    static inline void adjustForNullValue(UInt8 &, UInt8 &) {}

    static inline bool apply(bool a, bool b) { return a != b; }
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

private:
    bool extractConstColumns(ColumnRawPtrs & in, UInt8 & res, UInt8 & res_not_null, UInt8 & input_has_null) const
    {
        bool has_res = false;
        for (int i = static_cast<int>(in.size()) - 1; i >= 0; --i)
        {
            if (!in[i]->isColumnConst())
                continue;

            Field value = (*in[i])[0];
            if constexpr (special_impl_for_nulls)
            {
                input_has_null |= value.isNull();
                res_not_null |= Impl::resNotNull(value);
            }

            UInt8 x = !value.isNull() && applyVisitor(FieldVisitorConvertToNumber<bool>(), value);
            if (has_res)
            {
                res = Impl::apply(res, x);
            }
            else
            {
                res = x;
                has_res = true;
            }

            in.erase(in.begin() + i);
        }
        return has_res;
    }

    template <typename T>
    bool convertTypeToUInt8(const IColumn * column, UInt8Container & res, UInt8Container & res_not_null) const
    {
        auto col = checkAndGetColumn<ColumnVector<T>>(column);
        if (!col)
            return false;
        const auto & vec = col->getData();
        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
        {
            res[i] = !!vec[i];
            if constexpr (special_impl_for_nulls)
                res_not_null[i] |= Impl::resNotNull(res[i], false);
        }

        return true;
    }

    bool convertOnlyNullToUInt8(
        const IColumn * column,
        UInt8Container & res,
        UInt8Container & res_not_null,
        UInt8Container & input_has_null) const
    {
        if (!column->onlyNull())
            return false;

        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
        {
            res[i] = false;
            if constexpr (special_impl_for_nulls)
            {
                res_not_null[i] |= Impl::resNotNull(res[i], true);
                input_has_null[i] |= true;
            }
        }

        return true;
    }

    template <typename T>
    bool convertNullableTypeToUInt8(
        const IColumn * column,
        UInt8Container & res,
        UInt8Container & res_not_null,
        UInt8Container & input_has_null) const
    {
        auto col_nullable = checkAndGetColumn<ColumnNullable>(column);

        auto col = checkAndGetColumn<ColumnVector<T>>(&col_nullable->getNestedColumn());
        if (!col)
            return false;

        const auto & vec = col->getData();
        const auto & null_map = col_nullable->getNullMapData();

        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
        {
            res[i] = !!vec[i] && !null_map[i];
            if constexpr (special_impl_for_nulls)
            {
                res_not_null[i] |= Impl::resNotNull(res[i], null_map[i]);
                input_has_null[i] |= null_map[i];
            }
        }

        return true;
    }

    void convertToUInt8(
        const IColumn * column,
        UInt8Container & res,
        UInt8Container & res_not_null,
        UInt8Container & input_has_null) const
    {
        if (!convertTypeToUInt8<Int8>(column, res, res_not_null)
            && !convertTypeToUInt8<Int16>(column, res, res_not_null)
            && !convertTypeToUInt8<Int32>(column, res, res_not_null)
            && !convertTypeToUInt8<Int64>(column, res, res_not_null)
            && !convertTypeToUInt8<UInt16>(column, res, res_not_null)
            && !convertTypeToUInt8<UInt32>(column, res, res_not_null)
            && !convertTypeToUInt8<UInt64>(column, res, res_not_null)
            && !convertTypeToUInt8<Float32>(column, res, res_not_null)
            && !convertTypeToUInt8<Float64>(column, res, res_not_null)
            && !convertNullableTypeToUInt8<Int8>(column, res, res_not_null, input_has_null)
            && !convertNullableTypeToUInt8<Int16>(column, res, res_not_null, input_has_null)
            && !convertNullableTypeToUInt8<Int32>(column, res, res_not_null, input_has_null)
            && !convertNullableTypeToUInt8<Int64>(column, res, res_not_null, input_has_null)
            && !convertNullableTypeToUInt8<UInt8>(column, res, res_not_null, input_has_null)
            && !convertNullableTypeToUInt8<UInt16>(column, res, res_not_null, input_has_null)
            && !convertNullableTypeToUInt8<UInt32>(column, res, res_not_null, input_has_null)
            && !convertNullableTypeToUInt8<UInt64>(column, res, res_not_null, input_has_null)
            && !convertNullableTypeToUInt8<Float32>(column, res, res_not_null, input_has_null)
            && !convertNullableTypeToUInt8<Float64>(column, res, res_not_null, input_has_null)
            && !convertOnlyNullToUInt8(column, res, res_not_null, input_has_null))
            throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

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
        }
        else {}
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        bool has_nullable_input_column = false;
        if constexpr (!defaultImplForNull)
        {
            for (size_t i = 0; i < 2; ++i)
                has_nullable_input_column |= block.getByPosition(arguments[i]).type->isNullable();
        }

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

        size_t rows = block.rows();

        /// Combine all constant columns into a single value.
        UInt8 const_val = 0;
        UInt8 const_val_input_has_null = 0;
        UInt8 const_val_res_not_null = 0;
        bool has_consts = extractConstColumns(in, const_val, const_val_res_not_null, const_val_input_has_null);

        // If this value uniquely determines the result, return it.
        if (has_consts
            && (in.empty() || (!has_nullable_input_column && Impl::apply(const_val, 0) == Impl::apply(const_val, 1))))
        {
            if (!in.empty())
                const_val = Impl::apply(const_val, 0);
            if constexpr (!special_impl_for_nulls)
                block.getByPosition(result).column = DataTypeUInt8().createColumnConst(rows, toField(const_val));
            else
            {
                if (const_val_input_has_null && const_val_res_not_null)
                    Impl::adjustForNullValue(const_val, const_val_input_has_null);
                if (const_val_input_has_null)
                    block.getByPosition(result).column
                        = block.getByPosition(result).type->createColumnConst(rows, Null());
                else
                    block.getByPosition(result).column = has_nullable_input_column
                        ? makeNullable(DataTypeUInt8().createColumnConst(rows, toField(const_val)))
                        : DataTypeUInt8().createColumnConst(rows, toField(const_val));
            }
            return;
        }

        /// If this value is a neutral element, let's forget about it.
        if (!has_nullable_input_column && has_consts && Impl::apply(const_val, 0) == 0
            && Impl::apply(const_val, 1) == 1)
            has_consts = false;

        auto col_res = ColumnUInt8::create();
        UInt8Container & vec_res = col_res->getData();
        auto col_input_has_null = ColumnUInt8::create();
        UInt8Container & vec_input_has_null = col_input_has_null->getData();
        auto col_res_not_null = ColumnUInt8::create();
        UInt8Container & vec_res_not_null = col_res_not_null->getData();

        Int32 const_column_index = -1;
        if (has_consts)
        {
            vec_res.assign(rows, const_val);
            in.push_back(col_res.get());
            const_column_index = in.size() - 1;
            if constexpr (special_impl_for_nulls)
            {
                vec_input_has_null.assign(rows, const_val_input_has_null);
                vec_res_not_null.assign(rows, const_val_res_not_null);
            }
        }
        else
        {
            vec_res.resize(rows);
            if constexpr (special_impl_for_nulls)
            {
                vec_input_has_null.assign(rows, (UInt8)0);
                vec_res_not_null.assign(rows, (UInt8)0);
            }
        }

        /// Convert all columns to UInt8
        UInt8ColumnPtrs uint8_in;
        Columns converted_columns;

        for (size_t index = 0; index < in.size(); index++)
        {
            const IColumn * column = in[index];
            bool is_const_column [[maybe_unused]] = (Int32)index == const_column_index;
            if (auto uint8_column = checkAndGetColumn<ColumnUInt8>(column))
            {
                uint8_in.push_back(uint8_column);
                const auto & data = uint8_column->getData();
                if constexpr (special_impl_for_nulls)
                {
                    if (!is_const_column)
                    {
                        size_t n = uint8_column->size();
                        for (size_t i = 0; i < n; i++)
                            vec_res_not_null[i] |= Impl::resNotNull(data[i], false);
                    }
                }
            }
            else
            {
                auto converted_column = ColumnUInt8::create(rows);
                convertToUInt8(column, converted_column->getData(), vec_res_not_null, vec_input_has_null);
                uint8_in.push_back(converted_column.get());
                converted_columns.emplace_back(std::move(converted_column));
            }
        }

        /// Effeciently combine all the columns of the correct type.
        while (uint8_in.size() > 1)
        {
            /// With a large block size, combining 6 columns per pass is the fastest.
            /// When small - more, is faster.
            AssociativeOperationImpl<Impl, 10>::execute(uint8_in, vec_res);
            uint8_in.push_back(col_res.get());
        }

        /// This is possible if there is exactly one non-constant among the arguments, and it is of type UInt8.
        if (uint8_in[0] != col_res.get())
            vec_res.assign(uint8_in[0]->getData());

        if constexpr (!special_impl_for_nulls)
            block.getByPosition(result).column = std::move(col_res);
        else
        {
            if (has_nullable_input_column)
            {
                for (size_t i = 0; i < rows; i++)
                {
                    if (vec_input_has_null[i] && vec_res_not_null[i])
                        Impl::adjustForNullValue(vec_res[i], vec_input_has_null[i]);
                }
                block.getByPosition(result).column
                    = ColumnNullable::create(std::move(col_res), std::move(col_input_has_null));
            }
            else
                block.getByPosition(result).column = std::move(col_res);
        }
    }
};


template <template <typename> class Impl, typename Name>
class FunctionUnaryLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionUnaryLogical>(); }

private:
    template <typename T>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        if (auto col = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_res = ColumnUInt8::create();

            typename ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(col->getData().size());
            UnaryOperationImpl<T, Impl<T>>::vector(col->getData(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }

        return false;
    }

public:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isNumber())
            throw Exception(
                "Illegal type (" + arguments[0]->getName() + ") of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        if (!(executeType<UInt8>(block, arguments, result) || executeType<UInt16>(block, arguments, result)
              || executeType<UInt32>(block, arguments, result) || executeType<UInt64>(block, arguments, result)
              || executeType<Int8>(block, arguments, result) || executeType<Int16>(block, arguments, result)
              || executeType<Int32>(block, arguments, result) || executeType<Int64>(block, arguments, result)
              || executeType<Float32>(block, arguments, result) || executeType<Float64>(block, arguments, result)))
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

// clang-format off
struct NameBinaryAnd { static constexpr auto name = "and"; };
struct NameBinaryOr { static constexpr auto name = "or"; };
struct NameBinaryXor { static constexpr auto name = "xor"; };
// clang-format on

// using FunctionAnd = FunctionAnyArityLogical<AndImpl, NameAnd, true>;
// using FunctionOr = FunctionAnyArityLogical<OrImpl, NameOr, true>;
// using FunctionXor = FunctionAnyArityLogical<XorImpl, NameXor, false>;
// using FunctionNot = FunctionUnaryLogical<NotImpl, NameNot>;

} // namespace DB
