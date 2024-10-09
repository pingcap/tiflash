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

#pragma once

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_COLUMN;
} // namespace ErrorCodes

struct BinaryAndImpl
{
    static inline void evaluate(bool a, bool b, UInt8 & res) { res = a && b; }
    static inline void evaluateOneNullable(bool a, bool a_is_null, bool b, UInt8 & res, UInt8 & res_is_null)
    {
        // result is null if a is null and b is true
        res_is_null = a_is_null && b;
        // when a_is_null == true, a could be either true or false, but we don't
        // need to consider this because if result_is_null = true, res is
        // meaningless if result_is_null = false, then b must be false, then a && b
        // is always false
        res = a && b;
    }
    static inline void evaluateTwoNullable(
        bool a,
        bool a_is_null,
        bool b,
        bool b_is_null,
        UInt8 & res,
        UInt8 & res_is_null)
    {
        // result is null if
        // 1. both a and b is null
        // 2. a is null and b is true
        // 3. b is null and a is true
        res_is_null = (a_is_null && b_is_null) || (a_is_null && b) || (b_is_null && a);
        // when a_is_null/b_is_null == true, a/b could be either true or false, but
        // we don't need to consider this because if result_is_null = true, res is
        // meaningless if result_is_null = false, then b/a must be false, then a &&
        // b is always false
        res = a && b;
    }
};

struct BinaryOrImpl
{
    static inline void evaluate(bool a, bool b, UInt8 & res) { res = a || b; }
    static inline void evaluateOneNullable(bool a, bool a_is_null, bool b, UInt8 & res, UInt8 & res_is_null)
    {
        // result is null if a is null and b is false
        res_is_null = a_is_null && !b;
        // when a_is_null == true, a could be either true or false, but we don't
        // need to consider this because if result_is_null = true, res is
        // meaningless if result_is_null = false, then b must be true, then a && b
        // is always true
        res = a || b;
    }
    static inline void evaluateTwoNullable(
        bool a,
        bool a_is_null,
        bool b,
        bool b_is_null,
        UInt8 & res,
        UInt8 & res_is_null)
    {
        // result is null if
        // 1. both a and b is null
        // 2. a is null and b is false
        // 3. b is null and a is false
        res_is_null = (a_is_null && b_is_null) || (a_is_null && !b) || (b_is_null && !a);
        // when a_is_null/b_is_null == true, a/b could be either true or false, but
        // we don't need to consider this because if result_is_null = true, res is
        // meaningless if result_is_null = false, then b/a must be true, then a && b
        // is always true
        res = a || b;
    }
};

struct BinaryXorImpl
{
    static inline void evaluate(bool a, bool b, UInt8 & res) { res = a != b; }
    static inline void evaluateOneNullable(bool a, bool a_is_null, bool b, UInt8 & res, UInt8 & res_is_null)
    {
        res_is_null = a_is_null;
        res = a != b;
    }
    static inline void evaluateTwoNullable(
        bool a,
        bool a_is_null,
        bool b,
        bool b_is_null,
        UInt8 & res,
        UInt8 & res_is_null)
    {
        res_is_null = a_is_null || b_is_null;
        res = a != b;
    }
};

using UInt8Container = ColumnUInt8::Container;

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

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return defaultImplForNull; }

    /// Get result types by argument types. If the function does not apply to
    /// these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        bool has_nullable_input_column = false;
        for (size_t i = 0; i < arguments.size(); ++i)
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
    bool extractConstColumns(ColumnRawPtrs & in, UInt8 & res, UInt8 & res_is_null) const
    {
        bool has_res = false;
        for (int i = static_cast<int>(in.size()) - 1; i >= 0; --i)
        {
            if (!in[i]->isColumnConst())
                continue;

            Field value = (*in[i])[0];
            UInt8 const_is_null = false;
            UInt8 const_value = false;
            if constexpr (!defaultImplForNull)
            {
                const_is_null = value.isNull();
                if (!const_is_null)
                    const_value = applyVisitor(FieldVisitorConvertToNumber<bool>(), value);
            }
            else
            {
                const_value = applyVisitor(FieldVisitorConvertToNumber<bool>(), value);
            }
            if (has_res)
            {
                if constexpr (!defaultImplForNull)
                {
                    Impl::evaluateTwoNullable(res, res_is_null, const_value, const_is_null, res, res_is_null);
                }
                else
                {
                    Impl::evaluate(res, const_value, res);
                }
            }
            else
            {
                res = const_value;
                res_is_null = const_is_null;
                has_res = true;
            }
            in.erase(in.begin() + i);
        }
        return has_res;
    }

    template <typename T>
    bool executeConstantVectorNullableImpl(
        const IColumn * column,
        const ConstNullMapPtr column_null_map,
        bool constant_value,
        bool is_constant_null,
        UInt8Container & res,
        UInt8Container & result_null_map) const
    {
        auto col = checkAndGetColumn<ColumnVector<T>>(column);
        if (!col)
            return false;
        const auto & vec = col->getData();
        size_t n = res.size();
        if (column_null_map == nullptr)
        {
            if (is_constant_null)
            {
                for (size_t i = 0; i < n; i++)
                    Impl::evaluateOneNullable(constant_value, true, vec[i], res[i], result_null_map[i]);
            }
            else
            {
                for (size_t i = 0; i < n; ++i)
                {
                    Impl::evaluate(vec[i], constant_value, res[i]);
                }
            }
        }
        else
        {
            if (is_constant_null)
            {
                for (size_t i = 0; i < n; ++i)
                {
                    Impl::evaluateTwoNullable(
                        vec[i],
                        (*column_null_map)[i],
                        constant_value,
                        is_constant_null,
                        res[i],
                        result_null_map[i]);
                }
            }
            else
            {
                for (size_t i = 0; i < n; ++i)
                {
                    Impl::evaluateOneNullable(
                        vec[i],
                        (*column_null_map)[i],
                        constant_value,
                        res[i],
                        result_null_map[i]);
                }
            }
        }
        return true;
    }

    template <typename T>
    bool executeConstantVectorNotNullImpl(const IColumn * column, bool constant_value, UInt8Container & res) const
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
        return true;
    }

    void executeConstantVectorNullable(
        bool is_const_null,
        bool const_value,
        const IColumn * column,
        UInt8Container & res,
        UInt8Container & res_is_null) const
    {
        const IColumn * c = column;
        ConstNullMapPtr null_map = nullptr;
        // defaultImplForNull == false means the input column is always not-nullable
        if (column->isColumnNullable())
        {
            const auto & col_nullable = checkAndGetColumn<ColumnNullable>(column);
            c = &col_nullable->getNestedColumn();
            null_map = &col_nullable->getNullMapData();
        }

        if (!executeConstantVectorNullableImpl<Int8>(c, null_map, const_value, is_const_null, res, res_is_null)
            && !executeConstantVectorNullableImpl<Int16>(c, null_map, const_value, is_const_null, res, res_is_null)
            && !executeConstantVectorNullableImpl<Int32>(c, null_map, const_value, is_const_null, res, res_is_null)
            && !executeConstantVectorNullableImpl<Int64>(c, null_map, const_value, is_const_null, res, res_is_null)
            && !executeConstantVectorNullableImpl<UInt8>(c, null_map, const_value, is_const_null, res, res_is_null)
            && !executeConstantVectorNullableImpl<UInt16>(c, null_map, const_value, is_const_null, res, res_is_null)
            && !executeConstantVectorNullableImpl<UInt32>(c, null_map, const_value, is_const_null, res, res_is_null)
            && !executeConstantVectorNullableImpl<UInt64>(c, null_map, const_value, is_const_null, res, res_is_null)
            && !executeConstantVectorNullableImpl<Float32>(c, null_map, const_value, is_const_null, res, res_is_null)
            && !executeConstantVectorNullableImpl<Float64>(c, null_map, const_value, is_const_null, res, res_is_null))
            throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    void executeConstantVectorNotNull(bool constant_value, const IColumn * column, UInt8Container & res) const
    {
        if (!executeConstantVectorNotNullImpl<Int8>(column, constant_value, res)
            && !executeConstantVectorNotNullImpl<Int16>(column, constant_value, res)
            && !executeConstantVectorNotNullImpl<Int32>(column, constant_value, res)
            && !executeConstantVectorNotNullImpl<Int64>(column, constant_value, res)
            && !executeConstantVectorNotNullImpl<UInt8>(column, constant_value, res)
            && !executeConstantVectorNotNullImpl<UInt16>(column, constant_value, res)
            && !executeConstantVectorNotNullImpl<UInt32>(column, constant_value, res)
            && !executeConstantVectorNotNullImpl<UInt64>(column, constant_value, res)
            && !executeConstantVectorNotNullImpl<Float32>(column, constant_value, res)
            && !executeConstantVectorNotNullImpl<Float64>(column, constant_value, res))
            throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename LeftType, typename RightType>
    bool executeVectorVectorNotNullRight(
        const ColumnVector<LeftType> * column_a,
        const IColumn * column_b,
        UInt8Container & res) const
    {
        auto col = checkAndGetColumn<ColumnVector<RightType>>(column_b);
        if (!col)
            return false;
        auto & data_a = column_a->getData();
        auto & data_b = col->getData();
        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
            Impl::evaluate(data_a[i], data_b[i], res[i]);
        return true;
    }

    template <typename LeftType, typename RightType>
    bool executeVectorVectorNullableRight(
        const ColumnVector<LeftType> * a,
        ConstNullMapPtr a_null_map,
        const IColumn * b,
        ConstNullMapPtr b_null_map,
        UInt8Container & res,
        UInt8Container & res_is_null) const
    {
        auto col = checkAndGetColumn<ColumnVector<RightType>>(b);
        if (!col)
            return false;
        auto & data_a = a->getData();
        auto & data_b = col->getData();
        size_t n = res.size();
        if (a_null_map == nullptr)
        {
            // column_b_null_map must be not null
            for (size_t i = 0; i < n; ++i)
                Impl::evaluateOneNullable(data_b[i], (*b_null_map)[i], data_a[i], res[i], res_is_null[i]);
        }
        else if (b_null_map == nullptr)
        {
            // column_a_null_map must be not null
            for (size_t i = 0; i < n; ++i)
                Impl::evaluateOneNullable(data_a[i], (*a_null_map)[i], data_b[i], res[i], res_is_null[i]);
        }
        else
        {
            for (size_t i = 0; i < n; ++i)
                Impl::evaluateTwoNullable(
                    data_a[i],
                    (*a_null_map)[i],
                    data_b[i],
                    (*b_null_map)[i],
                    res[i],
                    res_is_null[i]);
        }
        return true;
    }

    template <typename T>
    bool executeVectorVectorNotNullLeft(const IColumn * column_a, const IColumn * column_b, UInt8Container & res) const
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
        return true;
    }

    template <typename T>
    bool executeVectorVectorNullableLeft(
        const IColumn * a,
        ConstNullMapPtr a_null_map,
        const IColumn * b,
        ConstNullMapPtr b_null_map,
        UInt8Container & res,
        UInt8Container & res_is_null) const
    {
        auto col = checkAndGetColumn<ColumnVector<T>>(a);
        if (!col)
            return false;
        if (!executeVectorVectorNullableRight<T, Int8>(col, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableRight<T, Int16>(col, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableRight<T, Int32>(col, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableRight<T, Int64>(col, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableRight<T, UInt8>(col, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableRight<T, UInt16>(col, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableRight<T, UInt32>(col, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableRight<T, UInt64>(col, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableRight<T, Float32>(col, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableRight<T, Float64>(col, a_null_map, b, b_null_map, res, res_is_null))
            throw Exception("Unexpected type of column: " + b->getName(), ErrorCodes::ILLEGAL_COLUMN);
        return true;
    }

    void executeVectorVectorNullable(
        const IColumn * column_a,
        const IColumn * column_b,
        UInt8Container & res,
        UInt8Container & res_is_null) const
    {
        const IColumn * a = column_a;
        ConstNullMapPtr a_null_map = nullptr;
        const IColumn * b = column_b;
        ConstNullMapPtr b_null_map = nullptr;
        // defaultImplForNull == false means the input column is always not-nullable
        if (column_a->isColumnNullable())
        {
            const auto & col_nullable = checkAndGetColumn<ColumnNullable>(column_a);
            a = &col_nullable->getNestedColumn();
            a_null_map = &col_nullable->getNullMapData();
        }
        if (column_b->isColumnNullable())
        {
            const auto & col_nullable = checkAndGetColumn<ColumnNullable>(column_b);
            b = &col_nullable->getNestedColumn();
            b_null_map = &col_nullable->getNullMapData();
        }

        if (!executeVectorVectorNullableLeft<Int8>(a, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableLeft<Int16>(a, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableLeft<Int32>(a, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableLeft<Int64>(a, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableLeft<UInt8>(a, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableLeft<UInt16>(a, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableLeft<UInt32>(a, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableLeft<UInt64>(a, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableLeft<Float32>(a, a_null_map, b, b_null_map, res, res_is_null)
            && !executeVectorVectorNullableLeft<Float64>(a, a_null_map, b, b_null_map, res, res_is_null))
            throw Exception("Unexpected type of column: " + column_a->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    void executeVectorVectorNotNull(const IColumn * column_a, const IColumn * column_b, UInt8Container & res) const
    {
        if (!executeVectorVectorNotNullLeft<Int8>(column_a, column_b, res)
            && !executeVectorVectorNotNullLeft<Int16>(column_a, column_b, res)
            && !executeVectorVectorNotNullLeft<Int32>(column_a, column_b, res)
            && !executeVectorVectorNotNullLeft<Int64>(column_a, column_b, res)
            && !executeVectorVectorNotNullLeft<UInt8>(column_a, column_b, res)
            && !executeVectorVectorNotNullLeft<UInt16>(column_a, column_b, res)
            && !executeVectorVectorNotNullLeft<UInt32>(column_a, column_b, res)
            && !executeVectorVectorNotNullLeft<UInt64>(column_a, column_b, res)
            && !executeVectorVectorNotNullLeft<Float32>(column_a, column_b, res)
            && !executeVectorVectorNotNullLeft<Float64>(column_a, column_b, res))
            throw Exception("Unexpected type of column: " + column_a->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

public:
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        bool has_nullable_input_column = false;
        size_t num_arguments = arguments.size();
        if constexpr (!defaultImplForNull)
        {
            for (size_t i = 0; i < num_arguments; ++i)
                has_nullable_input_column |= block.getByPosition(arguments[i]).type->isNullable();
        }
        ColumnRawPtrs in(num_arguments);
        for (size_t i = 0; i < num_arguments; ++i)
        {
            const auto & col = block.getByPosition(arguments[i]);
            in[i] = col.column.get();
            if constexpr (!defaultImplForNull)
                has_nullable_input_column |= col.type->isNullable();
        }

        size_t rows = in[0]->size();
        UInt8 const_val = 0;
        UInt8 const_val_is_null = false;
        bool has_consts = extractConstColumns(in, const_val, const_val_is_null);

        // If this value uniquely determines the result, return it.
        if (has_consts)
        {
            bool result_is_constant = false;
            UInt8 const_res = 0, const_res_is_null = false;
            if (in.empty())
            {
                // all the input is constant
                result_is_constant = true;
                const_res = const_val;
                const_res_is_null = const_val_is_null;
            }
            else
            {
                if (has_nullable_input_column)
                {
                    // test for true
                    Impl::evaluateOneNullable(const_val, const_val_is_null, 1, const_res, const_res_is_null);
                    // test for false
                    UInt8 res, res_is_null;
                    Impl::evaluateOneNullable(const_val, const_val_is_null, 0, res, res_is_null);
                    if (const_res == res && const_res_is_null == res_is_null)
                    {
                        // test for null
                        Impl::evaluateTwoNullable(const_val, const_val_is_null, 0, true, res, res_is_null);
                        if (const_res == res && const_res_is_null == res_is_null)
                            result_is_constant = true;
                    }
                }
                else
                {
                    assert(!const_val_is_null);
                    // test for true
                    Impl::evaluate(const_val, 1, const_res);
                    // test for false
                    UInt8 res;
                    Impl::evaluate(const_val, 0, res);
                    if (const_res == res)
                        result_is_constant = true;
                }
            }
            if (result_is_constant)
            {
                // if result is constant, just return constant
                if (has_nullable_input_column)
                {
                    // return type is Nullable(UInt8)
                    if (const_res_is_null)
                        block.getByPosition(result).column
                            = block.getByPosition(result).type->createColumnConst(rows, Null());
                    else
                        block.getByPosition(result).column
                            = makeNullable(DataTypeUInt8().createColumnConst(rows, toField(const_res)));
                }
                else
                {
                    // return type is UInt8
                    block.getByPosition(result).column = DataTypeUInt8().createColumnConst(rows, toField(const_res));
                }
                return;
            }
        }

        auto result_vec = ColumnUInt8::create(rows);
        auto & result_vec_data = result_vec->getData();
        size_t next_col_index;

        if (has_nullable_input_column)
        {
            // result is nullable
            // set not null by default
            auto result_null_map_vec = ColumnUInt8::create(rows, static_cast<UInt8>(0));
            auto & result_null_map_vec_data = result_null_map_vec->getData();
            if (has_consts)
            {
                assert(!in.empty());
                executeConstantVectorNullable(
                    const_val_is_null,
                    const_val,
                    in[0],
                    result_vec_data,
                    result_null_map_vec_data);
                next_col_index = 1;
            }
            else
            {
                assert(in.size() >= 2);
                executeVectorVectorNullable(in[0], in[1], result_vec_data, result_null_map_vec_data);
                next_col_index = 2;
            }
            for (; next_col_index < in.size(); ++next_col_index)
            {
                const IColumn * not_null_column = in[next_col_index];
                ConstNullMapPtr column_null_map_ptr = nullptr;
                if (in[next_col_index]->isColumnNullable())
                {
                    const auto & col_nullable = checkAndGetColumn<ColumnNullable>(in[next_col_index]);
                    not_null_column = &col_nullable->getNestedColumn();
                    column_null_map_ptr = &col_nullable->getNullMapData();
                }
                auto res = executeVectorVectorNullableLeft<UInt8>(
                    result_vec.get(),
                    &result_null_map_vec_data,
                    not_null_column,
                    column_null_map_ptr,
                    result_vec_data,
                    result_null_map_vec_data);
                RUNTIME_CHECK_MSG(res, "The left column type must be UInt8");
            }
            block.getByPosition(result).column
                = ColumnNullable::create(std::move(result_vec), std::move(result_null_map_vec));
        }
        else
        {
            // result is not nullable
            assert(!const_val_is_null);
            if (has_consts)
            {
                // handle const value first
                assert(!in.empty());
                executeConstantVectorNotNull(const_val, in[0], result_vec_data);
                next_col_index = 1;
            }
            else
            {
                assert(in.size() >= 2);
                executeVectorVectorNotNull(in[0], in[1], result_vec_data);
                next_col_index = 2;
            }
            for (; next_col_index < in.size(); ++next_col_index)
            {
                // the left side is UInt8
                auto res = executeVectorVectorNotNullLeft<UInt8>(result_vec.get(), in[next_col_index], result_vec_data);
                RUNTIME_CHECK_MSG(res, "The left column type must be UInt8");
            }
            block.getByPosition(result).column = std::move(result_vec);
        }
    }
};

// clang-format off
struct NameBinaryAnd { static constexpr auto name = "binary_and"; };
struct NameBinaryOr { static constexpr auto name = "binary_or"; };
struct NameBinaryXor { static constexpr auto name = "binary_xor"; };
// clang-format on

using FunctionBinaryAnd = FunctionBinaryLogical<BinaryAndImpl, NameBinaryAnd, false>;
using FunctionBinaryOr = FunctionBinaryLogical<BinaryOrImpl, NameBinaryOr, false>;
using FunctionBinaryXor = FunctionBinaryLogical<BinaryXorImpl, NameBinaryXor, true>;

} // namespace DB
