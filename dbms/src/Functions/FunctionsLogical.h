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
#include <vector>

#include "Columns/ColumnNothing.h"
#include "Common/Exception.h"
#include "Common/FieldVisitors.h"
#include "Core/Field.h"
#include "Core/Types.h"


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


struct AndImpl
{
    static constexpr bool isSaturable = true;

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
    static inline bool isSaturatedValue(bool a, bool is_null) { return !is_null && !a; }

    static inline void apply(bool a, bool b, UInt8 & res) { res = a && b; }
    static inline void applyNullable(bool a, bool a_is_null, bool b, bool b_is_null, UInt8 & res, UInt8 & res_is_null)
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

struct OrImpl
{
    static constexpr bool isSaturable = true;

    static inline bool isSaturatedValue(bool a) { return a; }
    static inline bool isSaturatedValue(bool a, bool is_null) { return !is_null && a; }

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

    static inline void apply(bool a, bool b, UInt8 & res) { res = a || b; }
    static inline void applyNullable(bool a, bool a_is_null, bool b, bool b_is_null, UInt8 & res, UInt8 & res_is_null)
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

struct XorImpl
{
    static constexpr bool isSaturable = false;

    static inline bool isSaturatedValue(bool) { return false; }
    static inline bool isSaturatedValue(bool, bool) { return false; }

    static inline bool resNotNull(const Field &) { return true; }

    static inline bool resNotNull(UInt8, UInt8) { return true; }

    static inline void adjustForNullValue(UInt8 &, UInt8 &) {}

    static inline void apply(bool a, bool b, UInt8 & res) { res = a != b; }
    static inline void applyNullable(bool a, bool a_is_null, bool b, bool b_is_null, UInt8 & res, UInt8 & res_is_null)
    {
        res_is_null = a_is_null || b_is_null;
        res = a != b;
    }
};

template <typename A>
struct NotImpl
{
    using ResultType = UInt8;

    static inline bool apply(A a) { return !a; }
};


using UInt8Container = ColumnUInt8::Container;
using UInt8ColumnPtrs = std::vector<const ColumnUInt8 *>;


template <typename Op, size_t N>
struct AssociativeOperationImpl
{
    /// Erases the N last columns from `in` (if there are less, then all) and puts into `result` their combination.
    static void NO_INLINE execute(UInt8ColumnPtrs & in, UInt8Container & result)
    {
        if (N > in.size())
        {
            AssociativeOperationImpl<Op, N - 1>::execute(in, result);
            return;
        }

        AssociativeOperationImpl<Op, N> operation(in);
        in.erase(in.end() - N, in.end());

        size_t n = result.size();
        for (size_t i = 0; i < n; ++i)
        {
            operation.apply(i, result[i]);
        }
    }

    const UInt8Container & vec;
    const UInt8Container * null_map = nullptr;
    AssociativeOperationImpl<Op, N - 1> continuation;

    /// Remembers the last N columns from `in`.
    explicit AssociativeOperationImpl(UInt8ColumnPtrs & in)
        : vec(in[in.size() - N]->getData())
        , continuation(in)
    {}

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline void apply(size_t i, UInt8 & res) const
    {
        if constexpr (Op::isSaturable)
        {
            // cast a: UInt8 -> bool -> UInt8 is a trick
            // TiFlash converts columns with non-UInt8 type to UInt8 type and sets value to 0 or 1
            // which correspond to false or true. However, for columns with UInt8 type,
            // no more convertion will be executed on them and the values stored
            // in them are 'origin' which means that they won't be converted to 0 or 1.
            // For example:
            //   Input column with non-UInt8 type:
            //      column_values = {-2, 0, 2}
            //   then, they will be converted to:
            //      vec = {1, 0, 1} (here vec stores converted values)
            //
            //   Input column with UInt8 type:
            //      column_values = {1, 0, 2}
            //   then, the vec will be:
            //      vec = {1, 0, 2} (error, we only want 0 or 1)
            // See issue: https://github.com/pingcap/tidb/issues/37258
            bool a = static_cast<bool>(vec[i]);
            if (Op::isSaturatedValue(a))
                res = a;
            else
            {
                continuation.apply(i, res);
            }
        }
        else
        {
            UInt8 tmp = false;
            continuation.apply(i, tmp);
            Op::apply(vec[i], tmp, res);
        }
    }
};

template <typename Op>
struct AssociativeOperationImpl<Op, 2>
{
    static void execute(UInt8ColumnPtrs & in, UInt8Container & res)
    {
        if (in.size() <= 1)
            throw Exception("should not reach here");
        AssociativeOperationImpl<Op, 2> operation(in);
        in.erase(in.end() - 2, in.end());

        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
        {
            operation.apply(i, res[i]);
        }
    }

    const UInt8Container & a;
    const UInt8Container & b;

    explicit AssociativeOperationImpl(UInt8ColumnPtrs & in)
        : a(in[in.size() - 2]->getData())
        , b(in[in.size() - 1]->getData())
    {}

    inline void apply(size_t i, UInt8 & res) const 
    { 
        Op::apply(a[i], b[i], res);
    }
};

template <typename Op>
struct AssociativeOperationImpl<Op, 1>
{
    static void execute(UInt8ColumnPtrs &, UInt8Container &)
    {
        throw Exception("Logical error: AssociativeOperationImpl<Op, 1>::execute called", ErrorCodes::LOGICAL_ERROR);
    }

    explicit AssociativeOperationImpl(UInt8ColumnPtrs &)
    {
        throw Exception("Logical error: should not reach here");
    }

    inline void apply(size_t , UInt8 & ) const 
    { 
        throw Exception("Logical error: should not reach here");
    }
};

template <typename Op, size_t N>
struct NullableAssociativeOperationImpl
{
    /// Erases the N last columns from `in` (if there are less, then all) and puts into `result` their combination.
    static void NO_INLINE execute(
        UInt8ColumnPtrs & in,
        std::vector<const UInt8Container *> null_map_in,
        UInt8Container & result,
        UInt8Container & result_is_null)
    {
        if (N > in.size())
        {
            NullableAssociativeOperationImpl<Op, N - 1>::execute(in, null_map_in, result, result_is_null);
            return;
        }

        NullableAssociativeOperationImpl<Op, N> operation(in, null_map_in);
        in.erase(in.end() - N, in.end());
        null_map_in.erase(null_map_in.end() - N, null_map_in.end());

        size_t n = result.size();
        for (size_t i = 0; i < n; ++i)
        {
            operation.apply(i, result[i], result_is_null[i]);
        }
    }

    const UInt8Container & vec;
    const UInt8Container * null_map;
    NullableAssociativeOperationImpl<Op, N - 1> continuation;

    /// Remembers the last N columns from `in`.
    NullableAssociativeOperationImpl(UInt8ColumnPtrs & in, std::vector<const UInt8Container *> null_map_in)
        : vec(in[in.size() - N]->getData())
        , null_map(null_map_in[null_map_in.size() - N])
        , continuation(in, null_map_in)
    {}

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline void apply(size_t i, UInt8 & res, UInt8 & res_is_null) const
    {
        bool a = static_cast<bool>(vec[i]);
        bool is_null = (*null_map)[i];
        //if constexpr (Op::isSaturable)
        //{
        // cast a: UInt8 -> bool -> UInt8 is a trick
        // TiFlash converts columns with non-UInt8 type to UInt8 type and sets value to 0 or 1
        // which correspond to false or true. However, for columns with UInt8 type,
        // no more convertion will be executed on them and the values stored
        // in them are 'origin' which means that they won't be converted to 0 or 1.
        // For example:
        //   Input column with non-UInt8 type:
        //      column_values = {-2, 0, 2}
        //   then, they will be converted to:
        //      vec = {1, 0, 1} (here vec stores converted values)
        //
        //   Input column with UInt8 type:
        //      column_values = {1, 0, 2}
        //   then, the vec will be:
        //      vec = {1, 0, 2} (error, we only want 0 or 1)
        // See issue: https://github.com/pingcap/tidb/issues/37258
        //    if (Op::isSaturatedValue(a, is_null))
        //    {
        //        res = a;
        //        res_is_null = false;
        //    }
        //    else
        //    {
        //        UInt8 tmp, tmp_is_null;
        //        continuation.apply(i, tmp, tmp_is_null);
        //        Op::applyNullable(a, is_null, tmp, tmp_is_null, res, res_is_null);
        //    }
        //}
        //else
        //{
        UInt8 tmp, tmp_is_null;
        continuation.apply(i, tmp, tmp_is_null);
        Op::applyNullable(a, is_null, tmp, tmp_is_null, res, res_is_null);
        //}
    }
};

template <typename Op>
struct NullableAssociativeOperationImpl<Op, 2>
{
    static void execute(UInt8ColumnPtrs & in, std::vector<const UInt8Container *> & null_map_in, UInt8Container & res, UInt8Container & res_is_null)
    {
        if (in.size() <= 1)
            throw Exception("Logical error: should not reach here");
        NullableAssociativeOperationImpl<Op, 2> operation(in, null_map_in);
        in.erase(in.end() - 2, in.end());
        null_map_in.erase(null_map_in.end() - 2, null_map_in.end());

        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
        {
            operation.apply(i, res[i], res_is_null[i]);
        }
    }

    const UInt8Container & a;
    const UInt8Container * a_null_map;
    const UInt8Container & b;
    const UInt8Container * b_null_map;

    NullableAssociativeOperationImpl(UInt8ColumnPtrs & in, std::vector<const UInt8Container *> & null_map_in)
        : a(in[in.size() - 2]->getData())
        , a_null_map(null_map_in[null_map_in.size() - 2])
        , b(in[in.size() - 1]->getData())
        , b_null_map(null_map_in[null_map_in.size() - 1])
    {}

    inline void apply(size_t i, UInt8 & res, UInt8 & res_is_null) const
    {
        Op::applyNullable(a[i],(*a_null_map)[i], b[i], (*b_null_map)[i], res, res_is_null);
    }
};

template <typename Op>
struct NullableAssociativeOperationImpl<Op, 1>
{
    static void execute(UInt8ColumnPtrs &, std::vector<const UInt8Container *> &, UInt8Container &, UInt8Container &)
    {
        throw Exception("Logical error: AssociativeOperationImpl<Op, 1>::execute called", ErrorCodes::LOGICAL_ERROR);
    }

    NullableAssociativeOperationImpl(UInt8ColumnPtrs & , std::vector<const UInt8Container *> & )
    {
        throw Exception("Logical error: should not reach here");
    }

    inline void apply(size_t , UInt8 & , UInt8 & ) const
    {
        throw Exception("Logical error: should not reach here");
    }
};

/**
 * The behavior of and and or is the same as
 * https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
 */
template <typename Impl, typename Name, bool special_impl_for_nulls>
class FunctionAnyArityLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionAnyArityLogical>(); }

private:
    bool extractConstColumns(ColumnRawPtrs & in, UInt8 & res, UInt8 & res_is_null) const
    {
        bool has_res = false;
        for (int i = static_cast<int>(in.size()) - 1; i >= 0; --i)
        {
            // special case, if column->onlyNull() returns true, treated as constant column
            if (!in[i]->isColumnConst() && !in[i]->onlyNull())
                continue;

            UInt8 const_val = false;
            UInt8 const_val_is_null = false;
            if (in[i]->onlyNull())
            {
                // special case
                const_val_is_null = true;
            }
            else
            {
                Field value = (*in[i])[0];
                if constexpr (special_impl_for_nulls)
                {
                    const_val_is_null = value.isNull();
                    if (!const_val_is_null)
                        const_val = applyVisitor(FieldVisitorConvertToNumber<bool>(), value);
                }
                else
                {
                    const_val = applyVisitor(FieldVisitorConvertToNumber<bool>(), value);
                }
            }

            if (has_res)
            {
                if constexpr (special_impl_for_nulls)
                {
                    Impl::applyNullable(res, res_is_null, const_val, const_val_is_null, res, res_is_null);
                }
                else
                {
                    Impl::apply(res, const_val, res);
                }
            }
            else
            {
                res = const_val;
                res_is_null = const_val_is_null;
                has_res = true;
            }
            in.erase(in.begin() + i);
        }
        return has_res;
    }

    template <typename T>
    bool convertTypeToUInt8(const IColumn * column, UInt8Container & res) const
    {
        const auto * col = checkAndGetColumn<ColumnVector<T>>(column);
        if (!col)
            return false;
        const auto & vec = col->getData();
        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
            res[i] = !!vec[i];
        return true;
    }

    bool convertColumnNothingToUInt8(const IColumn * column, UInt8Container & res) const
    {
        const auto * col = checkAndGetColumn<ColumnNothing>(column);
        if (!col)
            return false;

        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
            res[i] = false;
        return true;
    }

    template <typename T>
    bool convertNullableTypeToUInt8(
        const IColumn * column,
        UInt8Container & res,
        UInt8Container & res_not_null,
        UInt8Container & input_has_null) const
    {
        const auto * col_nullable = checkAndGetColumn<ColumnNullable>(column);

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

    void convertToUInt8(const IColumn * column, UInt8Container & res) const
    {
        if (!convertTypeToUInt8<Int8>(column, res) && !convertTypeToUInt8<Int16>(column, res)
            && !convertTypeToUInt8<Int32>(column, res) && !convertTypeToUInt8<Int64>(column, res)
            && !convertTypeToUInt8<UInt16>(column, res) && !convertTypeToUInt8<UInt32>(column, res)
            && !convertTypeToUInt8<UInt64>(column, res) && !convertTypeToUInt8<Float32>(column, res)
            && !convertTypeToUInt8<Float64>(column, res))
            throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

public:
    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForNulls() const override { return !special_impl_for_nulls; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        bool has_nullable_input_column = false;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            has_nullable_input_column |= arguments[i]->isNullable();
            if (!(arguments[i]->isNumber()
                  || (special_impl_for_nulls
                      && (arguments[i]->onlyNull() || removeNullable(arguments[i])->isNumber()))))
                throw Exception(
                    "Illegal type (" + arguments[i]->getName() + ") of " + toString(i + 1) + " argument of function "
                        + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (has_nullable_input_column)
            return makeNullable(std::make_shared<DataTypeUInt8>());
        else
            return std::make_shared<DataTypeUInt8>();
    }

    bool handleConstantInput(
        Block & block,
        size_t result,
        size_t rows,
        UInt8 const_val,
        UInt8 const_val_is_null,
        bool has_other_column,
        bool has_nullable_input_column) const
    {
        bool result_is_constant = false;
        UInt8 const_res = 0, const_res_is_null = false;
        if (!has_other_column)
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
                Impl::applyNullable(const_val, const_val_is_null, true, false, const_res, const_res_is_null);
                // test for false
                UInt8 res, res_is_null;
                Impl::applyNullable(const_val, const_val_is_null, false, false, res, res_is_null);
                if (const_res == res && const_res_is_null == res_is_null)
                {
                    // test for null
                    Impl::applyNullable(const_val, const_val_is_null, false, true, res, res_is_null);
                    if (const_res == res && const_res_is_null == res_is_null)
                        result_is_constant = true;
                }
            }
            else
            {
                assert(!const_val_is_null);
                // test for true
                Impl::apply(const_val, 1, const_res);
                // test for false
                UInt8 res;
                Impl::apply(const_val, 0, res);
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
        }
        return result_is_constant;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        bool has_nullable_input_column = false;
        size_t num_arguments = arguments.size();

        for (size_t i = 0; i < num_arguments; ++i)
            has_nullable_input_column |= block.getByPosition(arguments[i]).type->isNullable();

        ColumnRawPtrs in(num_arguments);
        for (size_t i = 0; i < num_arguments; ++i)
            in[i] = block.getByPosition(arguments[i]).column.get();

        size_t rows = in[0]->size();

        /// Combine all constant columns into a single value.
        UInt8 const_val = 0;
        UInt8 const_val_is_null = 0;
        bool has_consts = extractConstColumns(in, const_val, const_val_is_null);
        // If this value uniquely determines the result, return it.
        if (has_consts)
        {
            if (handleConstantInput(
                    block,
                    result,
                    rows,
                    const_val,
                    const_val_is_null,
                    !in.empty(),
                    has_nullable_input_column))
                return;
        }

        auto col_res = ColumnUInt8::create(rows, static_cast<UInt8>(0));
        UInt8Container & vec_res = col_res->getData();
        auto col_res_is_null = ColumnUInt8::create();
        UInt8Container & vec_res_is_null = col_res_is_null->getData();
        if (has_nullable_input_column)
            vec_res_is_null.assign(rows, static_cast<UInt8>(0));

        /// Convert all columns to UInt8
        UInt8ColumnPtrs uint8_in_not_null;
        UInt8ColumnPtrs uint8_in_nullable;
        std::vector<const UInt8Container *> null_map_in;
        Columns converted_columns;

        for (const auto * column : in)
        {
            if (const auto * uint8_column = checkAndGetColumn<ColumnUInt8>(column))
            {
                uint8_in_not_null.push_back(uint8_column);
            }
            else
            {
                if (column->isColumnNullable())
                {
                    const auto * nullable_col = checkAndGetColumn<ColumnNullable>(column);
                    null_map_in.push_back(&nullable_col->getNullMapData());
                    const auto * not_null_col = &nullable_col->getNestedColumn();
                    if (const auto * uint8_column = checkAndGetColumn<ColumnUInt8>(not_null_col))
                    {
                        uint8_in_nullable.push_back(uint8_column);
                    }
                    else
                    {
                        auto converted_column = ColumnUInt8::create(rows);
                        convertToUInt8(not_null_col, converted_column->getData());
                        uint8_in_nullable.push_back(converted_column.get());
                        converted_columns.emplace_back(std::move(converted_column));
                    }
                }
                else
                {
                    auto converted_column = ColumnUInt8::create(rows);
                    convertToUInt8(column, converted_column->getData());
                    uint8_in_not_null.push_back(converted_column.get());
                    converted_columns.emplace_back(std::move(converted_column));
                }
            }
        }

        bool has_not_null_column;
        // first handle not-null column
        if (!uint8_in_not_null.empty())
        {
            has_not_null_column = true;
            if (uint8_in_not_null.size() == 1)
            {
                vec_res.assign(uint8_in_not_null[0]->getData());
            }
            else
            {
                while (uint8_in_not_null.size() > 1)
                {
                    AssociativeOperationImpl<Impl, 10>::execute(uint8_in_not_null, vec_res);
                    uint8_in_not_null.push_back(col_res.get());
                }
            }
        }
        // then handle nullable column
        if (!uint8_in_nullable.empty())
        {
            if (has_not_null_column)
            {
                // if there is not null column, then need to append the current result to uint8_not_null_column
                uint8_in_nullable.push_back(col_res.get());
                null_map_in.push_back(&vec_res_is_null);
            }
            if (uint8_in_nullable.size() == 1)
            {
                // special case
                vec_res.assign(uint8_in_nullable[0]->getData());
                vec_res_is_null.assign(*null_map_in[0]);
            }
            else
            {
                while (uint8_in_nullable.size() > 1)
                {
                    NullableAssociativeOperationImpl<Impl, 10>::execute(
                        uint8_in_nullable,
                        null_map_in,
                        vec_res,
                        vec_res_is_null);
                    uint8_in_nullable.push_back(col_res.get());
                    null_map_in.push_back(&vec_res_is_null);
                }
            }
        }

        if (has_consts)
        {
            // if there is const, then apply the constant
            if (has_nullable_input_column)
            {
                for (size_t i = 0; i < rows; ++i)
                {
                    Impl::applyNullable(
                        const_val,
                        const_val_is_null,
                        vec_res[i],
                        vec_res_is_null[i],
                        vec_res[i],
                        vec_res_is_null[i]);
                }
            }
            else
            {
                for (size_t i = 0; i < rows; ++i)
                {
                    Impl::apply(const_val, vec_res[i], vec_res[i]);
                }
            }
        }

        if (has_nullable_input_column)
        {
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_res), std::move(col_res_is_null));
        }
        else
        {
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
struct NameAnd { static constexpr auto name = "and"; };
struct NameOr { static constexpr auto name = "or"; };
struct NameXor { static constexpr auto name = "xor"; };
struct NameNot { static constexpr auto name = "not"; };
// clang-format on

using FunctionAnd = FunctionAnyArityLogical<AndImpl, NameAnd, true>;
using FunctionOr = FunctionAnyArityLogical<OrImpl, NameOr, true>;
using FunctionXor = FunctionAnyArityLogical<XorImpl, NameXor, false>;
using FunctionNot = FunctionUnaryLogical<NotImpl, NameNot>;

} // namespace DB
