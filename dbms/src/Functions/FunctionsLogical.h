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
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/FieldVisitors.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>

#include <tuple>
#include <utility>
#include <vector>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct AndImpl
{
    static constexpr bool isSaturable = true;

    static inline bool isSaturatedValue(bool a) { return !a; }
    static inline bool isSaturatedValue(bool a, bool a_is_null) { return !a_is_null && !a; }
    static inline bool canConstantBeIgnored(bool a, bool a_is_null) { return !a_is_null && a; }

    static inline bool apply(bool a, bool b) { return a && b; }
    static inline std::pair<bool, bool> applyTwoNullable(bool a, bool a_is_null, bool b, bool b_is_null)
    {
        // result is null if
        // 1. both a and b is null
        // 2. a is null and b is true
        // 3. b is null and a is true
        auto res_is_null = (a_is_null && b_is_null) || (a_is_null && b) || (b_is_null && a);
        // when a_is_null/b_is_null == true, a/b could be either true or false, but
        // we don't need to consider this because if result_is_null = true, res is
        // meaningless if result_is_null = false, then b/a must be false, then a &&
        // b is always false
        auto res = a && b;
        return std::make_pair(res, res_is_null);
    }
    static inline std::pair<bool, bool> applyOneNullable(bool a, bool a_is_null, bool b)
    {
        // result is null if a is null and b is true
        auto res_is_null = a_is_null && b;
        // when a_is_null == true, a could be either true or false, but we don't
        // need to consider this because if result_is_null = true, res is
        // meaningless if result_is_null = false, then b must be false, then a && b
        // is always false
        auto res = a && b;
        return std::make_pair(res, res_is_null);
    }
};

struct OrImpl
{
    static constexpr bool isSaturable = true;

    static inline bool isSaturatedValue(bool a) { return a; }
    static inline bool isSaturatedValue(bool a, bool a_is_null) { return !a_is_null && a; }
    static inline bool canConstantBeIgnored(bool a, bool a_is_null) { return !a_is_null && !a; }

    static inline bool apply(bool a, bool b) { return a || b; }
    static inline std::pair<bool, bool> applyTwoNullable(bool a, bool a_is_null, bool b, bool b_is_null)
    {
        // result is null if
        // 1. both a and b is null
        // 2. a is null and b is false
        // 3. b is null and a is false
        auto res_is_null = (a_is_null && b_is_null) || (a_is_null && !b) || (b_is_null && !a);
        // when a_is_null/b_is_null == true, a/b could be either true or false, but
        // we don't need to consider this because if result_is_null = true, res is
        // meaningless if result_is_null = false, then b/a must be true, then a && b
        // is always true
        auto res = a || b;
        return std::make_pair(res, res_is_null);
    }
    static inline std::pair<bool, bool> applyOneNullable(bool a, bool a_is_null, bool b)
    {
        // result is null if a is null and b is false
        auto res_is_null = a_is_null && !b;
        // when a_is_null == true, a could be either true or false, but we don't
        // need to consider this because if result_is_null = true, res is
        // meaningless if result_is_null = false, then b must be true, then a && b
        // is always true
        auto res = a || b;
        return std::make_pair(res, res_is_null);
    }
};

struct XorImpl
{
    static constexpr bool isSaturable = false;

    static inline bool isSaturatedValue(bool) { return false; }
    static inline bool isSaturatedValue(bool, bool) { return false; }
    static inline bool canConstantBeIgnored(bool, bool) { return false; }

    static inline bool apply(bool a, bool b) { return a != b; }
    static inline std::pair<bool, bool> applyTwoNullable(bool a, bool a_is_null, bool b, bool b_is_null)
    {
        auto res_is_null = a_is_null || b_is_null;
        auto res = a != b;
        return std::make_pair(res, res_is_null);
    }
    static inline std::pair<bool, bool> applyOneNullable(bool a, bool a_is_null, bool b)
    {
        auto res_is_null = a_is_null;
        auto res = a != b;
        return std::make_pair(res, res_is_null);
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
    /// Erases the N last columns from `in` (if there are less, then all) and puts
    /// into `result` their combination.
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
            result[i] = operation.apply(i);
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

    /// Returns a combination of values in the i-th row of all columns stored in
    /// the constructor.
    inline bool apply(size_t i) const
    {
        if constexpr (Op::isSaturable)
        {
            // cast a: UInt8 -> bool -> UInt8 is a trick
            // TiFlash converts columns with non-UInt8 type to UInt8 type and sets
            // value to 0 or 1 which correspond to false or true. However, for columns
            // with UInt8 type, no more convertion will be executed on them and the
            // values stored in them are 'origin' which means that they won't be
            // converted to 0 or 1. For example:
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
            return Op::isSaturatedValue(a) ? a : continuation.apply(i);
        }
        else
        {
            return Op::apply(vec[i], continuation.apply(i));
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
            res[i] = operation.apply(i);
        }
    }

    const UInt8Container & a;
    const UInt8Container & b;

    explicit AssociativeOperationImpl(UInt8ColumnPtrs & in)
        : a(in[in.size() - 2]->getData())
        , b(in[in.size() - 1]->getData())
    {}

    inline bool apply(size_t i) const { return Op::apply(a[i], b[i]); }
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
        throw Exception("Logical error: AssociativeOperationImpl<Op, 1> is constructed", ErrorCodes::LOGICAL_ERROR);
    }

    inline bool apply(size_t) const
    {
        throw Exception("Logical error: AssociativeOperationImpl<Op, 1>::apply called", ErrorCodes::LOGICAL_ERROR);
    }
};

template <typename Op, size_t N>
struct NullableAssociativeOperationImpl
{
    /// Erases the N last columns from `in` (if there are less, then all) and puts
    /// into `result` their combination.
    static void NO_INLINE execute(
        UInt8ColumnPtrs & in,
        std::vector<const UInt8Container *> & null_map_in,
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
            std::tie(result[i], result_is_null[i]) = operation.apply(i);
        }
    }
    static void NO_INLINE
    execute(UInt8ColumnPtrs & in, std::vector<const UInt8Container *> & null_map_in, UInt8Container & result)
    {
        if (N > in.size())
        {
            NullableAssociativeOperationImpl<Op, N - 1>::execute(in, null_map_in, result);
            return;
        }

        NullableAssociativeOperationImpl<Op, N> operation(in, null_map_in);
        in.erase(in.end() - N, in.end());
        null_map_in.erase(null_map_in.end() - N, null_map_in.end());

        size_t n = result.size();
        for (size_t i = 0; i < n; ++i)
        {
            result[i] = operation.applyNotNull(i);
        }
    }

    const UInt8Container & vec;
    const UInt8Container * null_map;
    NullableAssociativeOperationImpl<Op, N - 1> continuation;

    /// Remembers the last N columns from `in`.
    NullableAssociativeOperationImpl(UInt8ColumnPtrs & in, std::vector<const UInt8Container *> & null_map_in)
        : vec(in[in.size() - N]->getData())
        , null_map(null_map_in[null_map_in.size() - N])
        , continuation(in, null_map_in)
    {}

    /// Returns a combination of values in the i-th row of all columns stored in
    /// the constructor.
    inline std::pair<bool, bool> apply(size_t i) const
    {
        bool tmp, tmp_is_null;
        std::tie(tmp, tmp_is_null) = continuation.apply(i);
        bool a = static_cast<bool>(vec[i]);
        bool is_null = (*null_map)[i];
        return Op::applyTwoNullable(a, is_null, tmp, tmp_is_null);
    }
    inline bool applyNotNull(size_t i) const
    {
        bool a = !(*null_map)[i] && static_cast<bool>(vec[i]);
        return Op::isSaturatedValue(a) ? a : continuation.apply(i);
    }
};

template <typename Op>
struct NullableAssociativeOperationImpl<Op, 2>
{
    static void execute(
        UInt8ColumnPtrs & in,
        std::vector<const UInt8Container *> & null_map_in,
        UInt8Container & res,
        UInt8Container & res_is_null)
    {
        if (in.size() <= 1)
            throw Exception("Logical error: should not reach here");
        NullableAssociativeOperationImpl<Op, 2> operation(in, null_map_in);
        in.erase(in.end() - 2, in.end());
        null_map_in.erase(null_map_in.end() - 2, null_map_in.end());

        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
        {
            std::tie(res[i], res_is_null[i]) = operation.apply(i);
        }
    }
    static void execute(UInt8ColumnPtrs & in, std::vector<const UInt8Container *> & null_map_in, UInt8Container & res)
    {
        if (in.size() <= 1)
            throw Exception("Logical error: should not reach here");
        NullableAssociativeOperationImpl<Op, 2> operation(in, null_map_in);
        in.erase(in.end() - 2, in.end());
        null_map_in.erase(null_map_in.end() - 2, null_map_in.end());

        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
        {
            res[i] = operation.applyNotNull(i);
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

    inline std::pair<bool, bool> apply(size_t i) const
    {
        return Op::applyTwoNullable(a[i], (*a_null_map)[i], b[i], (*b_null_map)[i]);
    }
    inline bool applyNotNull(size_t i) const { return Op::apply(!(*a_null_map)[i] && a[i], !(*b_null_map)[i] && b[i]); }
};

template <typename Op>
struct NullableAssociativeOperationImpl<Op, 1>
{
    static void execute(UInt8ColumnPtrs &, std::vector<const UInt8Container *> &, UInt8Container &, UInt8Container &)
    {
        throw Exception(
            "Logical error: NullableAssociativeOperationImpl<Op, 1>::execute called",
            ErrorCodes::LOGICAL_ERROR);
    }
    static void execute(UInt8ColumnPtrs &, std::vector<const UInt8Container *> &, UInt8Container &)
    {
        throw Exception(
            "Logical error: NullableAssociativeOperationImpl<Op, 1>::execute called",
            ErrorCodes::LOGICAL_ERROR);
    }

    NullableAssociativeOperationImpl(UInt8ColumnPtrs &, std::vector<const UInt8Container *> &)
    {
        throw Exception(
            "Logical error: NullableAssociativeOperationImpl<Op, 1> is constructed",
            ErrorCodes::LOGICAL_ERROR);
    }

    inline std::pair<bool, bool> apply(size_t) const
    {
        throw Exception(
            "Logical error: NullableAssociativeOperationImpl<Op, 1>::apply called",
            ErrorCodes::LOGICAL_ERROR);
    }
    inline bool applyNotNull(size_t) const
    {
        throw Exception(
            "Logical error: NullableAssociativeOperationImpl<Op, 1>::applyNotNull called",
            ErrorCodes::LOGICAL_ERROR);
    }
};

/**
 * The behavior of and and or is the same as
 * https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
 * if two_value_logic_op is true, the function will only return true/false, and any null input will 
 * be treated as false
 */
template <typename Impl, typename Name, bool special_impl_for_nulls, bool two_value_logic_op = false>
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
            // special case, if column->onlyNull() returns true, treated as constant
            // column
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
            if constexpr (two_value_logic_op)
            {
                const_val = const_val_is_null ? false : const_val;
                const_val_is_null = false;
            }

            if (has_res)
            {
                if constexpr (special_impl_for_nulls)
                {
                    std::tie(res, res_is_null) = Impl::applyTwoNullable(res, res_is_null, const_val, const_val_is_null);
                }
                else
                {
                    res = Impl::apply(res, const_val);
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

    /// Get result types by argument types. If the function does not apply to
    /// these arguments, throw an exception.
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

        if (!two_value_logic_op && has_nullable_input_column)
            return makeNullable(std::make_shared<DataTypeUInt8>());
        else
            return std::make_shared<DataTypeUInt8>();
    }

    // if the result is constant, then generate the constant result and return true
    bool tryGenerateConstantResult(
        Block & block,
        size_t result,
        size_t rows,
        UInt8 const_val,
        UInt8 const_val_is_null,
        bool has_other_column,
        bool result_is_nullable) const
    {
        bool result_is_constant = false;
        UInt8 const_res = const_val, const_res_is_null = const_val_is_null;
        if (!has_other_column)
        {
            // all the input is constant
            result_is_constant = true;
        }
        else
        {
            // if the value is saturated value, then the result is constant
            result_is_constant = Impl::isSaturatedValue(const_val, const_res_is_null);
            if (result_is_constant)
                std::tie(const_res, const_res_is_null)
                    = Impl::applyTwoNullable(const_val, const_val_is_null, true, false);
        }
        if (result_is_constant)
        {
            // if result is constant, just return constant
            if (result_is_nullable)
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

    /*
     * handle the cases that there is only one input column
     * this could happen when there is N original input, but N-1 of them are constant, and
     * the merged constant value could be ignored for current logical operator
     * for example, the merged constant value is true for And operator, in this case
     * there is no need to do the real calculation, just copy the input column is enough
     */
    void handleSingleInputColumn(
        Block & block,
        size_t result,
        size_t rows,
        UInt8ColumnPtrs & not_null_uint8_columns,
        UInt8ColumnPtrs & nullable_uint8_columns,
        std::vector<const UInt8Container *> & null_maps,
        bool result_is_nullable) const
    {
        // logical op with 1 input columns, special case when there is a constant and the constant can be ignored
        auto col_res = ColumnUInt8::create(rows);
        UInt8Container & vec_res = col_res->getData();
        auto col_res_is_null = ColumnUInt8::create();
        UInt8Container & vec_res_is_null = col_res_is_null->getData();
        if (!not_null_uint8_columns.empty())
        {
            const auto & col_data = not_null_uint8_columns[0]->getData();
            // according to https://github.com/pingcap/tiflash/issues/5849
            // need to cast the UInt8 column to bool explicitly
            for (size_t i = 0; i < rows; ++i)
            {
                vec_res[i] = static_cast<bool>(col_data[i]);
            }
            if (result_is_nullable)
                vec_res_is_null.assign(rows, static_cast<UInt8>(0));
        }
        else
        {
            const auto & col_data = nullable_uint8_columns[0]->getData();
            if constexpr (two_value_logic_op)
            {
                for (size_t i = 0; i < rows; ++i)
                {
                    vec_res[i] = !(*null_maps[0])[i] && col_data[i];
                }
            }
            else
            {
                // according to https://github.com/pingcap/tiflash/issues/5849
                // need to cast the UInt8 column to bool explicitly
                for (size_t i = 0; i < rows; ++i)
                {
                    vec_res[i] = static_cast<bool>(col_data[i]);
                }
                vec_res_is_null.assign(*null_maps[0]);
            }
        }
        if (result_is_nullable)
        {
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_res), std::move(col_res_is_null));
        }
        else
        {
            block.getByPosition(result).column = std::move(col_res);
        }
    }

    /*
     * handle the case that there are only 2 input columns
     * the main difference between this function and handleMultipleInputColumns is
     * when the 2 input columns are [not_null_column, nullable_column]
     * Impl::applyOneNullable is used
     */
    void handleTwoInputColumns(
        Block & block,
        size_t result,
        size_t rows,
        bool has_const,
        UInt8 const_val,
        UInt8 const_val_is_null,
        UInt8ColumnPtrs & not_null_uint8_columns,
        UInt8ColumnPtrs & nullable_uint8_columns,
        std::vector<const UInt8Container *> & null_maps,
        bool result_is_nullable) const
    {
        // logical op with 2 input columns
        assert(static_cast<UInt8>(has_const) + not_null_uint8_columns.size() + nullable_uint8_columns.size() == 2);
        auto col_res = ColumnUInt8::create(rows, static_cast<UInt8>(0));
        UInt8Container & vec_res = col_res->getData();
        auto col_res_is_null = ColumnUInt8::create();
        UInt8Container & vec_res_is_null = col_res_is_null->getData();
        if (result_is_nullable)
            vec_res_is_null.assign(rows, static_cast<UInt8>(0));
        if (has_const)
        {
            if (!not_null_uint8_columns.empty())
            {
                const auto & col_data = not_null_uint8_columns[0]->getData();
                if (const_val_is_null)
                {
                    for (size_t i = 0; i < rows; ++i)
                        std::tie(vec_res[i], vec_res_is_null[i])
                            = Impl::applyOneNullable(const_val, const_val_is_null, col_data[i]);
                }
                else
                {
                    for (size_t i = 0; i < rows; ++i)
                    {
                        vec_res[i] = Impl::apply(const_val, col_data[i]);
                    }
                }
            }
            else
            {
                const auto & col_data = nullable_uint8_columns[0]->getData();
                const auto & null_data = *null_maps[0];
                if (const_val_is_null)
                {
                    for (size_t i = 0; i < rows; ++i)
                        std::tie(vec_res[i], vec_res_is_null[i])
                            = Impl::applyTwoNullable(const_val, const_val_is_null, col_data[i], null_data[i]);
                }
                else
                {
                    if constexpr (two_value_logic_op)
                    {
                        for (size_t i = 0; i < rows; ++i)
                            vec_res[i] = Impl::apply(!null_data[i] && col_data[i], const_val);
                    }
                    else
                    {
                        for (size_t i = 0; i < rows; ++i)
                            std::tie(vec_res[i], vec_res_is_null[i])
                                = Impl::applyOneNullable(col_data[i], null_data[i], const_val);
                    }
                }
            }
        }
        else
        {
            if (not_null_uint8_columns.size() == 2)
            {
                // case 1: 2 not null
                AssociativeOperationImpl<Impl, 2>::execute(not_null_uint8_columns, vec_res);
            }
            else if (not_null_uint8_columns.size() == 1 && nullable_uint8_columns.size() == 1)
            {
                // case 2: 1 not null + 1 nullable
                const auto & col_1 = not_null_uint8_columns[0]->getData();
                const auto & col_2 = nullable_uint8_columns[0]->getData();
                const auto & null_map_2 = *null_maps[0];
                if constexpr (two_value_logic_op)
                {
                    for (size_t i = 0; i < rows; ++i)
                    {
                        vec_res[i] = Impl::apply(!null_map_2[i] && col_2[i], col_1[i]);
                    }
                }
                else
                {
                    for (size_t i = 0; i < rows; ++i)
                    {
                        std::tie(vec_res[i], vec_res_is_null[i])
                            = Impl::applyOneNullable(col_2[i], null_map_2[i], col_1[i]);
                    }
                }
            }
            else
            {
                // case 3: 2 nullable
                if constexpr (two_value_logic_op)
                {
                    NullableAssociativeOperationImpl<Impl, 2>::execute(nullable_uint8_columns, null_maps, vec_res);
                }
                else
                {
                    NullableAssociativeOperationImpl<Impl, 2>::execute(
                        nullable_uint8_columns,
                        null_maps,
                        vec_res,
                        vec_res_is_null);
                }
            }
        }
        if (result_is_nullable)
        {
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_res), std::move(col_res_is_null));
        }
        else
        {
            block.getByPosition(result).column = std::move(col_res);
        }
    }

    /*
     * handle the cases that there are more than 2 input columns
     */
    void handleMultipleInputColumns(
        Block & block,
        size_t result,
        size_t rows,
        bool has_const,
        UInt8 const_val,
        UInt8 const_val_is_null,
        UInt8ColumnPtrs & not_null_uint8_columns,
        UInt8ColumnPtrs & nullable_uint8_columns,
        std::vector<const UInt8Container *> & null_maps,
        bool result_is_nullable) const
    {
        // logical op with more than 2 input columns
        auto col_res = ColumnUInt8::create(rows, static_cast<UInt8>(0));
        UInt8Container & vec_res = col_res->getData();
        auto col_res_is_null = ColumnUInt8::create();
        UInt8Container & vec_res_is_null = col_res_is_null->getData();
        if (result_is_nullable)
            vec_res_is_null.assign(rows, static_cast<UInt8>(0));

        bool has_not_null_column = false;
        // first handle not-null column
        if (!not_null_uint8_columns.empty())
        {
            has_not_null_column = true;
            if (not_null_uint8_columns.size() == 1)
            {
                const auto & col_data = not_null_uint8_columns[0]->getData();
                // according to https://github.com/pingcap/tiflash/issues/5849
                // need to cast the UInt8 column to bool explicitly
                for (size_t i = 0; i < rows; ++i)
                {
                    vec_res[i] = static_cast<bool>(col_data[i]);
                }
            }
            else
            {
                while (not_null_uint8_columns.size() > 1)
                {
                    // micro benchmark shows 4 << 6, 8 outperforms 6 slightly, and performance may decline when set to 10
                    AssociativeOperationImpl<Impl, 8>::execute(not_null_uint8_columns, vec_res);
                    not_null_uint8_columns.push_back(col_res.get());
                }
            }
        }
        // then handle nullable column
        if (!nullable_uint8_columns.empty())
        {
            if (!result_is_nullable)
            {
                // for temperary usage
                vec_res_is_null.assign(rows, static_cast<UInt8>(0));
            }
            if (has_not_null_column)
            {
                // if there is not null column, then need to append the current result
                // to uint8_not_null_column as the input column
                nullable_uint8_columns.push_back(col_res.get());
                null_maps.push_back(&vec_res_is_null);
            }
            if (nullable_uint8_columns.size() == 1)
            {
                // special case
                assert(!has_not_null_column);
                const auto & col_data = nullable_uint8_columns[0]->getData();
                // according to https://github.com/pingcap/tiflash/issues/5849
                // need to cast the UInt8 column to bool explicitly
                if constexpr (two_value_logic_op)
                {
                    for (size_t i = 0; i < rows; ++i)
                    {
                        vec_res[i] = !(*null_maps[0])[i] && static_cast<bool>(col_data[i]);
                    }
                }
                else
                {
                    for (size_t i = 0; i < rows; ++i)
                    {
                        vec_res[i] = static_cast<bool>(col_data[i]);
                    }
                    vec_res_is_null.assign(*null_maps[0]);
                }
            }
            else
            {
                while (nullable_uint8_columns.size() > 1)
                {
                    // micro benchmark shows 4 << 6, 8 outperforms 6 slightly, and performance may decline when set to 10
                    if constexpr (two_value_logic_op)
                    {
                        NullableAssociativeOperationImpl<Impl, 8>::execute(nullable_uint8_columns, null_maps, vec_res);
                        nullable_uint8_columns.push_back(col_res.get());
                        null_maps.push_back(&vec_res_is_null);
                    }
                    else
                    {
                        NullableAssociativeOperationImpl<Impl, 8>::execute(
                            nullable_uint8_columns,
                            null_maps,
                            vec_res,
                            vec_res_is_null);
                        nullable_uint8_columns.push_back(col_res.get());
                        null_maps.push_back(&vec_res_is_null);
                    }
                }
            }
        }

        if (has_const)
        {
            // if there is const, then apply the constant at last
            if (result_is_nullable)
            {
                for (size_t i = 0; i < rows; ++i)
                {
                    std::tie(vec_res[i], vec_res_is_null[i])
                        = Impl::applyTwoNullable(const_val, const_val_is_null, vec_res[i], vec_res_is_null[i]);
                }
            }
            else
            {
                for (size_t i = 0; i < rows; ++i)
                {
                    vec_res[i] = Impl::apply(const_val, vec_res[i]);
                }
            }
        }

        if (result_is_nullable)
        {
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_res), std::move(col_res_is_null));
        }
        else
        {
            block.getByPosition(result).column = std::move(col_res);
        }
    }

    void convertAllInputToUInt8(
        ColumnRawPtrs & input_columns,
        size_t rows,
        UInt8ColumnPtrs & not_null_uint8_columns,
        UInt8ColumnPtrs & nullable_uint8_columns,
        std::vector<const UInt8Container *> & null_maps,
        Columns & converted_columns) const
    {
        for (const auto * column : input_columns)
        {
            if (const auto * uint8_column = checkAndGetColumn<ColumnUInt8>(column))
            {
                not_null_uint8_columns.push_back(uint8_column);
            }
            else
            {
                if (column->isColumnNullable())
                {
                    const auto * nullable_col = checkAndGetColumn<ColumnNullable>(column);
                    null_maps.push_back(&nullable_col->getNullMapData());
                    const auto * not_null_col = &nullable_col->getNestedColumn();
                    if (const auto * uint8_column = checkAndGetColumn<ColumnUInt8>(not_null_col))
                    {
                        nullable_uint8_columns.push_back(uint8_column);
                    }
                    else
                    {
                        auto converted_column = ColumnUInt8::create(rows);
                        convertToUInt8(not_null_col, converted_column->getData());
                        nullable_uint8_columns.push_back(converted_column.get());
                        converted_columns.emplace_back(std::move(converted_column));
                    }
                }
                else
                {
                    auto converted_column = ColumnUInt8::create(rows);
                    convertToUInt8(column, converted_column->getData());
                    not_null_uint8_columns.push_back(converted_column.get());
                    converted_columns.emplace_back(std::move(converted_column));
                }
            }
        }
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
        if (has_consts)
        {
            // If this value uniquely determines the result, return it.
            if (tryGenerateConstantResult(
                    block,
                    result,
                    rows,
                    const_val,
                    const_val_is_null,
                    !in.empty(),
                    has_nullable_input_column))
                return;
        }

        // used to hold the ColumnPtr for not null uint8 columns
        UInt8ColumnPtrs not_null_uint8_columns;
        // used to hold the ColumnPtr for nullable uint8 columns
        UInt8ColumnPtrs nullable_uint8_columns;
        // used to hold the NullMap for nullable uint8 columns
        std::vector<const UInt8Container *> null_maps;
        // used to hold the converted columns, the columns in
        // not_null_uint8_columns/nullable_uint8_columns are raw pointer
        // and converted_columns is used to hold the ColumnPtr of these
        // raw pointer if needed
        Columns converted_columns;
        /// Convert all columns to UInt8
        convertAllInputToUInt8(in, rows, not_null_uint8_columns, nullable_uint8_columns, null_maps, converted_columns);

        if (Impl::canConstantBeIgnored(const_val, const_val_is_null))
            has_consts = false;

        if (in.size() + static_cast<UInt8>(has_consts) == 1)
        {
            RUNTIME_CHECK_MSG(has_consts == false, " Logical error, has_consts must be false");
            handleSingleInputColumn(
                block,
                result,
                rows,
                not_null_uint8_columns,
                nullable_uint8_columns,
                null_maps,
                has_nullable_input_column);
        }
        else if (in.size() + static_cast<UInt8>(has_consts) == 2)
        {
            // input is 2 columns
            handleTwoInputColumns(
                block,
                result,
                rows,
                has_consts,
                const_val,
                const_val_is_null,
                not_null_uint8_columns,
                nullable_uint8_columns,
                null_maps,
                has_nullable_input_column);
        }
        else
        {
            // input is more than 2 columns
            handleMultipleInputColumns(
                block,
                result,
                rows,
                has_consts,
                const_val,
                const_val_is_null,
                not_null_uint8_columns,
                nullable_uint8_columns,
                null_maps,
                has_nullable_input_column);
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
struct NameAndTwoValue { static constexpr auto name = "and_two_value"; };
struct NameOr { static constexpr auto name = "or"; };
struct NameXor { static constexpr auto name = "xor"; };
struct NameNot { static constexpr auto name = "not"; };
// clang-format on

using FunctionAnd = FunctionAnyArityLogical<AndImpl, NameAnd, true>;
using FunctionAndTwoValue = FunctionAnyArityLogical<AndImpl, NameAndTwoValue, true, true>;
using FunctionOr = FunctionAnyArityLogical<OrImpl, NameOr, true>;
using FunctionXor = FunctionAnyArityLogical<XorImpl, NameXor, false>;
using FunctionNot = FunctionUnaryLogical<NotImpl, NameNot>;

} // namespace DB
