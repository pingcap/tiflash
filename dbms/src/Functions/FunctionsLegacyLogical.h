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


struct LegacyAndImpl
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

struct LegacyOrImpl
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

struct LegacyXorImpl
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


template <typename Op, size_t N>
struct LegacyAssociativeOperationImpl
{
    /// Erases the N last columns from `in` (if there are less, then all) and puts into `result` their combination.
    static void NO_INLINE execute(UInt8ColumnPtrs & in, UInt8Container & result)
    {
        if (N > in.size())
        {
            LegacyAssociativeOperationImpl<Op, N - 1>::execute(in, result);
            return;
        }

        LegacyAssociativeOperationImpl<Op, N> operation(in);
        in.erase(in.end() - N, in.end());

        size_t n = result.size();
        for (size_t i = 0; i < n; ++i)
        {
            result[i] = operation.apply(i);
        }
    }

    const UInt8Container & vec;
    LegacyAssociativeOperationImpl<Op, N - 1> continuation;

    /// Remembers the last N columns from `in`.
    LegacyAssociativeOperationImpl(UInt8ColumnPtrs & in)
        : vec(in[in.size() - N]->getData())
        , continuation(in)
    {}

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline bool apply(size_t i) const
    {
        if (Op::isSaturable())
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
            return Op::isSaturatedValue(a) ? a : continuation.apply(i);
        }
        else
        {
            return Op::apply(vec[i], continuation.apply(i));
        }
    }
};

template <typename Op>
struct LegacyAssociativeOperationImpl<Op, 1>
{
    static void execute(UInt8ColumnPtrs &, UInt8Container &)
    {
        throw Exception("Logical error: AssociativeOperationImpl<Op, 1>::execute called", ErrorCodes::LOGICAL_ERROR);
    }

    const UInt8Container & vec;

    LegacyAssociativeOperationImpl(UInt8ColumnPtrs & in)
        : vec(in[in.size() - 1]->getData())
    {}

    inline bool apply(size_t i) const { return vec[i]; }
};


/**
 * The behavior of and and or is the same as
 * https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
 */
template <typename Impl, typename Name, bool special_impl_for_nulls>
class FunctionLegacyAnyArityLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionLegacyAnyArityLogical>(); }

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
            LegacyAssociativeOperationImpl<Impl, 10>::execute(uint8_in, vec_res);
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


// clang-format off
struct NameLegacyAnd { static constexpr auto name = "legacy_and"; };
struct NameLegacyOr { static constexpr auto name = "legacy_or"; };
struct NameLegacyXor { static constexpr auto name = "legacy_xor"; };
// clang-format on

using FunctionLegacyAnd = FunctionLegacyAnyArityLogical<LegacyAndImpl, NameLegacyAnd, true>;
using FunctionLegacyOr = FunctionLegacyAnyArityLogical<LegacyOrImpl, NameLegacyOr, true>;
using FunctionLegacyXor = FunctionLegacyAnyArityLogical<LegacyXorImpl, NameLegacyXor, false>;

} // namespace DB
