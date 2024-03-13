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
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitors.h>
#include <Common/toSafeUnsigned.h>
#include <Common/typeid_cast.h>
#include <Core/AccurateComparison.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Functions/DataTypeFromFieldType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/IsOperation.h>
#include <Functions/castTypeToEither.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <fmt/core.h>

#include <boost/integer/common_factor.hpp>
#include <ext/range.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_DIVISION;
extern const int ILLEGAL_COLUMN;
extern const int LOGICAL_ERROR;
extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
extern const int DECIMAL_OVERFLOW;
} // namespace ErrorCodes

//
/// this one is just for convenience
template <bool B, typename T1, typename T2>
using If = std::conditional_t<B, T1, T2>;

/** Arithmetic operations: +, -, *, /, %,
  * intDiv (integer division).
  * Bitwise operations: |, &, ^, ~.
  * Etc.
  */

template <typename A, typename B, typename Op, typename OpResultType = typename Op::ResultType>
struct BinaryOperationImplBase
{
    using ResultType = OpResultType;
    using ColVecA = std::conditional_t<IsDecimal<A>, ColumnDecimal<A>, ColumnVector<A>>;
    using ColVecB = std::conditional_t<IsDecimal<B>, ColumnDecimal<B>, ColumnVector<B>>;
    using ArrayA = typename ColVecA::Container;
    using ArrayB = typename ColVecB::Container;

    static void NO_INLINE vectorVector(const ArrayA & a, const ArrayB & b, PaddedPODArray<ResultType> & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            if constexpr (IsDecimal<A> && IsDecimal<B>)
                c[i] = Op::template apply<ResultType>(
                    DecimalField<A>(a[i], a.getScale()),
                    DecimalField<B>(b[i], b.getScale()));
            else if constexpr (IsDecimal<A>)
                c[i] = Op::template apply<ResultType>(DecimalField<A>(a[i], a.getScale()), b[i]);
            else if constexpr (IsDecimal<B>)
                c[i] = Op::template apply<ResultType>(a[i], DecimalField<B>(b[i], b.getScale()));
            else
                c[i] = Op::template apply<ResultType>(a[i], b[i]);
    }

    static void NO_INLINE vectorVectorNullable(
        const ArrayA & a,
        const ColumnUInt8 * a_nullmap,
        const ArrayB & b,
        const ColumnUInt8 * b_nullmap,
        PaddedPODArray<ResultType> & c,
        typename ColumnUInt8::Container & res_null)
    {
        size_t size = a.size();
        if (a_nullmap != nullptr && b_nullmap != nullptr)
        {
            const auto & a_nullmap_data = a_nullmap->getData();
            const auto & b_nullmap_data = b_nullmap->getData();
            for (size_t i = 0; i < size; i++)
                res_null[i] = a_nullmap_data[i] || b_nullmap_data[i];
        }
        else if (a_nullmap != nullptr || b_nullmap != nullptr)
        {
            const auto & nullmap_data = a_nullmap != nullptr ? a_nullmap->getData() : b_nullmap->getData();
            for (size_t i = 0; i < size; i++)
                res_null[i] = nullmap_data[i];
        }
        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (IsDecimal<A> && IsDecimal<B>)
                c[i] = Op::template apply<ResultType>(
                    DecimalField<A>(a[i], a.getScale()),
                    DecimalField<B>(b[i], b.getScale()),
                    res_null[i]);
            else if constexpr (IsDecimal<A>)
                c[i] = Op::template apply<ResultType>(DecimalField<A>(a[i], a.getScale()), b[i], res_null[i]);
            else if constexpr (IsDecimal<B>)
                c[i] = Op::template apply<ResultType>(a[i], DecimalField<B>(b[i], b.getScale()), res_null[i]);
            else
                c[i] = Op::template apply<ResultType>(a[i], b[i], res_null[i]);
        }
    }

    static void NO_INLINE
    vectorConstant(const ArrayA & a, typename NearestFieldType<B>::Type b, PaddedPODArray<ResultType> & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            if constexpr (IsDecimal<A>)
                c[i] = Op::template apply<ResultType>(DecimalField<A>(a[i], a.getScale()), b);
            else
                c[i] = Op::template apply<ResultType>(a[i], b);
    }

    static void NO_INLINE vectorConstantNullable(
        const ArrayA & a,
        const ColumnUInt8 * a_nullmap,
        typename NearestFieldType<B>::Type b,
        PaddedPODArray<ResultType> & c,
        typename ColumnUInt8::Container & res_null)
    {
        size_t size = a.size();
        if (a_nullmap != nullptr)
        {
            const auto & nullmap_data = a_nullmap->getData();
            for (size_t i = 0; i < size; ++i)
                res_null[i] = nullmap_data[i];
        }
        for (size_t i = 0; i < size; ++i)
            if constexpr (IsDecimal<A>)
                c[i] = Op::template apply<ResultType>(DecimalField<A>(a[i], a.getScale()), b, res_null[i]);
            else
                c[i] = Op::template apply<ResultType>(a[i], b, res_null[i]);
    }

    static void NO_INLINE
    constantVector(typename NearestFieldType<A>::Type a, const ArrayB & b, PaddedPODArray<ResultType> & c)
    {
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (IsDecimal<B>)
                c[i] = Op::template apply<ResultType>(a, DecimalField<B>(b[i], b.getScale()));
            else
                c[i] = Op::template apply<ResultType>(a, b[i]);
        }
    }

    static void NO_INLINE constantVectorNullable(
        typename NearestFieldType<A>::Type a,
        const ArrayB & b,
        const ColumnUInt8 * b_nullmap,
        PaddedPODArray<ResultType> & c,
        typename ColumnUInt8::Container & res_null)
    {
        size_t size = b.size();
        if (b_nullmap != nullptr)
        {
            const auto & nullmap_data = b_nullmap->getData();
            for (size_t i = 0; i < size; i++)
                res_null[i] = nullmap_data[i];
        }
        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (IsDecimal<B>)
                c[i] = Op::template apply<ResultType>(a, DecimalField<B>(b[i], b.getScale()), res_null[i]);
            else
                c[i] = Op::template apply<ResultType>(a, b[i], res_null[i]);
        }
    }

    static ResultType constantConstant(typename NearestFieldType<A>::Type a, typename NearestFieldType<B>::Type b)
    {
        return Op::template apply<ResultType>(a, b);
    }
    static ResultType constantConstantNullable(
        typename NearestFieldType<A>::Type a,
        typename NearestFieldType<B>::Type b,
        UInt8 & res_null)
    {
        return Op::template apply<ResultType>(a, b, res_null);
    }
};

template <typename A, typename B, typename Op, typename ResultType = typename Op::ResultType>
struct BinaryOperationImpl : BinaryOperationImplBase<A, B, Op, ResultType>
{
};

/// these ones for better semantics
template <typename T>
using Then = T;
template <typename T>
using Else = T;

/// Binary operations for Decimals need scale args
/// +|- scale one of args (which scale factor is not 1). ScaleR = oneof(Scale1, Scale2);
/// *   no agrs scale. ScaleR = Scale1 + Scale2;
/// /   first arg scale. ScaleR = Scale1 (scale_a = DecimalType<B>::getScale()).
template <typename A, typename B, template <typename, typename> typename Operation, typename OpResultType>
struct DecimalBinaryOperation
{
    //static_assert((IsDecimal<A> || IsDecimal<B>) && IsDecimal<ResultType_>);
    //static_assert(IsDecimal<A> || std::is_integral_v<A>);
    //static_assert(IsDecimal<B> || std::is_integral_v<B>);

    static constexpr bool is_plus_minus = IsOperation<Operation>::plus || IsOperation<Operation>::minus;
    static constexpr bool is_multiply = IsOperation<Operation>::multiply;
    static constexpr bool is_modulo = IsOperation<Operation>::modulo;
    static constexpr bool is_float_division = IsOperation<Operation>::div_floating;
    static constexpr bool is_int_division = IsOperation<Operation>::div_int;
    static constexpr bool is_division = is_float_division || is_int_division;
    static constexpr bool is_compare = IsOperation<Operation>::least || IsOperation<Operation>::greatest;
    static constexpr bool is_plus_minus_compare = is_plus_minus || is_compare;
    static constexpr bool can_overflow = is_plus_minus || is_multiply;

    static constexpr bool need_promote_type = (std::is_same_v<OpResultType, A> || std::is_same_v<OpResultType, B>)&&(
        is_plus_minus_compare || is_division || is_multiply || is_modulo); // And is multiple / division / modulo
    static constexpr bool check_overflow
        = need_promote_type && std::is_same_v<OpResultType, Decimal256>; // Check if exceeds 10 * 66;

    using ResultType = OpResultType;
    using NativeResultType = typename ResultType::NativeType;
    using ColVecA = std::conditional_t<IsDecimal<A>, ColumnDecimal<A>, ColumnVector<A>>;
    using ColVecB = std::conditional_t<IsDecimal<B>, ColumnDecimal<B>, ColumnVector<B>>;
    using ArrayA = typename ColVecA::Container;
    using ArrayB = typename ColVecB::Container;
    using ArrayC = typename ColumnDecimal<ResultType>::Container;
    using PromoteResultType = typename PromoteType<NativeResultType>::Type;
    using InputType = std::conditional_t<need_promote_type, PromoteResultType, NativeResultType>;
    using Op = Operation<InputType, InputType>;

    static void inline evaluateNullmap(
        size_t size,
        const ColumnUInt8 * a_nullmap,
        const ColumnUInt8 * b_nullmap,
        typename ColumnUInt8::Container & res_null)
    {
        if (a_nullmap != nullptr && b_nullmap != nullptr)
        {
            const auto & a_nullmap_data = a_nullmap->getData();
            const auto & b_nullmap_data = b_nullmap->getData();
            for (size_t i = 0; i < size; ++i)
                res_null[i] = a_nullmap_data[i] || b_nullmap_data[i];
        }
        else if (a_nullmap != nullptr || b_nullmap != nullptr)
        {
            const auto & nullmap_data = a_nullmap != nullptr ? a_nullmap->getData() : b_nullmap->getData();
            for (size_t i = 0; i < size; ++i)
                res_null[i] = nullmap_data[i];
        }
    }

    static void NO_INLINE vectorVector(
        const ArrayA & a,
        const ArrayB & b,
        ArrayC & c,
        NativeResultType scale_a [[maybe_unused]],
        NativeResultType scale_b [[maybe_unused]],
        NativeResultType scale_result [[maybe_unused]])
    {
        size_t size = a.size();
        if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(a[i], b[i], scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(a[i], b[i], scale_b);
                return;
            }
        }
        else if constexpr (is_multiply)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaledMul(a[i], b[i], scale_result);
            return;
        }
        else if constexpr (is_division)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaled<true>(a[i], b[i], scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a[i], b[i]);
    }

    static void NO_INLINE vectorVectorNullable(
        const ArrayA & a,
        const ColumnUInt8 * a_nullmap,
        const ArrayB & b,
        const ColumnUInt8 * b_nullmap,
        ArrayC & c,
        typename ColumnUInt8::Container & res_null,
        NativeResultType scale_a [[maybe_unused]],
        NativeResultType scale_b [[maybe_unused]],
        NativeResultType scale_result [[maybe_unused]])
    {
        size_t size = a.size();

        evaluateNullmap(size, a_nullmap, b_nullmap, res_null);

        if constexpr (is_division)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaled<true>(a[i], b[i], scale_a, res_null[i]);
            return;
        }
        else if constexpr (is_modulo)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(a[i], b[i], scale_a, res_null[i]);
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(a[i], b[i], scale_b, res_null[i]);
            }

            return;
        }

        throw Exception("Should not reach here");
    }

    static void NO_INLINE vectorConstant(
        const ArrayA & a,
        B b,
        ArrayC & c,
        NativeResultType scale_a [[maybe_unused]],
        NativeResultType scale_b [[maybe_unused]],
        NativeResultType scale_result [[maybe_unused]])
    {
        size_t size = a.size();
        if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(a[i], b, scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(a[i], b, scale_b);
                return;
            }
        }
        else if constexpr (is_multiply)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaledMul(a[i], b, scale_result);
            return;
        }
        else if constexpr (is_division)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaled<true>(a[i], b, scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a[i], b);
    }

    static void NO_INLINE vectorConstantNullable(
        const ArrayA & a,
        const ColumnUInt8 * a_nullmap,
        B b,
        ArrayC & c,
        typename ColumnUInt8::Container & res_null,
        NativeResultType scale_a [[maybe_unused]],
        NativeResultType scale_b [[maybe_unused]],
        NativeResultType scale_result [[maybe_unused]])
    {
        size_t size = a.size();

        evaluateNullmap(size, a_nullmap, nullptr, res_null);

        if constexpr (is_division)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaled<true>(a[i], b, scale_a, res_null[i]);
            return;
        }
        else if constexpr (is_modulo)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(a[i], b, scale_a, res_null[i]);
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(a[i], b, scale_b, res_null[i]);
            }

            return;
        }

        throw Exception("Should not reach here");
    }

    static void NO_INLINE constantVector(
        A a,
        const ArrayB & b,
        ArrayC & c,
        NativeResultType scale_a [[maybe_unused]],
        NativeResultType scale_b [[maybe_unused]],
        NativeResultType scale_result [[maybe_unused]])
    {
        size_t size = b.size();
        if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(a, b[i], scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(a, b[i], scale_b);
                return;
            }
        }
        else if constexpr (is_multiply)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaledMul(a, b[i], scale_result);
            return;
        }
        else if constexpr (is_division)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaled<true>(a, b[i], scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a, b[i]);
    }

    static void NO_INLINE constantVectorNullable(
        A a,
        const ArrayB & b,
        const ColumnUInt8 * b_nullmap,
        ArrayC & c,
        typename ColumnUInt8::Container & res_null,
        NativeResultType scale_a [[maybe_unused]],
        NativeResultType scale_b [[maybe_unused]],
        NativeResultType scale_result [[maybe_unused]])
    {
        size_t size = b.size();

        evaluateNullmap(size, nullptr, b_nullmap, res_null);

        if constexpr (is_division)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaled<true>(a, b[i], scale_a, res_null[i]);
            return;
        }
        else if constexpr (is_modulo)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(a, b[i], scale_a, res_null[i]);
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(a, b[i], scale_b, res_null[i]);
            }

            return;
        }

        throw Exception("Should not reach here");
    }

    static ResultType constantConstant(
        A a,
        B b,
        NativeResultType scale_a [[maybe_unused]],
        NativeResultType scale_b [[maybe_unused]],
        NativeResultType scale_result [[maybe_unused]])
    {
        if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
                return applyScaled<true>(a, b, scale_a);
            else if (scale_b != 1)
                return applyScaled<false>(a, b, scale_b);
        }
        else if constexpr (is_multiply)
        {
            return applyScaledMul(a, b, scale_result);
        }
        else if constexpr (is_division)
            return applyScaled<true>(a, b, scale_a);

        return apply(a, b);
    }

    static ResultType constantConstantNullable(
        A a,
        B b,
        NativeResultType scale_a [[maybe_unused]],
        NativeResultType scale_b [[maybe_unused]],
        NativeResultType scale_result [[maybe_unused]],
        UInt8 & res_null)
    {
        if constexpr (is_division)
        {
            return applyScaled<true>(a, b, scale_a, res_null);
        }
        else if constexpr (is_modulo)
        {
            if (scale_a != 1)
                return applyScaled<true>(a, b, scale_a, res_null);
            else
                return applyScaled<false>(a, b, scale_b, res_null);
        }

        throw Exception("Should not reach here");
    }

private:
    static NativeResultType applyScaledMul(NativeResultType a, NativeResultType b, NativeResultType scale)
    {
        if constexpr (is_multiply)
        {
            if constexpr (need_promote_type)
            {
                PromoteResultType res = Op::template apply<PromoteResultType>(a, b);
                res = res / scale;
                if constexpr (check_overflow)
                {
                    if (res > DecimalMaxValue::maxValue())
                    {
                        throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
                    }
                }
                return static_cast<NativeResultType>(res);
            }
            else
            {
                NativeResultType res = Op::template apply<NativeResultType>(a, b);
                res = res / scale;
                return res;
            }
        }
    }

    /// there's implicit type convertion here
    static NativeResultType apply(NativeResultType a, NativeResultType b)
    {
        if constexpr (need_promote_type)
        {
            auto res = Op::template apply<PromoteResultType>(a, b);
            if constexpr (check_overflow)
            {
                if (res > DecimalMaxValue::maxValue())
                {
                    throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
                }
            }
            return static_cast<NativeResultType>(res);
        }
        else
        {
            return Op::template apply<NativeResultType>(a, b);
        }
    }

    template <bool scale_left>
    static NativeResultType applyScaled(InputType a, InputType b, InputType scale)
    {
        if constexpr (is_plus_minus_compare || is_division || is_modulo)
        {
            InputType res;

            if constexpr (scale_left)
                a = a * scale;
            else
                b = b * scale;

            res = Op::template apply<InputType>(a, b);

            if constexpr (check_overflow)
            {
                if (res > DecimalMaxValue::maxValue())
                {
                    throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
                }
            }

            return static_cast<NativeResultType>(res);
        }
    }

    template <bool scale_left>
    static NativeResultType applyScaled(InputType a, InputType b, InputType scale, UInt8 & res_null)
    {
        if constexpr (is_division || is_modulo)
        {
            InputType res;

            if constexpr (scale_left)
                a = a * scale;
            else
                b = b * scale;

            res = Op::template apply<InputType>(a, b, res_null);

            if constexpr (check_overflow)
            {
                if (res > DecimalMaxValue::maxValue())
                {
                    throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
                }
            }

            return static_cast<NativeResultType>(res);
        }
        throw Exception("Should not reach here");
    }
};

template <typename DataType>
constexpr bool IsIntegral = false;
template <>
inline constexpr bool IsIntegral<DataTypeUInt8> = true;
template <>
inline constexpr bool IsIntegral<DataTypeUInt16> = true;
template <>
inline constexpr bool IsIntegral<DataTypeUInt32> = true;
template <>
inline constexpr bool IsIntegral<DataTypeUInt64> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt8> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt16> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt32> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt64> = true;

template <typename DataType>
constexpr bool IsDateOrDateTime = false;
template <>
inline constexpr bool IsDateOrDateTime<DataTypeDate> = true;
template <>
inline constexpr bool IsDateOrDateTime<DataTypeDateTime> = true;

/** Returns appropriate result type for binary operator on dates (or datetimes):
 *  Date + Integral -> Date
 *  Integral + Date -> Date
 *  Date - Date     -> Int32
 *  Date - Integral -> Date
 *  least(Date, Date) -> Date
 *  greatest(Date, Date) -> Date
 *  All other operations are not defined and return InvalidType, operations on
 *  distinct date types are also undefined (e.g. DataTypeDate - DataTypeDateTime)
 */
template <template <typename, typename> class Operation, typename LeftDataType, typename RightDataType>
struct DateBinaryOperationTraits
{
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using Op = Operation<T0, T1>;

    using ResultDataType = If<
        std::is_same_v<Op, PlusImpl<T0, T1>>,
        Then<
            If<IsDateOrDateTime<LeftDataType> && IsIntegral<RightDataType>,
               Then<LeftDataType>,
               Else<
                   If<IsIntegral<LeftDataType> && IsDateOrDateTime<RightDataType>,
                      Then<RightDataType>,
                      Else<InvalidType>>>>>,
        Else<
            If<std::is_same_v<Op, MinusImpl<T0, T1>>,
               Then<
                   If<IsDateOrDateTime<LeftDataType>,
                      Then<
                          If<std::is_same_v<LeftDataType, RightDataType>,
                             Then<DataTypeInt32>,
                             Else<If<IsIntegral<RightDataType>, Then<LeftDataType>, Else<InvalidType>>>>>,
                      Else<InvalidType>>>,
               Else<
                   If<std::is_same_v<T0, T1>
                          && (std::is_same_v<Op, BinaryLeastBaseImpl<T0, T1>>
                              || std::is_same_v<Op, BinaryGreatestBaseImpl<T0, T1>>),
                      Then<LeftDataType>,
                      Else<InvalidType>>>>>>;
};


/// Decides among date and numeric operations
template <template <typename, typename> class Operation, typename LeftDataType, typename RightDataType>
struct BinaryOperationTraits
{
    using ResultDataType = If<
        IsDateOrDateTime<LeftDataType> || IsDateOrDateTime<RightDataType>,
        Then<typename DateBinaryOperationTraits<Operation, LeftDataType, RightDataType>::ResultDataType>,
        Else<typename DataTypeFromFieldType<
            typename Operation<typename LeftDataType::FieldType, typename RightDataType::FieldType>::ResultType>::
                 Type>>;
};


template <template <typename, typename> class Op, typename Name, bool default_impl_for_nulls = true>
class FunctionBinaryArithmetic : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionBinaryArithmetic>(context); }

    explicit FunctionBinaryArithmetic(const Context & context)
        : context(context)
    {}

    bool useDefaultImplementationForNulls() const override { return default_impl_for_nulls; }

private:
    const Context & context;

    template <typename ResultDataType>
    bool checkRightTypeImpl(DataTypePtr & type_res) const
    {
        /// Overload for InvalidType
        if constexpr (std::is_same_v<ResultDataType, InvalidType>)
            return false;
        else
        {
            type_res = std::make_shared<ResultDataType>();
            return true;
        }
    }

    std::pair<PrecType, ScaleType> getPrecAndScale(const IDataType * input_type) const
    {
        const IDataType * type = input_type;
        if constexpr (!default_impl_for_nulls)
        {
            if (const auto * ptr = typeid_cast<const DataTypeNullable *>(input_type))
            {
                type = ptr->getNestedType().get();
            }
        }
        if (const auto * ptr = typeid_cast<const DataTypeDecimal32 *>(type))
        {
            return std::make_pair(ptr->getPrec(), ptr->getScale());
        }
        if (const auto * ptr = typeid_cast<const DataTypeDecimal64 *>(type))
        {
            return std::make_pair(ptr->getPrec(), ptr->getScale());
        }
        if (const auto * ptr = typeid_cast<const DataTypeDecimal128 *>(type))
        {
            return std::make_pair(ptr->getPrec(), ptr->getScale());
        }
        const auto * ptr = typeid_cast<const DataTypeDecimal256 *>(type);
        return std::make_pair(ptr->getPrec(), ptr->getScale());
    }

    template <typename LeftDataType, typename RightDataType>
    DataTypePtr getDecimalReturnType(const DataTypes & arguments) const
    {
        using LeftFieldType = typename LeftDataType::FieldType;
        using RightFieldType = typename RightDataType::FieldType;
        if constexpr (!IsDecimal<typename Op<LeftFieldType, RightFieldType>::ResultType>)
        {
            return std::make_shared<
                typename DataTypeFromFieldType<typename Op<LeftFieldType, RightFieldType>::ResultType>::Type>();
        }
        else
        {
            Int32 div_prec_incr = DEFAULT_DIV_PRECISION_INCREMENT;
            // For some test cases, context won't have DAGContext, use default value
            if likely (context.getDAGContext() != nullptr)
            {
                div_prec_incr = context.getDAGContext()->getDivPrecisionIncrement();
            }
            // Treat integer as a kind of decimal;
            if constexpr (std::is_integral_v<LeftFieldType>)
            {
                PrecType left_prec = IntPrec<LeftFieldType>::prec;
                auto [right_prec, right_scale] = getPrecAndScale(arguments[1].get());
                auto [result_prec, result_scale] = Op<LeftFieldType, RightFieldType>::ResultPrecInferer::infer(
                    left_prec,
                    0,
                    right_prec,
                    right_scale,
                    div_prec_incr);
                return createDecimal(result_prec, result_scale);
            }
            else if constexpr (std::is_integral_v<RightFieldType>)
            {
                ScaleType right_prec = IntPrec<RightFieldType>::prec;
                auto [left_prec, left_scale] = getPrecAndScale(arguments[0].get());
                auto [result_prec, result_scale] = Op<LeftFieldType, RightFieldType>::ResultPrecInferer::infer(
                    left_prec,
                    left_scale,
                    right_prec,
                    0,
                    div_prec_incr);
                return createDecimal(result_prec, result_scale);
            }
            auto [left_prec, left_scale] = getPrecAndScale(arguments[0].get());
            auto [right_prec, right_scale] = getPrecAndScale(arguments[1].get());
            auto [result_prec, result_scale] = Op<LeftFieldType, RightFieldType>::ResultPrecInferer::infer(
                left_prec,
                left_scale,
                right_prec,
                right_scale,
                div_prec_incr);

            return createDecimal(result_prec, result_scale);
        }
    }

    template <typename LeftDataType, typename RightDataType>
    bool checkRightType(const DataTypes & arguments, DataTypePtr & type_res) const
    {
        auto right_type = arguments[1];
        if constexpr (!default_impl_for_nulls)
        {
            right_type = removeNullable(right_type);
        }
        if constexpr (IsDecimal<typename LeftDataType::FieldType> || IsDecimal<typename RightDataType::FieldType>)
        {
            if (typeid_cast<const RightDataType *>(right_type.get()))
            {
                type_res = getDecimalReturnType<LeftDataType, RightDataType>(arguments);
                return true;
            }
            return false;
        }
        else
        {
            using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;

            if (typeid_cast<const RightDataType *>(right_type.get()))
                return checkRightTypeImpl<ResultDataType>(type_res);

            return false;
        }
    }

    template <typename T0>
    bool checkLeftType(const DataTypes & arguments, DataTypePtr & type_res) const
    {
        auto left_type = arguments[0];
        if constexpr (!default_impl_for_nulls)
        {
            left_type = removeNullable(left_type);
        }
        if (typeid_cast<const T0 *>(left_type.get()))
        {
            if (checkRightType<T0, DataTypeDate>(arguments, type_res)
                || checkRightType<T0, DataTypeDateTime>(arguments, type_res)
                || checkRightType<T0, DataTypeUInt8>(arguments, type_res)
                || checkRightType<T0, DataTypeUInt16>(arguments, type_res)
                || checkRightType<T0, DataTypeUInt32>(arguments, type_res)
                || checkRightType<T0, DataTypeUInt64>(arguments, type_res)
                || checkRightType<T0, DataTypeInt8>(arguments, type_res)
                || checkRightType<T0, DataTypeInt16>(arguments, type_res)
                || checkRightType<T0, DataTypeInt32>(arguments, type_res)
                || checkRightType<T0, DataTypeInt64>(arguments, type_res)
                || checkRightType<T0, DataTypeFloat32>(arguments, type_res)
                || checkRightType<T0, DataTypeDecimal32>(arguments, type_res)
                || checkRightType<T0, DataTypeDecimal64>(arguments, type_res)
                || checkRightType<T0, DataTypeDecimal128>(arguments, type_res)
                || checkRightType<T0, DataTypeDecimal256>(arguments, type_res)
                || checkRightType<T0, DataTypeFloat64>(arguments, type_res))
                return true;
        }
        return false;
    }

    FunctionBuilderPtr getFunctionForIntervalArithmetic(const DataTypePtr & type0, const DataTypePtr & type1) const
    {
        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        /// We construct another function (example: addMonths) and call it.

        constexpr bool function_is_plus = IsOperation<Op>::plus;
        constexpr bool function_is_minus = IsOperation<Op>::minus;

        if (!function_is_plus && !function_is_minus)
            return {};

        int interval_arg = 1;
        /// do not check null type because only divide op may use non-default-impl for nulls
        const auto * interval_data_type = checkAndGetDataType<DataTypeInterval>(type1.get());
        if (!interval_data_type)
        {
            interval_arg = 0;
            interval_data_type = checkAndGetDataType<DataTypeInterval>(type0.get());
        }
        if (!interval_data_type)
            return {};

        if (interval_arg == 0 && function_is_minus)
            throw Exception(
                fmt::format(
                    "Wrong order of arguments for function {}: argument of type Interval cannot be first.",
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypeDate * date_data_type
            = checkAndGetDataType<DataTypeDate>(interval_arg == 0 ? type1.get() : type0.get());
        const DataTypeDateTime * date_time_data_type = nullptr;
        if (!date_data_type)
        {
            date_time_data_type = checkAndGetDataType<DataTypeDateTime>(interval_arg == 0 ? type1.get() : type0.get());
            if (!date_time_data_type)
                throw Exception(
                    fmt::format(
                        "Wrong argument types for function {}: if one argument is Interval, then another must be Date "
                        "or DateTime.",
                        getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return FunctionFactory::instance().get(
            fmt::format("{}{}s", (function_is_plus ? "add" : "subtract"), interval_data_type->kindToString()),
            context);
    }

public:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        if (auto function_builder = getFunctionForIntervalArithmetic(arguments[0], arguments[1]))
        {
            ColumnsWithTypeAndName new_arguments(2);

            for (size_t i = 0; i < 2; ++i)
                new_arguments[i].type = arguments[i];

            /// Interval argument must be second.
            if (checkDataType<DataTypeInterval>(new_arguments[0].type.get()))
                std::swap(new_arguments[0], new_arguments[1]);

            /// Change interval argument to its representation
            new_arguments[1].type
                = std::make_shared<typename DataTypeFromFieldType<DataTypeInterval::FieldType>::Type>();

            auto function = function_builder->build(new_arguments);
            return function->getReturnType();
        }

        DataTypePtr type_res;
        if constexpr (!default_impl_for_nulls)
        {
            /// if one of the input is null constant, return null constant
            const auto * left_null_type = typeid_cast<const DataTypeNullable *>(arguments[0].get());
            bool left_null_const = left_null_type != nullptr && left_null_type->onlyNull();
            if (left_null_const)
                type_res = arguments[0];
            const auto * right_null_type = typeid_cast<const DataTypeNullable *>(arguments[1].get());
            bool right_null_const = right_null_type != nullptr && right_null_type->onlyNull();
            if (right_null_const)
                type_res = arguments[1];
            if (left_null_const || right_null_const)
                return type_res;
        }

        if (!(checkLeftType<DataTypeDate>(arguments, type_res) || checkLeftType<DataTypeDateTime>(arguments, type_res)
              || checkLeftType<DataTypeUInt8>(arguments, type_res) || checkLeftType<DataTypeUInt16>(arguments, type_res)
              || checkLeftType<DataTypeUInt32>(arguments, type_res)
              || checkLeftType<DataTypeUInt64>(arguments, type_res) || checkLeftType<DataTypeInt8>(arguments, type_res)
              || checkLeftType<DataTypeInt16>(arguments, type_res) || checkLeftType<DataTypeInt32>(arguments, type_res)
              || checkLeftType<DataTypeInt64>(arguments, type_res)
              || checkLeftType<DataTypeDecimal<Decimal32>>(arguments, type_res)
              || checkLeftType<DataTypeDecimal<Decimal64>>(arguments, type_res)
              || checkLeftType<DataTypeDecimal<Decimal128>>(arguments, type_res)
              || checkLeftType<DataTypeDecimal<Decimal256>>(arguments, type_res)
              || checkLeftType<DataTypeFloat32>(arguments, type_res)
              || checkLeftType<DataTypeFloat64>(arguments, type_res)))
            throw Exception(
                fmt::format(
                    "Illegal types {} and {} of arguments of function {}",
                    arguments[0]->getName(),
                    arguments[1]->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if constexpr (!default_impl_for_nulls)
            type_res = makeNullable(type_res);
        return type_res;
    }

    template <typename F>
    bool castType(const IDataType * type, F && f) const
    {
        return castTypeToEither<
            DataTypeUInt8,
            DataTypeUInt16,
            DataTypeUInt32,
            DataTypeUInt64,
            DataTypeInt8,
            DataTypeInt16,
            DataTypeInt32,
            DataTypeInt64,
            DataTypeFloat32,
            DataTypeFloat64,
            DataTypeDate,
            DataTypeDateTime,
            DataTypeDecimal32,
            DataTypeDecimal64,
            DataTypeDecimal128,
            DataTypeDecimal256>(type, std::forward<F>(f));
    }

    template <typename F>
    bool castBothTypes(DataTypePtr left, DataTypePtr right, DataTypePtr result, F && f) const
    {
        return castType(left.get(), [&](const auto & left_, bool is_left_nullable_) {
            return castType(right.get(), [&](const auto & right_, bool is_right_nullable_) {
                return castType(result.get(), [&](const auto & result_, bool) {
                    return f(left_, is_left_nullable_, right_, is_right_nullable_, result_);
                });
            });
        });
    }

    template <typename A, typename B, bool check = IsDecimal<A>>
    struct RefineCls;

    template <typename T, typename ResultType>
    struct RefineCls<T, ResultType, true>
    {
        using Type = If<std::is_floating_point_v<ResultType>, ResultType, typename T::NativeType>;
    };


    template <typename T, typename ResultType>
    struct RefineCls<T, ResultType, false>
    {
        using Type = T;
    };

    template <typename A, typename B>
    using Refine = typename RefineCls<A, B>::Type;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        if (auto function_builder = getFunctionForIntervalArithmetic(
                block.getByPosition(arguments[0]).type,
                block.getByPosition(arguments[1]).type))
        {
            ColumnNumbers new_arguments = arguments;

            /// Interval argument must be second.
            if (checkDataType<DataTypeInterval>(block.getByPosition(arguments[0]).type.get()))
                std::swap(new_arguments[0], new_arguments[1]);

            /// Change interval argument type to its representation
            Block new_block = block;
            new_block.getByPosition(new_arguments[1]).type
                = std::make_shared<typename DataTypeFromFieldType<DataTypeInterval::FieldType>::Type>();

            ColumnsWithTypeAndName new_arguments_with_type_and_name
                = {new_block.getByPosition(new_arguments[0]), new_block.getByPosition(new_arguments[1])};
            auto function = function_builder->build(new_arguments_with_type_and_name);

            function->execute(new_block, new_arguments, result);
            block.getByPosition(result).column = new_block.getByPosition(result).column;

            return;
        }

        auto left_generic = block.getByPosition(arguments[0]).type;
        auto right_generic = block.getByPosition(arguments[1]).type;
        DataTypes types;
        types.push_back(left_generic);
        types.push_back(right_generic);
        DataTypePtr result_type = getReturnTypeImpl(types);
        if constexpr (!default_impl_for_nulls)
        {
            if (result_type->onlyNull())
            {
                block.getByPosition(result).column = result_type->createColumnConst(block.rows(), Null());
                return;
            }
        }
        bool valid = castBothTypes(
            left_generic,
            right_generic,
            result_type,
            [&](const auto & left,
                bool is_left_nullable [[maybe_unused]],
                const auto & right,
                bool is_right_nullable [[maybe_unused]],
                const auto & result_type) {
                using LeftDataType = std::decay_t<decltype(left)>;
                using RightDataType = std::decay_t<decltype(right)>;
                using ResultDataType = std::decay_t<decltype(result_type)>;
                constexpr bool result_is_decimal = IsDecimal<typename ResultDataType::FieldType>;
                constexpr bool is_multiply [[maybe_unused]] = IsOperation<Op>::multiply;
                constexpr bool is_division [[maybe_unused]] = IsOperation<Op>::div_floating || IsOperation<Op>::div_int;

                using T0 = typename LeftDataType::FieldType;
                using T1 = typename RightDataType::FieldType;
                using ResultType = typename ResultDataType::FieldType;
                using ExpectedResultType = typename Op<T0, T1>::ResultType;
                if constexpr ((!IsDecimal<ResultType>
                               || !IsDecimal<ExpectedResultType>)&&!std::is_same_v<ResultType, ExpectedResultType>)
                {
                    return false;
                }
                else if constexpr (!std::is_same_v<ResultDataType, InvalidType>)
                {
                    using ColVecT0 = std::conditional_t<IsDecimal<T0>, ColumnDecimal<T0>, ColumnVector<T0>>;
                    using ColVecT1 = std::conditional_t<IsDecimal<T1>, ColumnDecimal<T1>, ColumnVector<T1>>;
                    using ColVecResult = std::conditional_t<
                        IsDecimal<ResultType>,
                        ColumnDecimal<ResultType>,
                        ColumnVector<typename Op<T0, T1>::ResultType>>;

                    /// Only for arithmatic operator
                    using T0_ = Refine<T0, ResultType>;
                    using T1_ = Refine<T1, ResultType>;
                    using FieldT0 = typename NearestFieldType<T0>::Type;
                    using FieldT1 = typename NearestFieldType<T1>::Type;
                    /// Decimal operations need scale. Operations are on result type.
                    using OpImpl = std::conditional_t<
                        result_is_decimal,
                        DecimalBinaryOperation<T0, T1, Op, ResultType>,
                        BinaryOperationImpl<T0, T1, Op<T0_, T1_>, typename Op<T0, T1>::ResultType>>;

                    const auto * col_left_raw = block.getByPosition(arguments[0]).column.get();
                    const auto * col_right_raw = block.getByPosition(arguments[1]).column.get();
                    const ColumnUInt8 * col_left_nullmap [[maybe_unused]] = nullptr;
                    const ColumnUInt8 * col_right_nullmap [[maybe_unused]] = nullptr;
                    bool is_left_null_constant [[maybe_unused]] = false;
                    bool is_right_null_constant [[maybe_unused]] = false;
                    DataTypePtr nullable_result_type [[maybe_unused]] = nullptr;
                    if constexpr (result_is_decimal)
                    {
                        nullable_result_type = makeNullable(
                            std::make_shared<ResultDataType>(result_type.getPrec(), result_type.getScale()));
                    }
                    else
                    {
                        nullable_result_type = makeNullable(std::make_shared<ResultDataType>());
                    }

                    if constexpr (!default_impl_for_nulls)
                    {
                        if (is_left_nullable)
                        {
                            if (const auto * col_nullable = typeid_cast<const ColumnNullable *>(col_left_raw))
                            {
                                col_left_nullmap = &col_nullable->getNullMapColumn();
                                col_left_raw = &col_nullable->getNestedColumn();
                            }
                            else if (const auto * col_const = typeid_cast<const ColumnConst *>(col_left_raw))
                            {
                                if (col_const->isNullAt(0))
                                    is_left_null_constant = true;
                            }
                            else
                            {
                                return false;
                            }
                        }
                        if (is_right_nullable)
                        {
                            if (const auto * col_nullable = typeid_cast<const ColumnNullable *>(col_right_raw))
                            {
                                col_right_nullmap = &col_nullable->getNullMapColumn();
                                col_right_raw = &col_nullable->getNestedColumn();
                            }
                            else if (const auto * col_const = typeid_cast<const ColumnConst *>(col_right_raw))
                            {
                                if (col_const->isNullAt(0))
                                    is_right_null_constant = true;
                            }
                            else
                            {
                                return false;
                            }
                        }
                        if (is_left_null_constant || is_right_null_constant)
                        {
                            /// if one of the input is null constant, just return null constant
                            block.getByPosition(result).column
                                = nullable_result_type->createColumnConst(col_left_raw->size(), Null());
                            return true;
                        }
                    }

                    if (auto col_left = checkAndGetColumnConst<ColVecT0>(col_left_raw, is_left_nullable))
                    {
                        if (auto col_right = checkAndGetColumnConst<ColVecT1>(col_right_raw, is_right_nullable))
                        {
                            /// the only case with a non-vector result
                            if constexpr (result_is_decimal)
                            {
                                auto [scale_a, scale_b, scale_result]
                                    = result_type.getScales(left, right, is_multiply, is_division);

                                if constexpr (default_impl_for_nulls)
                                {
                                    auto res = OpImpl::constantConstant(
                                        col_left->template getValue<T0>(),
                                        col_right->template getValue<T1>(),
                                        scale_a,
                                        scale_b,
                                        scale_result);
                                    block.getByPosition(result).column
                                        = ResultDataType(result_type.getPrec(), result_type.getScale())
                                              .createColumnConst(
                                                  col_left->size(),
                                                  toField(res, result_type.getScale()));
                                }
                                else
                                {
                                    UInt8 res_null = false;
                                    Field result_field = Null();
                                    auto res = OpImpl::constantConstantNullable(
                                        col_left->template getValue<T0>(),
                                        col_right->template getValue<T1>(),
                                        scale_a,
                                        scale_b,
                                        scale_result,
                                        res_null);
                                    if (!res_null)
                                        result_field = toField(res, result_type.getScale());
                                    block.getByPosition(result).column
                                        = nullable_result_type->createColumnConst(col_left->size(), result_field);
                                }
                            }
                            else
                            {
                                if constexpr (default_impl_for_nulls)
                                {
                                    auto res = OpImpl::constantConstant(
                                        col_left->getField().template safeGet<FieldT0>(),
                                        col_right->getField().template safeGet<FieldT1>());
                                    block.getByPosition(result).column
                                        = ResultDataType().createColumnConst(col_left->size(), toField(res));
                                }
                                else
                                {
                                    UInt8 res_null = false;
                                    Field result_field = Null();
                                    auto res = OpImpl::constantConstantNullable(
                                        col_left->getField().template safeGet<FieldT0>(),
                                        col_right->getField().template safeGet<FieldT1>(),
                                        res_null);
                                    if (!res_null)
                                        result_field = toField(res);
                                    block.getByPosition(result).column
                                        = nullable_result_type->createColumnConst(col_left->size(), result_field);
                                }
                            }
                            return true;
                        }
                    }

                    typename ColVecResult::MutablePtr col_res = nullptr;
                    if constexpr (result_is_decimal)
                    {
                        col_res = ColVecResult::create(0, result_type.getScale());
                    }
                    else
                        col_res = ColVecResult::create();

                    auto & vec_res = col_res->getData();
                    vec_res.resize(block.rows());

                    typename ColumnUInt8::MutablePtr res_nullmap = ColumnUInt8::create();
                    typename ColumnUInt8::Container & vec_res_nulmap = res_nullmap->getData();
                    if constexpr (!default_impl_for_nulls)
                    {
                        vec_res_nulmap.assign(block.rows(), static_cast<UInt8>(0));
                    }

                    if (auto col_left_const = checkAndGetColumnConst<ColVecT0>(col_left_raw, is_left_nullable))
                    {
                        if (auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw))
                        {
                            if constexpr (result_is_decimal)
                            {
                                auto [scale_a, scale_b, scale_result]
                                    = result_type.getScales(left, right, is_multiply, is_division);
                                if constexpr (default_impl_for_nulls)
                                {
                                    OpImpl::constantVector(
                                        col_left_const->template getValue<T0>(),
                                        col_right->getData(),
                                        vec_res,
                                        scale_a,
                                        scale_b,
                                        scale_result);
                                }
                                else
                                {
                                    OpImpl::constantVectorNullable(
                                        col_left_const->template getValue<T0>(),
                                        col_right->getData(),
                                        col_right_nullmap,
                                        vec_res,
                                        vec_res_nulmap,
                                        scale_a,
                                        scale_b,
                                        scale_result);
                                }
                            }
                            else
                            {
                                if constexpr (default_impl_for_nulls)
                                {
                                    OpImpl::constantVector(
                                        col_left_const->getField().template safeGet<FieldT0>(),
                                        col_right->getData(),
                                        vec_res);
                                }
                                else
                                {
                                    OpImpl::constantVectorNullable(
                                        col_left_const->getField().template safeGet<FieldT0>(),
                                        col_right->getData(),
                                        col_right_nullmap,
                                        vec_res,
                                        vec_res_nulmap);
                                }
                            }
                        }
                        else
                            return false;
                    }
                    else if (auto col_left = checkAndGetColumn<ColVecT0>(col_left_raw))
                    {
                        if (auto col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_raw, is_right_nullable))
                        {
                            if constexpr (result_is_decimal)
                            {
                                auto [scale_a, scale_b, scale_result]
                                    = result_type.getScales(left, right, is_multiply, is_division);
                                if constexpr (default_impl_for_nulls)
                                {
                                    OpImpl::vectorConstant(
                                        col_left->getData(),
                                        col_right_const->template getValue<T1>(),
                                        vec_res,
                                        scale_a,
                                        scale_b,
                                        scale_result);
                                }
                                else
                                {
                                    OpImpl::vectorConstantNullable(
                                        col_left->getData(),
                                        col_left_nullmap,
                                        col_right_const->template getValue<T1>(),
                                        vec_res,
                                        vec_res_nulmap,
                                        scale_a,
                                        scale_b,
                                        scale_result);
                                }
                            }
                            else
                            {
                                if constexpr (default_impl_for_nulls)
                                {
                                    OpImpl::vectorConstant(
                                        col_left->getData(),
                                        col_right_const->getField().template safeGet<FieldT1>(),
                                        vec_res);
                                }
                                else
                                {
                                    OpImpl::vectorConstantNullable(
                                        col_left->getData(),
                                        col_left_nullmap,
                                        col_right_const->getField().template safeGet<FieldT1>(),
                                        vec_res,
                                        vec_res_nulmap);
                                }
                            }
                        }
                        else if (auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw))
                        {
                            if constexpr (result_is_decimal)
                            {
                                auto [scale_a, scale_b, scale_result]
                                    = result_type.getScales(left, right, is_multiply, is_division);
                                if constexpr (default_impl_for_nulls)
                                {
                                    OpImpl::vectorVector(
                                        col_left->getData(),
                                        col_right->getData(),
                                        vec_res,
                                        scale_a,
                                        scale_b,
                                        scale_result);
                                }
                                else
                                {
                                    OpImpl::vectorVectorNullable(
                                        col_left->getData(),
                                        col_left_nullmap,
                                        col_right->getData(),
                                        col_right_nullmap,
                                        vec_res,
                                        vec_res_nulmap,
                                        scale_a,
                                        scale_b,
                                        scale_result);
                                }
                            }
                            else
                            {
                                if constexpr (default_impl_for_nulls)
                                {
                                    OpImpl::vectorVector(col_left->getData(), col_right->getData(), vec_res);
                                }
                                else
                                {
                                    OpImpl::vectorVectorNullable(
                                        col_left->getData(),
                                        col_left_nullmap,
                                        col_right->getData(),
                                        col_right_nullmap,
                                        vec_res,
                                        vec_res_nulmap);
                                }
                            }
                        }
                        else
                            return false;
                    }
                    else
                        return false;

                    if constexpr (default_impl_for_nulls)
                    {
                        block.getByPosition(result).column = std::move(col_res);
                    }
                    else
                    {
                        block.getByPosition(result).column
                            = ColumnNullable::create(std::move(col_res), std::move(res_nullmap));
                    }
                    return true;
                }
                return false;
            });
        if (!valid)
            throw Exception(
                fmt::format("{}'s arguments do not match the expected data types", getName()),
                ErrorCodes::LOGICAL_ERROR);
    }
};

} // namespace DB