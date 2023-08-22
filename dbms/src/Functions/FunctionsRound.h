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

#include <Common/Decimal.h>
#include <Common/TiFlashException.h>
#include <Common/toSafeUnsigned.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <IO/WriteHelpers.h>

#include <array>
#include <cfenv>
#include <cmath>
#include <ext/bit_cast.h>
#include <type_traits>

#if __SSE4_1__
#include <smmintrin.h>
#endif

#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int OVERFLOW_ERROR;
} // namespace ErrorCodes


/** Rounding Functions:
    * round(x, N) - rounding to nearest (N = 0 by default). Use banker's rounding for floating point numbers.
    * floor(x, N) is the largest number <= x (N = 0 by default).
    * ceil(x, N) is the smallest number >= x (N = 0 by default).
    * trunc(x, N) - is the largest by absolute value number that is not greater than x by absolute value (N = 0 by default).
    *
    * The value of the parameter N (scale):
    * - N > 0: round to the number with N decimal places after the decimal point
    * - N < 0: round to an integer with N zero characters
    * - N = 0: round to an integer
    *
    * Type of the result is the type of argument.
    * For integer arguments, when passing negative scale, overflow can occur.
    * In that case, the behavior is implementation specific.
    *
    * roundToExp2 - down to the nearest power of two (see below);
    *
    * Deprecated functions:
    * roundDuration - down to the nearest of: 0, 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000;
    * roundAge - down to the nearest of: 0, 18, 25, 35, 45, 55.
    */

template <typename T>
inline std::enable_if_t<std::is_integral_v<T> && (sizeof(T) <= sizeof(UInt32)), T> roundDownToPowerOfTwo(T x)
{
    return x <= 0 ? 0 : (T(1) << (31 - __builtin_clz(x)));
}

template <typename T>
inline std::enable_if_t<std::is_integral_v<T> && (sizeof(T) == sizeof(UInt64)), T> roundDownToPowerOfTwo(T x)
{
    return x <= 0 ? 0 : (T(1) << (63 - __builtin_clzll(x)));
}

template <typename T>
inline std::enable_if_t<std::is_same_v<T, Float32>, T> roundDownToPowerOfTwo(T x)
{
    return ext::bit_cast<T>(ext::bit_cast<UInt32>(x) & ~((1ULL << 23) - 1));
}

template <typename T>
inline std::enable_if_t<std::is_same_v<T, Float64>, T> roundDownToPowerOfTwo(T x)
{
    return ext::bit_cast<T>(ext::bit_cast<UInt64>(x) & ~((1ULL << 52) - 1));
}

template <typename T>
inline T roundDownToPowerOfTwo(Decimal<T>)
{
    throw Exception("have not implement roundDownToPowerOfTwo of type Decimal.");
}

/** For integer data types:
  * - if number is greater than zero, round it down to nearest power of two (example: roundToExp2(100) = 64, roundToExp2(64) = 64);
  * - otherwise, return 0.
  *
  * For floating point data types: zero out mantissa, but leave exponent.
  * - if number is greater than zero, round it down to nearest power of two (example: roundToExp2(3) = 2);
  * - negative powers are also used (example: roundToExp2(0.7) = 0.5);
  * - if number is zero, return zero;
  * - if number is less than zero, the result is symmetrical: roundToExp2(x) = -roundToExp2(-x). (example: roundToExp2(-0.3) = -0.25);
  */

template <typename T>
struct RoundToExp2Impl
{
    using ResultType = T;

    static inline T apply(T x) { return roundDownToPowerOfTwo<T>(x); }
};


template <typename A>
struct RoundDurationImpl
{
    using ResultType = UInt16;

    static inline ResultType apply(A x)
    {
        // clang-format off
        return x < 1 ? 0
        : (x < 10 ? 1
        : (x < 30 ? 10
        : (x < 60 ? 30
        : (x < 120 ? 60
        : (x < 180 ? 120
        : (x < 240 ? 180
        : (x < 300 ? 240
        : (x < 600 ? 300
        : (x < 1200 ? 600
        : (x < 1800 ? 1200
        : (x < 3600 ? 1800
        : (x < 7200 ? 3600
        : (x < 18000 ? 7200
        : (x < 36000 ? 18000
        : 36000))))))))))))));
        // clang-format on
    }
};

template <typename T>
struct RoundDurationImpl<Decimal<T>>
{
    using ResultType = UInt16;
    static inline ResultType apply(Decimal<T>) { throw Exception("RoundDuration of decimal is not implemented yet."); }
};

template <typename A>
struct RoundAgeImpl
{
    using ResultType = UInt8;

    static inline ResultType apply(A x)
    {
        // clang-format off
        return x < 1 ? 0
        : (x < 18 ? 17
        : (x < 25 ? 18
        : (x < 35 ? 25
        : (x < 45 ? 35
        : (x < 55 ? 45
        : 55)))));
        // clang-format on
    }
};

template <typename T>
struct RoundAgeImpl<Decimal<T>>
{
    using ResultType = UInt8;
    static inline ResultType apply(Decimal<T>) { throw Exception("RoundAge of decimal is not implemented yet."); }
};

/** This parameter controls the behavior of the rounding functions.
  */
enum class ScaleMode
{
    Positive, // round to a number with N decimal places after the decimal point
    Negative, // round to an integer with N zero characters
    Zero, // round to an integer
};

enum class RoundingMode
{
#if __SSE4_1__
    Round = _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC,
    Floor = _MM_FROUND_TO_NEG_INF | _MM_FROUND_NO_EXC,
    Ceil = _MM_FROUND_TO_POS_INF | _MM_FROUND_NO_EXC,
    Trunc = _MM_FROUND_TO_ZERO | _MM_FROUND_NO_EXC,
#else
    Round = 8, /// Values are correspond to above just in case.
    Floor = 9,
    Ceil = 10,
    Trunc = 11,
#endif
};

/** Rounding functions for decimal values
 */

template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode, typename OutputType>
struct DecimalRoundingComputation
{
    static_assert(IsDecimal<T>);
    static const size_t data_count = 1;
    static size_t prepare(size_t scale) { return scale; }
    // compute need decimal_scale to interpret decimals
    static inline void compute(
        const T * __restrict in,
        size_t scale,
        OutputType * __restrict out,
        ScaleType decimal_scale)
    {
        static_assert(std::is_same_v<T, OutputType> || std::is_same_v<OutputType, Int64>);
        Float64 val = in->template toFloat<Float64>(decimal_scale);

        if constexpr (scale_mode == ScaleMode::Positive)
        {
            val = val * scale;
        }
        else if constexpr (scale_mode == ScaleMode::Negative)
        {
            val = val / scale;
        }

        if constexpr (rounding_mode == RoundingMode::Round)
        {
            val = round(val);
        }
        else if constexpr (rounding_mode == RoundingMode::Floor)
        {
            val = floor(val);
        }
        else if constexpr (rounding_mode == RoundingMode::Ceil)
        {
            val = ceil(val);
        }
        else if constexpr (rounding_mode == RoundingMode::Trunc)
        {
            val = trunc(val);
        }


        if constexpr (scale_mode == ScaleMode::Positive)
        {
            val = val / scale;
        }
        else if constexpr (scale_mode == ScaleMode::Negative)
        {
            val = val * scale;
        }

        if constexpr (std::is_same_v<T, OutputType>)
        {
            *out = ToDecimal<Float64, T>(val, decimal_scale);
        }
        else if constexpr (std::is_same_v<OutputType, Int64>)
        {
            *out = static_cast<Int64>(val);
        }
        else
        {
            ; // never arrived here
        }
    }
};


/** Rounding functions for integer values.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct IntegerRoundingComputation
{
    static const size_t data_count = 1;

    static size_t prepare(size_t scale) { return scale; }

    static ALWAYS_INLINE T computeImpl(T x, T scale)
    {
        if constexpr (rounding_mode == RoundingMode::Trunc)
        {
            return x / scale * scale;
        }
        else if constexpr (rounding_mode == RoundingMode::Floor)
        {
            if (x < 0)
                x -= scale - 1;
            return x / scale * scale;
        }
        else if constexpr (rounding_mode == RoundingMode::Ceil)
        {
            if (x >= 0)
                x += scale - 1;
            return x / scale * scale;
        }
        else if constexpr (rounding_mode == RoundingMode::Round)
        {
            bool negative = x < 0;
            if (negative)
                x = -x;
            x = (x + scale / 2) / scale * scale;
            if (negative)
                x = -x;
            return x;
        }
    }

    static ALWAYS_INLINE T compute(T x, T scale)
    {
        if constexpr (scale_mode == ScaleMode::Zero)
        {
            (void)scale;
            return x;
        }
        else if constexpr (scale_mode == ScaleMode::Positive)
        {
            (void)scale;
            return x;
        }
        else if constexpr (scale_mode == ScaleMode::Negative)
        {
            return computeImpl(x, scale);
        }
    }

    static ALWAYS_INLINE void compute(const T * __restrict in, size_t scale, T * __restrict out)
    {
        *out = compute(*in, scale);
    }
};


#if __SSE4_1__

template <typename T>
class BaseFloatRoundingComputation;

template <>
class BaseFloatRoundingComputation<Float32>
{
public:
    using ScalarType = Float32;
    using VectorType = __m128;
    static const size_t data_count = 4;

    static VectorType load(const ScalarType * in) { return _mm_loadu_ps(in); }
    static VectorType load1(const ScalarType in) { return _mm_load1_ps(&in); }
    static void store(ScalarType * out, VectorType val) { _mm_storeu_ps(out, val); }
    static VectorType multiply(VectorType val, VectorType scale) { return _mm_mul_ps(val, scale); }
    static VectorType divide(VectorType val, VectorType scale) { return _mm_div_ps(val, scale); }
    template <RoundingMode mode>
    static VectorType apply(VectorType val)
    {
        return _mm_round_ps(val, int(mode));
    }

    static VectorType prepare(size_t scale) { return load1(scale); }
};

template <>
class BaseFloatRoundingComputation<Float64>
{
public:
    using ScalarType = Float64;
    using VectorType = __m128d;
    static const size_t data_count = 2;

    static VectorType load(const ScalarType * in) { return _mm_loadu_pd(in); }
    static VectorType load1(const ScalarType in) { return _mm_load1_pd(&in); }
    static void store(ScalarType * out, VectorType val) { _mm_storeu_pd(out, val); }
    static VectorType multiply(VectorType val, VectorType scale) { return _mm_mul_pd(val, scale); }
    static VectorType divide(VectorType val, VectorType scale) { return _mm_div_pd(val, scale); }
    template <RoundingMode mode>
    static VectorType apply(VectorType val)
    {
        return _mm_round_pd(val, int(mode));
    }

    static VectorType prepare(size_t scale) { return load1(scale); }
};

#else

/// Implementation for ARM. Not vectorized.

inline float roundWithMode(float x, RoundingMode mode)
{
    switch (mode)
    {
    case RoundingMode::Round:
        return roundf(x);
    case RoundingMode::Floor:
        return floorf(x);
    case RoundingMode::Ceil:
        return ceilf(x);
    case RoundingMode::Trunc:
        return truncf(x);
    default:
        throw Exception(
            "Logical error: unexpected 'mode' parameter passed to function roundWithMode",
            ErrorCodes::LOGICAL_ERROR);
    }
}

inline double roundWithMode(double x, RoundingMode mode)
{
    switch (mode)
    {
    case RoundingMode::Round:
        return round(x);
    case RoundingMode::Floor:
        return floor(x);
    case RoundingMode::Ceil:
        return ceil(x);
    case RoundingMode::Trunc:
        return trunc(x);
    default:
        throw Exception(
            "Logical error: unexpected 'mode' parameter passed to function roundWithMode",
            ErrorCodes::LOGICAL_ERROR);
    }
}

template <typename T>
class BaseFloatRoundingComputation
{
public:
    using ScalarType = T;
    using VectorType = T;
    static const size_t data_count = 1;

    static VectorType load(const ScalarType * in) { return *in; }
    static VectorType load1(const ScalarType in) { return in; }
    static VectorType store(ScalarType * out, ScalarType val) { return *out = val; }
    static VectorType multiply(VectorType val, VectorType scale) { return val * scale; }
    static VectorType divide(VectorType val, VectorType scale) { return val / scale; }
    template <RoundingMode mode>
    static VectorType apply(VectorType val)
    {
        return roundWithMode(val, mode);
    }

    static VectorType prepare(size_t scale) { return load1(scale); }
};

#endif


/** Implementation of low-level round-off functions for floating-point values.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
class FloatRoundingComputation : public BaseFloatRoundingComputation<T>
{
    using Base = BaseFloatRoundingComputation<T>;

public:
    static inline void compute(const T * __restrict in, const typename Base::VectorType & scale, T * __restrict out)
    {
        auto val = Base::load(in);

        if (scale_mode == ScaleMode::Positive)
            val = Base::multiply(val, scale);
        else if (scale_mode == ScaleMode::Negative)
            val = Base::divide(val, scale);

        val = Base::template apply<rounding_mode>(val);

        if (scale_mode == ScaleMode::Positive)
            val = Base::divide(val, scale);
        else if (scale_mode == ScaleMode::Negative)
            val = Base::multiply(val, scale);

        Base::store(out, val);
    }
};


/** Implementing high-level rounding functions.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct FloatRoundingImpl
{
private:
    using Op = FloatRoundingComputation<T, rounding_mode, scale_mode>;
    using Data = std::array<T, Op::data_count>;

public:
    static NO_INLINE void apply(const PaddedPODArray<T> & in, size_t scale, typename ColumnVector<T>::Container & out)
    {
        auto mm_scale = Op::prepare(scale);

        const size_t data_count = std::tuple_size<Data>();

        const T * end_in = in.data() + in.size();
        const T * limit = in.data() + in.size() / data_count * data_count;

        const T * __restrict p_in = in.data();
        T * __restrict p_out = out.data();

        while (p_in < limit)
        {
            Op::compute(p_in, mm_scale, p_out);
            p_in += data_count;
            p_out += data_count;
        }

        if (p_in < end_in)
        {
            Data tmp_src{{}};
            Data tmp_dst;

            size_t tail_size_bytes = (end_in - p_in) * sizeof(*p_in);

            memcpy(&tmp_src, p_in, tail_size_bytes);
            Op::compute(reinterpret_cast<T *>(&tmp_src), mm_scale, reinterpret_cast<T *>(&tmp_dst));
            memcpy(p_out, &tmp_dst, tail_size_bytes);
        }
    }
};

template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct IntegerRoundingImpl
{
private:
    using Op = IntegerRoundingComputation<T, rounding_mode, scale_mode>;
    using Data = T;

public:
    template <size_t scale>
    static NO_INLINE void applyImpl(const PaddedPODArray<T> & in, typename ColumnVector<T>::Container & out)
    {
        const T * end_in = in.data() + in.size();

        const T * __restrict p_in = in.data();
        T * __restrict p_out = out.data();

        while (p_in < end_in)
        {
            Op::compute(p_in, scale, p_out);
            ++p_in;
            ++p_out;
        }
    }

    static NO_INLINE void apply(const PaddedPODArray<T> & in, size_t scale, typename ColumnVector<T>::Container & out)
    {
        /// Manual function cloning for compiler to generate integer division by constant.
        switch (scale)
        {
        case 1ULL:
            return applyImpl<1ULL>(in, out);
        case 10ULL:
            return applyImpl<10ULL>(in, out);
        case 100ULL:
            return applyImpl<100ULL>(in, out);
        case 1000ULL:
            return applyImpl<1000ULL>(in, out);
        case 10000ULL:
            return applyImpl<10000ULL>(in, out);
        case 100000ULL:
            return applyImpl<100000ULL>(in, out);
        case 1000000ULL:
            return applyImpl<1000000ULL>(in, out);
        case 10000000ULL:
            return applyImpl<10000000ULL>(in, out);
        case 100000000ULL:
            return applyImpl<100000000ULL>(in, out);
        case 1000000000ULL:
            return applyImpl<1000000000ULL>(in, out);
        case 10000000000ULL:
            return applyImpl<10000000000ULL>(in, out);
        case 100000000000ULL:
            return applyImpl<100000000000ULL>(in, out);
        case 1000000000000ULL:
            return applyImpl<1000000000000ULL>(in, out);
        case 10000000000000ULL:
            return applyImpl<10000000000000ULL>(in, out);
        case 100000000000000ULL:
            return applyImpl<100000000000000ULL>(in, out);
        case 1000000000000000ULL:
            return applyImpl<1000000000000000ULL>(in, out);
        case 10000000000000000ULL:
            return applyImpl<10000000000000000ULL>(in, out);
        case 100000000000000000ULL:
            return applyImpl<100000000000000000ULL>(in, out);
        case 1000000000000000000ULL:
            return applyImpl<1000000000000000000ULL>(in, out);
        case 10000000000000000000ULL:
            return applyImpl<10000000000000000000ULL>(in, out);
        default:
            throw Exception(
                "Logical error: unexpected 'scale' parameter passed to function IntegerRoundingComputation::compute",
                ErrorCodes::LOGICAL_ERROR);
        }
    }
};

template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode, typename OutputType>
struct DecimalRoundingImpl;

// We may need round decimal to other types, currently the only choice is Int64
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct DecimalRoundingImpl<T, rounding_mode, scale_mode, Int64>
{
private:
    using Op = DecimalRoundingComputation<T, rounding_mode, scale_mode, Int64>;
    using Data = T;

public:
    static NO_INLINE void apply(
        const DecimalPaddedPODArray<T> & in,
        size_t scale,
        typename ColumnVector<Int64>::Container & out)
    {
        ScaleType decimal_scale = in.getScale();
        const T * end_in = in.data() + in.size();

        const T * __restrict p_in = in.data();
        Int64 * __restrict p_out = out.data();

        while (p_in < end_in)
        {
            Op::compute(p_in, scale, p_out, decimal_scale);
            ++p_in;
            ++p_out;
        }
    }
};

template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct DecimalRoundingImpl<T, rounding_mode, scale_mode, T>
{
private:
    using Op = DecimalRoundingComputation<T, rounding_mode, scale_mode, T>;
    using Data = T;

public:
    static NO_INLINE void apply(
        const DecimalPaddedPODArray<T> & in,
        size_t scale,
        typename ColumnDecimal<T>::Container & out)
    {
        ScaleType decimal_scale = in.getScale();
        const T * end_in = in.data() + in.size();

        const T * __restrict p_in = in.data();
        T * __restrict p_out = out.data();

        while (p_in < end_in)
        {
            Op::compute(p_in, scale, p_out, decimal_scale);
            ++p_in;
            ++p_out;
        }
    }
};

template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode, typename OutputType = T>
using FunctionRoundingImpl = std::conditional_t<
    std::is_floating_point_v<T>,
    FloatRoundingImpl<T, rounding_mode, scale_mode>,
    std::conditional_t<
        std::is_integral_v<T>,
        IntegerRoundingImpl<T, rounding_mode, scale_mode>,
        DecimalRoundingImpl<T, rounding_mode, scale_mode, OutputType>>>;


/** Select the appropriate processing algorithm depending on the scale and OutputType
  */
template <typename T, RoundingMode rounding_mode, typename OutputType = T>
struct Dispatcher
{
    template <typename Col>
    static void apply(Block & block, const Col * col, const ColumnNumbers & arguments, size_t result)
    {
        static_assert(std::is_same_v<Col, ColumnVector<T>> || std::is_same_v<Col, ColumnDecimal<T>>);
        Int64 scale_arg = 0;

        if (arguments.size() == 2)
        {
            const IColumn & scale_column = *block.getByPosition(arguments[1]).column;
            if (!scale_column.isColumnConst())
                throw Exception("Scale argument for rounding functions must be constant.", ErrorCodes::ILLEGAL_COLUMN);

            Field scale_field = static_cast<const ColumnConst &>(scale_column).getField();
            if (scale_field.getType() != Field::Types::UInt64 && scale_field.getType() != Field::Types::Int64)
                throw Exception(
                    "Scale argument for rounding functions must have integer type.",
                    ErrorCodes::ILLEGAL_COLUMN);

            scale_arg = scale_field.get<Int64>();
        }


        if constexpr (IsDecimal<OutputType>)
        {
            auto col_res = ColumnDecimal<OutputType>::create(col->getData().size(), col->getData().getScale());
            typename ColumnDecimal<OutputType>::Container & vec_res = col_res->getData();
            applyInternal(col, vec_res, col_res, block, scale_arg, result);
        }
        else
        {
            auto col_res = ColumnVector<OutputType>::create();
            typename ColumnVector<OutputType>::Container & vec_res = col_res->getData();
            applyInternal(col, vec_res, col_res, block, scale_arg, result);
        }
    }

private:
    template <typename VecRes, typename ColRes, typename Col>
    static void applyInternal(
        const Col * col,
        VecRes & vec_res,
        ColRes & col_res,
        Block & block,
        Int64 scale_arg,
        size_t result)
    {
        size_t scale = 1;
        vec_res.resize(col->getData().size());

        if (vec_res.empty())
        {
            block.getByPosition(result).column = std::move(col_res);
            return;
        }

        if (scale_arg == 0)
        {
            scale = 1;
            FunctionRoundingImpl<T, rounding_mode, ScaleMode::Zero, OutputType>::apply(col->getData(), scale, vec_res);
        }
        else if (scale_arg > 0)
        {
            scale = pow(10, scale_arg);
            FunctionRoundingImpl<T, rounding_mode, ScaleMode::Positive, OutputType>::apply(
                col->getData(),
                scale,
                vec_res);
        }
        else
        {
            scale = pow(10, -scale_arg);
            FunctionRoundingImpl<T, rounding_mode, ScaleMode::Negative, OutputType>::apply(
                col->getData(),
                scale,
                vec_res);
        }

        block.getByPosition(result).column = std::move(col_res);
    }
};

/** A template for functions that round the value of an input parameter of type
  * (U)Int8/16/32/64 or Float32/64, and accept an additional optional
  * parameter (default is 0).
  */
template <typename Name, RoundingMode rounding_mode>
class FunctionRounding : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRounding>(); }

private:
    template <typename T>
    bool executeForType(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        if constexpr (IsDecimal<T>)
        {
            if (auto col = checkAndGetColumn<ColumnDecimal<T>>(block.getByPosition(arguments[0]).column.get()))
            {
                Dispatcher<T, rounding_mode>::apply(block, col, arguments, result);
                return true;
            }
            return false;
        }
        else
        {
            if (auto col = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
            {
                Dispatcher<T, rounding_mode>::apply(block, col, arguments, result);
                return true;
            }
            return false;
        }
    }

public:
    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if ((arguments.empty()) || (arguments.size() > 2))
            throw Exception(
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be 1 or 2.",
                    getName(),
                    arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto & type : arguments)
            if (!type->isNumber() && !type->isDecimal())
                throw Exception(
                    fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        if (!(executeForType<UInt8>(block, arguments, result) || executeForType<UInt16>(block, arguments, result)
              || executeForType<UInt32>(block, arguments, result) || executeForType<UInt64>(block, arguments, result)
              || executeForType<Int8>(block, arguments, result) || executeForType<Int16>(block, arguments, result)
              || executeForType<Int32>(block, arguments, result) || executeForType<Int64>(block, arguments, result)
              || executeForType<Float32>(block, arguments, result) || executeForType<Float64>(block, arguments, result)
              || executeForType<Decimal32>(block, arguments, result)
              || executeForType<Decimal64>(block, arguments, result)
              || executeForType<Decimal128>(block, arguments, result)
              || executeForType<Decimal256>(block, arguments, result)))
        {
            throw Exception(
                fmt::format(
                    "Illegal column {} of argument of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return {true, true, true};
    }
};

/** FunctionRounding can only cast typeA to typeA
 * but TiDB may push down RoundDecimalToInt
 * (and this is the only round function that return type is different from arg type)
 * so we specialize RoundDecimalToInt and not use template
*/

template <typename Name, RoundingMode rounding_mode>
class FunctionRoundingDecimalToInt : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRoundingDecimalToInt>(); }

private:
    template <typename T>
    bool executeForType(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        static_assert(IsDecimal<T>);
        if (auto col = checkAndGetColumn<ColumnDecimal<T>>(block.getByPosition(arguments[0]).column.get()))
        {
            Dispatcher<T, rounding_mode, Int64>::apply(block, col, arguments, result);
            return true;
        }
        return false;
    }

public:
    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if ((arguments.empty()) || (arguments.size() > 2))
            throw Exception(
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be 1 or 2.",
                    getName(),
                    arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto & type : arguments)
            if (!type->isDecimal())
                throw Exception(
                    fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        if (!(executeForType<Decimal32>(block, arguments, result) || executeForType<Decimal64>(block, arguments, result)
              || executeForType<Decimal128>(block, arguments, result)
              || executeForType<Decimal256>(block, arguments, result)))
        {
            throw Exception(
                fmt::format(
                    "Illegal column {} of argument of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return {true, true, true};
    }
};

/**
 * differences between prec/scale/frac:
 * - prec/precision: number of decimal digits, including digits before and after decimal point.
 * - scale: number of decimal digits after decimal point.
 * - frac: the second argument of ROUND.
 *   - optional, and default to zero.
 *   - in MySQL, frac <= 30, which is decimal_max_scale.
 *
 * both prec and scale are non-negative, but frac can be negative.
 */
using FracType = Int64;

// build constant table of up to Nth power of 10 at compile time.
template <typename T, size_t N>
struct ConstPowOf10
{
    using ArrayType = std::array<T, N + 1>;

    static constexpr T base = static_cast<T>(10);

    static constexpr std::pair<ArrayType, bool> build()
    {
        ArrayType result{1};

        bool overflow = false;
        for (size_t i = 1; i <= N; ++i)
        {
            result[i] = result[i - 1] * base;
            overflow |= (result[i - 1] != result[i] / base);
        }

        return {result, overflow};
    }

    static constexpr auto pair = build();
    static constexpr auto result = pair.first;
    static constexpr bool overflow = pair.second;

    static_assert(!overflow, "Computation overflows");
};

template <typename InputType, typename OutputType>
struct TiDBFloatingRound
{
    static_assert(std::is_floating_point_v<InputType>);
    static_assert(std::is_floating_point_v<OutputType>);
    static_assert(sizeof(OutputType) >= sizeof(InputType));

    static OutputType eval(InputType input, FracType frac)
    {
        // modified from <https://github.com/pingcap/tidb/blob/26237b35f857c2388eab46f9ee3b351687143681/types/helper.go#L33-L48>.

        auto value = static_cast<OutputType>(input);
        auto base = 1.0;

        if (frac != 0)
        {
            // TODO: `std::pow` is expensive. Need optimization here.
            // one possible optimization is pre-computation. See <https://golang.org/src/math/pow10.go>.
            base = std::pow(10.0, frac);
            auto scaled_value = value * base;

            if (std::isinf(scaled_value))
            {
                // if frac is too large, base will be +inf.
                // at this time, it is usually beyond the effective precision of floating-point type.
                // no rounding is needed.
                return value;
            }
            else
                value = scaled_value;
        }

        // floating-point environment is thread-local, so `fesetround` is thread-safe.
        std::fesetround(FE_TONEAREST);
        value = std::nearbyint(value);

        if (frac != 0)
        {
            value /= base;

            if (!std::isfinite(value))
            {
                // if frac is too small, base will be zero.
                // at this time, even the maximum of possible floating-point values will be
                // rounded to zero.
                return 0.0;
            }
        }

        return value;
    }
};

template <typename InputType, typename OutputType>
struct TiDBIntegerRound
{
    static_assert(is_integer_v<InputType>);
    static_assert(is_integer_v<OutputType>);
    static_assert(actual_size_v<OutputType> >= actual_size_v<InputType>);

    static constexpr auto digits = std::numeric_limits<OutputType>::digits10;
    static constexpr auto max_digits = digits + 1;

    using UnsignedOutput = make_unsigned_t<OutputType>;
    using Pow = ConstPowOf10<UnsignedOutput, digits>;

    static void throwOverflowIf(bool condition)
    {
        if (condition)
            throw Exception(fmt::format("integer value is out of range in 'round'"), ErrorCodes::OVERFLOW_ERROR);
    }

    static OutputType castBack(bool negative [[maybe_unused]], UnsignedOutput value)
    {
        if constexpr (is_signed_v<OutputType>)
        {
            if (negative)
            {
                auto bound = toSafeUnsigned<UnsignedOutput>(std::numeric_limits<OutputType>::min());
                throwOverflowIf(value > bound);
                return static_cast<OutputType>(-value);
            }
            else
            {
                auto bound = toSafeUnsigned<UnsignedOutput>(std::numeric_limits<OutputType>::max());
                throwOverflowIf(value > bound);
                return static_cast<OutputType>(value);
            }
        }
        else
        {
            static_assert(std::is_same_v<OutputType, UnsignedOutput>);
            return value;
        }
    }

    static OutputType eval(InputType input, FracType frac)
    {
        auto value = static_cast<OutputType>(input);

        if (frac >= 0)
            return value;
        else if (frac < -max_digits)
            return 0;
        else
        {
            frac = -frac;

            // we need to cast input to unsigned first to ensure overflow is not
            // undefined behavior.
            auto absolute_value = toSafeUnsigned<OutputType>(value);

            if (frac == max_digits)
            {
                auto base = Pow::result[digits];

                // test `x >= 5 * base`, but `5 * base` may overflow.
                // since `base` is integer, `x / 5 >= base` iff. `floor(x / 5) >= base`.
                // if true, x will round up. But rounding up will definitely result in overflow.
                throwOverflowIf(absolute_value / 5 >= base);

                // round down.
                absolute_value = 0;
            }
            else
            {
                auto base = Pow::result[frac];
                auto remainder = absolute_value % base;

                absolute_value -= remainder;
                if (remainder >= base / 2)
                {
                    // round up.
                    auto rounded_value = absolute_value + base;
                    throwOverflowIf(rounded_value < absolute_value);
                    absolute_value = rounded_value;
                }
            }

            return castBack((input < 0), absolute_value);
        }
    }
};

struct TiDBDecimalRoundInfo
{
    FracType input_prec;
    FracType input_scale;
    FracType input_int_prec; // number of digits before decimal point.

    FracType output_prec;
    FracType output_scale;

    TiDBDecimalRoundInfo(const IDataType & input_type, const IDataType & output_type)
        : input_prec(getDecimalPrecision(input_type, 0))
        , input_scale(getDecimalScale(input_type, 0))
        , input_int_prec(input_prec - input_scale)
        , output_prec(getDecimalPrecision(output_type, 0))
        , output_scale(getDecimalScale(output_type, 0))
    {}
};

template <typename InputType, typename OutputType>
struct TiDBDecimalRound
{
    static_assert(IsDecimal<InputType>);
    static_assert(IsDecimal<OutputType>);

    // output type is promoted to prevent overflow.
    // this case is very rare, such as Decimal(90, 30) truncated to Decimal(65, 30).
    using UnsignedInput = make_unsigned_t<typename InputType::NativeType>;
    using UnsignedOutput = make_unsigned_t<typename PromoteType<typename OutputType::NativeType>::Type>;

    using PowForInput = ConstPowOf10<UnsignedInput, maxDecimalPrecision<InputType>()>;
    using PowForOutput = ConstPowOf10<UnsignedOutput, maxDecimalPrecision<OutputType>()>;

    static OutputType eval(const InputType & input, FracType frac, const TiDBDecimalRoundInfo & info)
    {
        // e.g. round(999.9999, -4) = 0.
        if (frac < -info.input_int_prec)
            return static_cast<OutputType>(0);

        // rounding.
        auto absolute_value = toSafeUnsigned<UnsignedInput>(input.value);
        if (frac < info.input_scale)
        {
            FracType frac_index = info.input_scale - frac;
            assert(frac_index <= info.input_prec);

            auto base = PowForInput::result[frac_index];
            auto remainder = absolute_value % base;

            absolute_value -= remainder;
            if (remainder >= base / 2)
            {
                // round up.
                absolute_value += base;
            }
        }

        // convert from input_scale to output_scale.
        FracType difference = info.input_scale - info.output_scale;

        if (difference > 0)
        {
            // output_scale will be different from input_scale only if frac is const.
            // in this case, all digits discarded by the following division should be zeroes.
            // they are reset to zeroes because of rounding.
            auto base = PowForInput::result[difference];
            assert(absolute_value % base == 0);

            absolute_value /= base;
        }

        auto scaled_value = static_cast<UnsignedOutput>(absolute_value);

        if (difference < 0)
            scaled_value *= PowForOutput::result[-difference];

        // check overflow and construct result.
        if (scaled_value > DecimalMaxValue::get(info.output_prec))
            throw TiFlashException("Data truncated", Errors::Decimal::Overflow);

        auto result = static_cast<typename OutputType::NativeType>(scaled_value);
        if (input.value < 0)
            return -static_cast<OutputType>(result);
        else
            return static_cast<OutputType>(result);
    }
};

struct TiDBRoundPrecisionInferer
{
    static std::tuple<PrecType, ScaleType> infer(PrecType prec, ScaleType scale, FracType frac, bool is_const_frac)
    {
        assert(prec >= scale);
        PrecType int_prec = prec - scale;
        ScaleType new_scale = scale;

        // +1 for possible overflow, e.g. round(99999.9) => 100000
        ScaleType int_prec_increment = 1;

        if (is_const_frac)
        {
            if (frac > static_cast<FracType>(decimal_max_scale))
                frac = decimal_max_scale;

            new_scale = std::max(0, frac);

            if (frac >= static_cast<FracType>(scale))
                int_prec_increment = 0;
        }

        PrecType new_prec = std::min(decimal_max_prec, int_prec + int_prec_increment + new_scale);
        return std::make_tuple(new_prec, new_scale);
    }
};

struct TiDBRoundArguments
{
    const DataTypePtr & input_type;
    const DataTypePtr & frac_type;
    const DataTypePtr & output_type;
    const ColumnPtr & input_column;
    const ColumnPtr & frac_column;
    MutableColumnPtr & output_column;
};

template <
    typename InputType,
    typename FracType,
    typename OutputType,
    typename InputColumn,
    typename FracColumn,
    typename OutputColumn>
struct TiDBRound
{
    static void apply(const TiDBRoundArguments & args)
    {
        auto input_column = checkAndGetColumn<InputColumn>(args.input_column.get());
        auto frac_column = checkAndGetColumn<FracColumn>(args.frac_column.get());

        if (input_column == nullptr)
            throw Exception(
                fmt::format("Illegal column {} for the first argument of function round", args.input_column->getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        if (frac_column == nullptr)
            throw Exception(
                fmt::format("Illegal column {} for the second argument of function round", args.frac_column->getName()),
                ErrorCodes::ILLEGAL_COLUMN);

        auto output_column = typeid_cast<OutputColumn *>(args.output_column.get());
        assert(output_column != nullptr);

        size_t size = input_column->size();
        auto & output_data = output_column->getData();
        output_data.resize(size);

        TiDBDecimalRoundInfo info [[maybe_unused]] (*args.input_type, *args.output_type);

        if constexpr (std::is_same_v<InputColumn, ColumnConst>)
        {
            if constexpr (std::is_same_v<FracColumn, ColumnConst>)
            {
                auto input_data = input_column->template getValue<InputType>();
                auto frac_data = frac_column->template getValue<FracType>();

                if constexpr (std::is_floating_point_v<InputType>)
                    output_data[0] = TiDBFloatingRound<InputType, OutputType>::eval(input_data, frac_data);
                else if constexpr (IsDecimal<InputType>)
                    output_data[0] = TiDBDecimalRound<InputType, OutputType>::eval(input_data, frac_data, info);
                else
                    output_data[0] = TiDBIntegerRound<InputType, OutputType>::eval(input_data, frac_data);
            }
            else
            {
                auto input_data = input_column->template getValue<InputType>();
                const auto & frac_data = frac_column->getData();

                for (size_t i = 0; i < size; ++i)
                {
                    if constexpr (std::is_floating_point_v<InputType>)
                        output_data[i] = TiDBFloatingRound<InputType, OutputType>::eval(input_data, frac_data[i]);
                    else if constexpr (IsDecimal<InputType>)
                        output_data[i] = TiDBDecimalRound<InputType, OutputType>::eval(input_data, frac_data[i], info);
                    else
                        output_data[i] = TiDBIntegerRound<InputType, OutputType>::eval(input_data, frac_data[i]);
                }
            }
        }
        else
        {
            if constexpr (std::is_same_v<FracColumn, ColumnConst>)
            {
                const auto & input_data = input_column->getData();
                auto frac_data = frac_column->template getValue<FracType>();

                for (size_t i = 0; i < size; ++i)
                {
                    if constexpr (std::is_floating_point_v<InputType>)
                        output_data[i] = TiDBFloatingRound<InputType, OutputType>::eval(input_data[i], frac_data);
                    else if constexpr (IsDecimal<InputType>)
                        output_data[i] = TiDBDecimalRound<InputType, OutputType>::eval(input_data[i], frac_data, info);
                    else
                        output_data[i] = TiDBIntegerRound<InputType, OutputType>::eval(input_data[i], frac_data);
                }
            }
            else
            {
                const auto & input_data = input_column->getData();
                const auto & frac_data = frac_column->getData();

                for (size_t i = 0; i < size; ++i)
                {
                    if constexpr (std::is_floating_point_v<InputType>)
                        output_data[i] = TiDBFloatingRound<InputType, OutputType>::eval(input_data[i], frac_data[i]);
                    else if constexpr (IsDecimal<InputType>)
                        output_data[i]
                            = TiDBDecimalRound<InputType, OutputType>::eval(input_data[i], frac_data[i], info);
                    else
                        output_data[i] = TiDBIntegerRound<InputType, OutputType>::eval(input_data[i], frac_data[i]);
                }
            }
        }
    }
};

/**
 * round(x, d) for TiDB.
 */
class FunctionTiDBRoundWithFrac : public IFunction
{
public:
    static constexpr auto name = "tidbRoundWithFrac";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTiDBRoundWithFrac>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool hasInformationAboutMonotonicity() const override { return true; }
    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return {true, true, true};
    }

    // default implementation might make const frac column into a non-const one, while const frac and
    // non-const frac column can generate different return types. Plese see TiDBRoundPrecisionInferer for details.
    bool useDefaultImplementationForConstants() const override { return false; }

private:
    static FracType getFracFromConstColumn(const ColumnConst * column)
    {
        using UnsignedFrac = make_unsigned_t<FracType>;

        auto field = column->getField();

        if (field.getType() == Field::TypeToEnum<FracType>::value)
            return field.get<FracType>();
        else if (field.getType() == Field::TypeToEnum<UnsignedFrac>::value)
        {
            auto unsigned_frac = field.get<UnsignedFrac>();

            // to prevent overflow. Large frac is useless in fact.
            unsigned_frac = std::min(unsigned_frac, static_cast<UnsignedFrac>(std::numeric_limits<FracType>::max()));

            return static_cast<FracType>(unsigned_frac);
        }
        else
        {
            throw Exception(
                fmt::format(
                    "Illegal frac column with type {}, expect const Int64/UInt64",
                    column->getField().getTypeName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArguments(arguments);

        auto input_type = arguments[0].type;

        const auto * frac_column = arguments[1].column.get();
        bool frac_column_const = frac_column && frac_column->isColumnConst();

        if (input_type->isInteger())
        {
            // in MySQL, integer round always returns 64-bit integer.
            // the sign is the same as input.
            if (input_type->isUnsignedInteger())
                return std::make_shared<DataTypeUInt64>();
            else
                return std::make_shared<DataTypeInt64>();
        }
        else if (input_type->isFloatingPoint())
        {
            // in MySQL, floating-point round always returns Float64.
            return std::make_shared<DataTypeFloat64>();
        }
        else
        {
            assert(input_type->isDecimal());

            auto prec = getDecimalPrecision(*input_type, std::numeric_limits<PrecType>::max());
            auto scale = getDecimalScale(*input_type, std::numeric_limits<ScaleType>::max());
            assert(prec != std::numeric_limits<PrecType>::max());
            assert(scale != std::numeric_limits<ScaleType>::max());

            // if is_const_frac is false, the value of frac will be ignored.
            FracType frac = 0;
            bool is_const_frac = true;

            if (frac_column_const)
            {
                const auto * column = typeid_cast<const ColumnConst *>(frac_column);
                assert(column != nullptr);

                is_const_frac = true;
                frac = getFracFromConstColumn(column);
            }
            else
                is_const_frac = false;

            auto [new_prec, new_scale] = TiDBRoundPrecisionInferer::infer(prec, scale, frac, is_const_frac);
            return createDecimal(new_prec, new_scale);
        }
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        ColumnsWithTypeAndName columns;
        for (const auto & position : arguments)
            columns.push_back(block.getByPosition(position));

        auto output_type = getReturnTypeImpl(columns);
        auto output_column = output_type->createColumn();

        auto input_type = columns[0].type;
        auto input_column = columns[0].column;
        auto frac_type = columns[1].type;
        auto frac_column = columns[1].column;

        bool is_all_column_const = input_column->isColumnConst() && frac_column->isColumnConst();

        // if the result is const, we need to only compute once.
        // but the column size may be greater than 1, so we resize them here.
        assert(input_column->size() == frac_column->size());
        if (is_all_column_const && input_column->size() != 1)
        {
            input_column = input_column->cloneResized(1);
            frac_column = frac_column->cloneResized(1);
        }

        checkInputTypeAndApply({input_type, frac_type, output_type, input_column, frac_column, output_column});

        if (is_all_column_const)
            block.getByPosition(result).column = ColumnConst::create(std::move(output_column), block.rows());
        else
            block.getByPosition(result).column = std::move(output_column);
    }

    template <typename F>
    bool castToNumericDataTypes(const IDataType * input_type, const F & f) const
    {
        return castTypeToEither<
            DataTypeFloat32,
            DataTypeFloat64,
            DataTypeDecimal32,
            DataTypeDecimal64,
            DataTypeDecimal128,
            DataTypeDecimal256,
            DataTypeInt8,
            DataTypeUInt8,
            DataTypeInt16,
            DataTypeUInt16,
            DataTypeInt32,
            DataTypeUInt32,
            DataTypeInt64,
            DataTypeUInt64>(input_type, f);
    }

    void checkInputTypeAndApply(const TiDBRoundArguments & args) const
    {
        if (!castToNumericDataTypes(args.input_type.get(), [&](const auto & input_type, bool) {
                using InputDataType = std::decay_t<decltype(input_type)>;
                checkFracTypeAndApply<typename InputDataType::FieldType>(args);
                return true;
            }))
        {
            throw Exception(
                fmt::format(
                    "Illegal column type {} for the first argument of function {}",
                    args.input_type->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    template <typename InputType>
    void checkFracTypeAndApply(const TiDBRoundArguments & args) const
    {
        if (!castTypeToEither<
                DataTypeInt64,
                DataTypeUInt64,
                DataTypeInt32,
                DataTypeUInt32,
                DataTypeInt16,
                DataTypeUInt16,
                DataTypeInt8,
                DataTypeUInt8>(args.frac_type.get(), [&](const auto & frac_type, bool) {
                using FracDataType = std::decay_t<decltype(frac_type)>;
                checkOutputTypeAndApply<InputType, typename FracDataType::FieldType>(args);
                return true;
            }))
        {
            throw Exception(
                fmt::format(
                    "Illegal column type {} for the second argument of function {}",
                    args.frac_type->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    template <typename InputType, typename FracType>
    void checkOutputTypeAndApply(const TiDBRoundArguments & args) const
    {
        if (!castToNumericDataTypes(args.output_type.get(), [&](const auto & output_type, bool) {
                using OutputDataType = std::decay_t<decltype(output_type)>;
                return checkColumnsAndApply<InputType, FracType, typename OutputDataType::FieldType>(args);
            }))
        {
            throw TiFlashException(
                fmt::format("Unexpected return type for function {}", getName()),
                Errors::Coprocessor::Internal);
        }
    }

    template <typename InputType, typename FracType, typename OutputType>
    bool checkColumnsAndApply(const TiDBRoundArguments & args) const
    {
        constexpr bool check_integer_output
            = is_signed_v<InputType> ? std::is_same_v<OutputType, Int64> : std::is_same_v<OutputType, UInt64>;

        if constexpr (
            (std::is_floating_point_v<InputType> && !std::is_same_v<OutputType, Float64>)
            || (IsDecimal<InputType> && !IsDecimal<OutputType>) || (is_integer_v<InputType> && !check_integer_output))
            return false;
        else
        {
            using InputColumn
                = std::conditional_t<IsDecimal<InputType>, ColumnDecimal<InputType>, ColumnVector<InputType>>;
            using FracColumn = ColumnVector<FracType>;
            using OutputColumn
                = std::conditional_t<IsDecimal<OutputType>, ColumnDecimal<OutputType>, ColumnVector<OutputType>>;

            if (args.input_column->isColumnConst())
            {
                if (args.frac_column->isColumnConst())
                    TiDBRound<InputType, FracType, OutputType, ColumnConst, ColumnConst, OutputColumn>::apply(args);
                else
                    TiDBRound<InputType, FracType, OutputType, ColumnConst, FracColumn, OutputColumn>::apply(args);
            }
            else
            {
                if (args.frac_column->isColumnConst())
                    TiDBRound<InputType, FracType, OutputType, InputColumn, ColumnConst, OutputColumn>::apply(args);
                else
                    TiDBRound<InputType, FracType, OutputType, InputColumn, FracColumn, OutputColumn>::apply(args);
            }

            return true;
        }
    }

    void checkArguments(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() != getNumberOfArguments())
            throw Exception(
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be {}",
                    getName(),
                    arguments.size(),
                    getNumberOfArguments()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto input_type = arguments[0].type;
        if (!input_type->isNumber() && !input_type->isDecimal())
            throw Exception(
                fmt::format("Illegal type {} of first argument of function {}", input_type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        // the second argument frac must be integers.
        auto frac_type = arguments[1].type;
        if (!frac_type->isInteger())
            throw Exception(
                fmt::format("Illegal type {} of second argument of function {}", frac_type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

// clang-format off
struct NameRoundToExp2 { static constexpr auto name = "roundToExp2"; };
struct NameRoundDuration { static constexpr auto name = "roundDuration"; };
struct NameRoundAge { static constexpr auto name = "roundAge"; };

struct NameRound { static constexpr auto name = "round"; };
struct NameCeil { static constexpr auto name = "ceil"; };
struct NameFloor { static constexpr auto name = "floor"; };
struct NameTrunc { static constexpr auto name = "trunc"; };

struct NameRoundDecimalToInt { static constexpr auto name = "roundDecimalToInt"; };
struct NameCeilDecimalToInt { static constexpr auto name = "ceilDecimalToInt"; };
struct NameFloorDecimalToInt { static constexpr auto name = "floorDecimalToInt"; };
struct NameTruncDecimalToInt { static constexpr auto name = "truncDecimalToInt"; };
// clang-format on

using FunctionRoundToExp2 = FunctionUnaryArithmetic<RoundToExp2Impl, NameRoundToExp2, false>;
using FunctionRoundDuration = FunctionUnaryArithmetic<RoundDurationImpl, NameRoundDuration, false>;
using FunctionRoundAge = FunctionUnaryArithmetic<RoundAgeImpl, NameRoundAge, false>;

using FunctionRound = FunctionRounding<NameRound, RoundingMode::Round>;
using FunctionFloor = FunctionRounding<NameFloor, RoundingMode::Floor>;
using FunctionCeil = FunctionRounding<NameCeil, RoundingMode::Ceil>;
using FunctionTrunc = FunctionRounding<NameTrunc, RoundingMode::Trunc>;

using FunctionRoundDecimalToInt = FunctionRoundingDecimalToInt<NameRoundDecimalToInt, RoundingMode::Round>;
using FunctionCeilDecimalToInt = FunctionRoundingDecimalToInt<NameCeilDecimalToInt, RoundingMode::Ceil>;
using FunctionFloorDecimalToInt = FunctionRoundingDecimalToInt<NameFloorDecimalToInt, RoundingMode::Floor>;
using FunctionTruncDecimalToInt = FunctionRoundingDecimalToInt<NameTruncDecimalToInt, RoundingMode::Trunc>;


struct PositiveMonotonicity
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field &, const Field &) { return {true}; }
};

// clang-format off
template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundToExp2> : PositiveMonotonicity {};
template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundDuration> : PositiveMonotonicity {};
template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundAge> : PositiveMonotonicity {};
// clang-format on

} // namespace DB
