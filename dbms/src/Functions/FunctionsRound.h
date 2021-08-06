#pragma once

#include <Common/Decimal.h>
#include <Common/TiFlashException.h>
#include <Common/toSafeUnsigned.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsArithmetic.h>
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
}


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
inline std::enable_if_t<std::is_integral_v<T> && (sizeof(T) <= sizeof(UInt32)), T>
roundDownToPowerOfTwo(T x)
{
    return x <= 0 ? 0 : (T(1) << (31 - __builtin_clz(x)));
}

template <typename T>
inline std::enable_if_t<std::is_integral_v<T> && (sizeof(T) == sizeof(UInt64)), T>
roundDownToPowerOfTwo(T x)
{
    return x <= 0 ? 0 : (T(1) << (63 - __builtin_clzll(x)));
}

template <typename T>
inline std::enable_if_t<std::is_same_v<T, Float32>, T>
roundDownToPowerOfTwo(T x)
{
    return ext::bit_cast<T>(ext::bit_cast<UInt32>(x) & ~((1ULL << 23) - 1));
}

template <typename T>
inline std::enable_if_t<std::is_same_v<T, Float64>, T>
roundDownToPowerOfTwo(T x)
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

    static inline T apply(T x)
    {
        return roundDownToPowerOfTwo<T>(x);
    }
};


template <typename A>
struct RoundDurationImpl
{
    using ResultType = UInt16;

    static inline ResultType apply(A x)
    {
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
    }
};

template <typename T> struct RoundDurationImpl<Decimal<T>>{
    using ResultType = UInt16;
    static inline ResultType apply(Decimal<T>) { throw Exception("RoundDuration of decimal is not implemented yet."); }
};

template <typename A>
struct RoundAgeImpl
{
    using ResultType = UInt8;

    static inline ResultType apply(A x)
    {
        return x < 1 ? 0
            : (x < 18 ? 17
            : (x < 25 ? 18
            : (x < 35 ? 25
            : (x < 45 ? 35
            : (x < 55 ? 45
            : 55)))));
    }
};

template <typename T> struct RoundAgeImpl<Decimal<T>>{
    using ResultType = UInt8;
    static inline ResultType apply(Decimal<T>) { throw Exception("RoundAge of decimal is not implemented yet."); }
};

/** This parameter controls the behavior of the rounding functions.
  */
enum class ScaleMode
{
    Positive,   // round to a number with N decimal places after the decimal point
    Negative,   // round to an integer with N zero characters
    Zero,       // round to an integer
};

enum class RoundingMode
{
#if __SSE4_1__
    Round   = _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC,
    Floor   = _MM_FROUND_TO_NEG_INF | _MM_FROUND_NO_EXC,
    Ceil    = _MM_FROUND_TO_POS_INF | _MM_FROUND_NO_EXC,
    Trunc   = _MM_FROUND_TO_ZERO | _MM_FROUND_NO_EXC,
#else
    Round   = 8,    /// Values are correspond to above just in case.
    Floor   = 9,
    Ceil    = 10,
    Trunc   = 11,
#endif
};

<<<<<<< HEAD
=======
/** Rounding functions for decimal values
 */

template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode, typename OutputType>
struct DecimalRoundingComputation
{
    static_assert(IsDecimal<T>);
    static const size_t data_count = 1;
    static size_t prepare(size_t scale) {
        return scale;
    }
    // compute need decimal_scale to interpret decimals
    static inline void compute(const T * __restrict in, size_t scale, OutputType * __restrict out, ScaleType decimal_scale) {
        static_assert(std::is_same_v<T, OutputType> || std::is_same_v<OutputType, Int64>);
        Float64 val = in->template toFloat<Float64>(decimal_scale);

        if constexpr(scale_mode == ScaleMode::Positive)
        {
            val = val * scale;
        }
        else if constexpr(scale_mode == ScaleMode::Negative)
        {
            val = val / scale;
        }

        if constexpr(rounding_mode == RoundingMode::Round)
        {
            val = round(val);
        }
        else if constexpr(rounding_mode == RoundingMode::Floor)
        {
            val = floor(val);
        }
        else if constexpr(rounding_mode == RoundingMode::Ceil)
        {
            val = ceil(val);
        }
        else if constexpr(rounding_mode == RoundingMode::Trunc)
        {
            val = trunc(val);
        }


        if constexpr(scale_mode == ScaleMode::Positive)
        {
            val = val / scale;
        }
        else if constexpr(scale_mode == ScaleMode::Negative)
        {
            val = val * scale;
        }

        if constexpr(std::is_same_v<T, OutputType>)
        {
            *out = ToDecimal<Float64, T>(val, decimal_scale);
        }
        else if constexpr(std::is_same_v<OutputType, Int64>)
        {
            *out = static_cast<Int64>(val);
        }
        else
        {
            ;   // never arrived here
        }
    }
};

>>>>>>> 4997809a0... Push down `ROUND(x)` on decimal types (#2492)

/** Rounding functions for integer values.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct IntegerRoundingComputation
{
    static const size_t data_count = 1;

    static size_t prepare(size_t scale)
    {
        return scale;
    }

    static ALWAYS_INLINE T computeImpl(T x, T scale)
    {
        switch (rounding_mode)
        {
            case RoundingMode::Trunc:
            {
                return x / scale * scale;
            }
            case RoundingMode::Floor:
            {
                if (x < 0)
                    x -= scale - 1;
                return x / scale * scale;
            }
            case RoundingMode::Ceil:
            {
                if (x >= 0)
                    x += scale - 1;
                return x / scale * scale;
            }
            case RoundingMode::Round:
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
    }

    static ALWAYS_INLINE T compute(T x, T scale)
    {
        switch (scale_mode)
        {
            case ScaleMode::Zero:
                return x;
            case ScaleMode::Positive:
                return x;
            case ScaleMode::Negative:
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
    static void store(ScalarType * out, VectorType val) { _mm_storeu_ps(out, val);}
    static VectorType multiply(VectorType val, VectorType scale) { return _mm_mul_ps(val, scale); }
    static VectorType divide(VectorType val, VectorType scale) { return _mm_div_ps(val, scale); }
    template <RoundingMode mode> static VectorType apply(VectorType val) { return _mm_round_ps(val, int(mode)); }

    static VectorType prepare(size_t scale)
    {
        return load1(scale);
    }
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
    static void store(ScalarType * out, VectorType val) { _mm_storeu_pd(out, val);}
    static VectorType multiply(VectorType val, VectorType scale) { return _mm_mul_pd(val, scale); }
    static VectorType divide(VectorType val, VectorType scale) { return _mm_div_pd(val, scale); }
    template <RoundingMode mode> static VectorType apply(VectorType val) { return _mm_round_pd(val, int(mode)); }

    static VectorType prepare(size_t scale)
    {
        return load1(scale);
    }
};

#else

/// Implementation for ARM. Not vectorized.

inline float roundWithMode(float x, RoundingMode mode)
{
    switch (mode)
    {
        case RoundingMode::Round: return roundf(x);
        case RoundingMode::Floor: return floorf(x);
        case RoundingMode::Ceil: return ceilf(x);
        case RoundingMode::Trunc: return truncf(x);
        default:
            throw Exception("Logical error: unexpected 'mode' parameter passed to function roundWithMode", ErrorCodes::LOGICAL_ERROR);
    }
}

inline double roundWithMode(double x, RoundingMode mode)
{
    switch (mode)
    {
        case RoundingMode::Round: return round(x);
        case RoundingMode::Floor: return floor(x);
        case RoundingMode::Ceil: return ceil(x);
        case RoundingMode::Trunc: return trunc(x);
        default:
            throw Exception("Logical error: unexpected 'mode' parameter passed to function roundWithMode", ErrorCodes::LOGICAL_ERROR);
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
    static VectorType store(ScalarType * out, ScalarType val) { return *out = val;}
    static VectorType multiply(VectorType val, VectorType scale) { return val * scale; }
    static VectorType divide(VectorType val, VectorType scale) { return val / scale; }
    template <RoundingMode mode> static VectorType apply(VectorType val) { return roundWithMode(val, mode); }

    static VectorType prepare(size_t scale)
    {
        return load1(scale);
    }
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

        const T* end_in = in.data() + in.size();
        const T* limit = in.data() + in.size() / data_count * data_count;

        const T* __restrict p_in = in.data();
        T* __restrict p_out = out.data();

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
        const T* end_in = in.data() + in.size();

        const T* __restrict p_in = in.data();
        T* __restrict p_out = out.data();

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
            case 1ULL: return applyImpl<1ULL>(in, out);
            case 10ULL: return applyImpl<10ULL>(in, out);
            case 100ULL: return applyImpl<100ULL>(in, out);
            case 1000ULL: return applyImpl<1000ULL>(in, out);
            case 10000ULL: return applyImpl<10000ULL>(in, out);
            case 100000ULL: return applyImpl<100000ULL>(in, out);
            case 1000000ULL: return applyImpl<1000000ULL>(in, out);
            case 10000000ULL: return applyImpl<10000000ULL>(in, out);
            case 100000000ULL: return applyImpl<100000000ULL>(in, out);
            case 1000000000ULL: return applyImpl<1000000000ULL>(in, out);
            case 10000000000ULL: return applyImpl<10000000000ULL>(in, out);
            case 100000000000ULL: return applyImpl<100000000000ULL>(in, out);
            case 1000000000000ULL: return applyImpl<1000000000000ULL>(in, out);
            case 10000000000000ULL: return applyImpl<10000000000000ULL>(in, out);
            case 100000000000000ULL: return applyImpl<100000000000000ULL>(in, out);
            case 1000000000000000ULL: return applyImpl<1000000000000000ULL>(in, out);
            case 10000000000000000ULL: return applyImpl<10000000000000000ULL>(in, out);
            case 100000000000000000ULL: return applyImpl<100000000000000000ULL>(in, out);
            case 1000000000000000000ULL: return applyImpl<1000000000000000000ULL>(in, out);
            case 10000000000000000000ULL: return applyImpl<10000000000000000000ULL>(in, out);
            default:
                throw Exception("Logical error: unexpected 'scale' parameter passed to function IntegerRoundingComputation::compute",
                    ErrorCodes::LOGICAL_ERROR);
        }
    }
};

template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
using FunctionRoundingImpl = std::conditional_t<std::is_floating_point_v<T>,
    FloatRoundingImpl<T, rounding_mode, scale_mode>,
    IntegerRoundingImpl<T, rounding_mode, scale_mode>>;


/** Select the appropriate processing algorithm depending on the scale.
  */
template <typename T, RoundingMode rounding_mode>
struct Dispatcher
{
    static void apply(Block & block, const ColumnVector<T> * col, const ColumnNumbers & arguments, size_t result)
    {
        size_t scale = 1;
        Int64 scale_arg = 0;

        if (arguments.size() == 2)
        {
            const IColumn & scale_column = *block.getByPosition(arguments[1]).column;
            if (!scale_column.isColumnConst())
                throw Exception("Scale argument for rounding functions must be constant.", ErrorCodes::ILLEGAL_COLUMN);

            Field scale_field = static_cast<const ColumnConst &>(scale_column).getField();
            if (scale_field.getType() != Field::Types::UInt64
                && scale_field.getType() != Field::Types::Int64)
                throw Exception("Scale argument for rounding functions must have integer type.", ErrorCodes::ILLEGAL_COLUMN);

            scale_arg = scale_field.get<Int64>();
        }

        auto col_res = ColumnVector<T>::create();

<<<<<<< HEAD
        typename ColumnVector<T>::Container & vec_res = col_res->getData();
=======
        if constexpr(IsDecimal<OutputType>)
        {
            auto col_res = ColumnDecimal<OutputType>::create(col->getData().size(), col->getData().getScale());
            typename ColumnDecimal<OutputType>::Container & vec_res = col_res->getData();
            applyInternal(col, vec_res, col_res, block, scale_arg, result);
        }
        else
        {   auto col_res = ColumnVector<OutputType>::create();
            typename ColumnVector<OutputType>::Container & vec_res = col_res->getData();
            applyInternal(col, vec_res, col_res, block, scale_arg, result);
        }

    }
private:
    template <typename VecRes, typename ColRes, typename Col>
    static void applyInternal(const Col * col, VecRes& vec_res, ColRes& col_res, Block& block, Int64 scale_arg, size_t result) {
        size_t scale = 1;
>>>>>>> 4997809a0... Push down `ROUND(x)` on decimal types (#2492)
        vec_res.resize(col->getData().size());

        if (vec_res.empty())
        {
            block.getByPosition(result).column = std::move(col_res);
            return;
        }

        if (scale_arg == 0)
        {
            scale = 1;
            FunctionRoundingImpl<T, rounding_mode, ScaleMode::Zero>::apply(col->getData(), scale, vec_res);
        }
        else if (scale_arg > 0)
        {
            scale = pow(10, scale_arg);
            FunctionRoundingImpl<T, rounding_mode, ScaleMode::Positive>::apply(col->getData(), scale, vec_res);
        }
        else
        {
            scale = pow(10, -scale_arg);
            FunctionRoundingImpl<T, rounding_mode, ScaleMode::Negative>::apply(col->getData(), scale, vec_res);
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
    bool executeForType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (auto col = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
        {
            Dispatcher<T, rounding_mode>::apply(block, col, arguments, result);
            return true;
        }
<<<<<<< HEAD
        return false;
=======
        else
        {
            if (auto col = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
            {
                Dispatcher<T, rounding_mode>::apply(block, col, arguments, result);
                return true;
            }
            return false;
        }
>>>>>>> 4997809a0... Push down `ROUND(x)` on decimal types (#2492)
    }

public:
    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if ((arguments.size() < 1) || (arguments.size() > 2))
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto & type : arguments)
            if (!type->isNumber())
                throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (!(    executeForType<UInt8>(block, arguments, result)
            ||    executeForType<UInt16>(block, arguments, result)
            ||    executeForType<UInt32>(block, arguments, result)
            ||    executeForType<UInt64>(block, arguments, result)
            ||    executeForType<Int8>(block, arguments, result)
            ||    executeForType<Int16>(block, arguments, result)
            ||    executeForType<Int32>(block, arguments, result)
            ||    executeForType<Int64>(block, arguments, result)
            ||    executeForType<Float32>(block, arguments, result)
            ||    executeForType<Float64>(block, arguments, result)))
        {
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return { true, true, true };
    }
};

<<<<<<< HEAD
=======
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
    bool executeForType(Block & block, const ColumnNumbers & arguments, size_t result)
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
    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if ((arguments.size() < 1) || (arguments.size() > 2))
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto & type : arguments)
            if (!type->isDecimal())
                throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (!(    executeForType<Decimal32>(block, arguments, result)
            ||    executeForType<Decimal64>(block, arguments, result)
            ||    executeForType<Decimal128>(block, arguments, result)
            ||    executeForType<Decimal256>(block, arguments, result)))
        {
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return { true, true, true };
    }
};
>>>>>>> 4997809a0... Push down `ROUND(x)` on decimal types (#2492)

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

template <typename InputType>
struct TiDBIntegerRound
{
    static_assert(is_integer_v<InputType>);

    static constexpr InputType eval(const InputType & input, FracType frac [[maybe_unused]])
    {
        // TODO: RoundWithFrac.
        assert(frac == 0);

        return input;
    }
};

template <typename InputType>
struct TiDBFloatingRound
{
    static_assert(std::is_floating_point_v<InputType>);

    // EvalType is the type used in evaluations.
    // in MySQL, floating round always returns Float64.
    using EvalType = Float64;

    static constexpr EvalType eval(const InputType & input, FracType frac [[maybe_unused]])
    {
        // TODO: RoundWithFrac.
        assert(frac == 0);

        auto value = static_cast<EvalType>(input);

        // floating-point environment is thread-local, so `fesetround` is thread-safe.
        std::fesetround(FE_TONEAREST);
        return std::nearbyint(value);
    }
};

// build constant table of up to Nth power of 10 at compile time.
template <typename T, size_t N>
struct ConstPowOf10
{
    using ArrayType = std::array<T, N + 1>;

    static constexpr ArrayType build()
    {
        ArrayType result = {1};
        for (size_t i = 1; i <= N; ++i)
            result[i] = result[i - 1] * static_cast<T>(10);
        return result;
    }

    static constexpr ArrayType result = build();
};

template <typename InputType, typename OutputType>
struct TiDBDecimalRound
{
    static_assert(IsDecimal<InputType>);

    using UnsignedNativeType = make_unsigned_t<typename InputType::NativeType>;
    using Pow = ConstPowOf10<UnsignedNativeType, maxDecimalPrecision<InputType>()>;

    static constexpr OutputType eval(const InputType & input, FracType frac [[maybe_unused]], ScaleType input_scale)
    {
        // TODO: RoundWithFrac.
        assert(frac == 0);

        auto divider = Pow::result[input_scale];
        auto absolute_value = toSafeUnsigned<UnsignedNativeType>(input.value);

        // "round half away from zero"
        // examples:
        // - input.value = 149, input_scale = 2, divider = 100, result = (149 + 100 / 2) / 100 = 1
        // - input.value = 150, input_scale = 2, divider = 100, result = (150 + 100 / 2) / 100 = 2
        auto absolute_result = static_cast<typename OutputType::NativeType>((absolute_value + divider / 2) / divider);

        if (input.value < 0)
            return -static_cast<OutputType>(absolute_result);
        else
            return static_cast<OutputType>(absolute_result);
    }
};

static FracType getFracFromConstColumn(const ColumnConst * column)
{
    FracType result;
    auto frac_field = column->getField();

    if (!frac_field.tryGet(result))
    {
        // maybe field is unsigned.
        static_assert(is_signed_v<FracType>);
        make_unsigned_t<FracType> unsigned_frac;

        if (!frac_field.tryGet(unsigned_frac))
        {
            throw Exception(
                fmt::format("Illegal frac column with type {}, expected to const Int64/UInt64", column->getField().getTypeName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }

        result = static_cast<FracType>(unsigned_frac);
    }

    // in MySQL, frac is clamped to 30, which is identical to decimal_max_scale.
    // frac is signed but decimal_max_scale is unsigned, so we have to cast before
    // comparison.
    if (result > static_cast<FracType>(decimal_max_scale))
        result = decimal_max_scale;

    return result;
}

struct TiDBRoundPrecisionInferer
{
    static std::tuple<PrecType, ScaleType> infer(
        PrecType prec, ScaleType scale, FracType frac [[maybe_unused]], bool is_const_frac [[maybe_unused]])
    {
        // TODO: RoundWithFrac.
        assert(is_const_frac);
        assert(frac == 0);

        assert(prec >= scale);
        PrecType new_prec = prec - scale;

        // +1 for possible overflow, e.g. round(99999.9) => 100000
        if (scale > 0)
            new_prec += 1;

        return std::make_tuple(new_prec, 0);
    }
};

template <typename InputType, typename OutputType, typename FracType, typename InputColumn, typename OutputColumn, typename FracColumn>
struct TiDBRound
{
    static void apply(const ColumnPtr & input_column_, const ColumnPtr & frac_column_, MutableColumnPtr & output_column_,
        ScaleType input_scale [[maybe_unused]], ScaleType output_scale [[maybe_unused]])
    {
        auto input_column = checkAndGetColumn<InputColumn>(input_column_.get());
        auto frac_column = checkAndGetColumn<FracColumn>(frac_column_.get());

        if (input_column == nullptr)
            throw Exception(fmt::format("Illegal column {} for the first argument of function round", input_column_->getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        if (frac_column == nullptr)
            throw Exception(fmt::format("Illegal column {} for the second argument of function round", frac_column_->getName()),
                ErrorCodes::ILLEGAL_COLUMN);

        // TODO: RoundWithFrac.
        assert(frac_column->isColumnConst());
        auto frac_value = getFracFromConstColumn(frac_column);
        assert(frac_value == 0);

        // TODO: const input column.
        assert(!input_column->isColumnConst());

        auto & input_data = input_column->getData();
        size_t size = input_data.size();

        auto output_column = typeid_cast<OutputColumn *>(output_column_.get());
        assert(output_column != nullptr);

        auto & output_data = output_column->getData();
        output_data.resize(size);

        for (size_t i = 0; i < size; ++i)
        {
            // TODO: RoundWithFrac.
            if constexpr (std::is_floating_point_v<InputType>)
                output_data[i] = TiDBFloatingRound<InputType>::eval(input_data[i], 0);
            else if constexpr (IsDecimal<InputType>)
                output_data[i] = TiDBDecimalRound<InputType, OutputType>::eval(input_data[i], 0, input_scale);
            else
                output_data[i] = TiDBIntegerRound<InputType>::eval(input_data[i], 0);
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
    bool useDefaultImplementationForConstants() const override { return true; }
    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override { return {true, true, true}; }

private:
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArguments(arguments);

        auto input_type = arguments[0].type;

        if (input_type->isInteger())
            return input_type;
        else if (input_type->isFloatingPoint())
        {
            // in MySQL, floating round always returns Float64.
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

            auto frac_column = arguments[1].column.get();
            if (frac_column->isColumnConst())
            {
                auto column = typeid_cast<const ColumnConst *>(frac_column);
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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        ColumnsWithTypeAndName columns;
        for (const auto & position : arguments)
            columns.push_back(block.getByPosition(position));

        auto return_type = getReturnTypeImpl(columns);
        auto result_column = return_type->createColumn();

        auto input_type = columns[0].type;
        auto input_column = columns[0].column;
        auto frac_type = columns[1].type;
        auto frac_column = columns[1].column;

        auto input_scale = getDecimalScale(*input_type, 0);
        auto result_scale = getDecimalScale(*return_type, 0);

        checkInputTypeAndApply(input_type, return_type, frac_type, input_column, frac_column, result_column, input_scale, result_scale);

        block.getByPosition(result).column = std::move(result_column);
    }

    void checkInputTypeAndApply(const DataTypePtr & input_type, const DataTypePtr & return_type, const DataTypePtr & frac_type,
        const ColumnPtr & input_column, const ColumnPtr & frac_column, MutableColumnPtr & result_column, ScaleType input_scale,
        ScaleType result_scale)
    {
        if (!castTypeToEither<DataTypeFloat32, DataTypeFloat64, DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128, DataTypeDecimal256,
                DataTypeInt8, DataTypeUInt8, DataTypeInt16, DataTypeUInt16, DataTypeInt32, DataTypeUInt32, DataTypeInt64, DataTypeUInt64>(
                input_type.get(), [&](const auto & input_type, bool) {
                    using InputDataType = std::decay_t<decltype(input_type)>;

                    checkReturnTypeAndApply<typename InputDataType::FieldType>(
                        return_type, frac_type, input_column, frac_column, result_column, input_scale, result_scale);

                    return true;
                }))
        {
            throw Exception(fmt::format("Illegal column type {} for the first argument of function {}", input_type->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    template <typename InputType>
    void checkReturnTypeAndApply(const DataTypePtr & return_type, const DataTypePtr & frac_type, const ColumnPtr & input_column,
        const ColumnPtr & frac_column, MutableColumnPtr & result_column, ScaleType input_scale, ScaleType result_scale)
    {
        if (!castTypeToEither<DataTypeFloat32, DataTypeFloat64, DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128, DataTypeDecimal256,
                DataTypeInt8, DataTypeUInt8, DataTypeInt16, DataTypeUInt16, DataTypeInt32, DataTypeUInt32, DataTypeInt64, DataTypeUInt64>(
                return_type.get(), [&](const auto & return_type, bool) {
                    using ReturnDataType = std::decay_t<decltype(return_type)>;

                    return checkFracTypeAndApply<InputType, typename ReturnDataType::FieldType>(
                        frac_type, input_column, frac_column, result_column, input_scale, result_scale);
                }))
        {
            throw TiFlashException(fmt::format("Unexpected return type for function {}", getName()), Errors::Coprocessor::Internal);
        }
    }

    template <typename InputType, typename ReturnType>
    bool checkFracTypeAndApply(const DataTypePtr & frac_type, const ColumnPtr & input_column, const ColumnPtr & frac_column,
        MutableColumnPtr & result_column, ScaleType input_scale [[maybe_unused]], ScaleType result_scale [[maybe_unused]])
    {
        if constexpr ((std::is_floating_point_v<InputType> && !std::is_same_v<ReturnType, Float64>)
            || (IsDecimal<InputType> && !IsDecimal<ReturnType>) || (is_integer_v<InputType> && !std::is_same_v<InputType, ReturnType>))
            return false;
        else
        {
            if (!castTypeToEither<DataTypeInt64, DataTypeUInt64>(frac_type.get(), [&](const auto & frac_type, bool) {
                    using FracDataType = std::decay_t<decltype(frac_type)>;

                    checkColumnsAndApply<InputType, ReturnType, typename FracDataType::FieldType>(
                        input_column, frac_column, result_column, input_scale, result_scale);

                    return true;
                }))
            {
                throw Exception(
                    fmt::format("Illegal column type {} for the second argument of function {}", frac_type->getName(), getName()),
                    ErrorCodes::ILLEGAL_COLUMN);
            }

            return true;
        }
    }

    template <typename InputType, typename ReturnType, typename FracType>
    void checkColumnsAndApply(const ColumnPtr & input_column, const ColumnPtr & frac_column, MutableColumnPtr & result_column,
        ScaleType input_scale, ScaleType result_scale)
    {
        using InputColumn = std::conditional_t<IsDecimal<InputType>, ColumnDecimal<InputType>, ColumnVector<InputType>>;
        using ResultColumn = std::conditional_t<IsDecimal<ReturnType>, ColumnDecimal<ReturnType>, ColumnVector<ReturnType>>;

        // TODO: RoundWithFrac
        assert(!input_column->isColumnConst());
        assert(frac_column->isColumnConst());

        TiDBRound<InputType, ReturnType, FracType, InputColumn, ResultColumn, ColumnConst>::apply(
            input_column, frac_column, result_column, input_scale, result_scale);
    }

    void checkArguments(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() != getNumberOfArguments())
            throw Exception(fmt::format("Number of arguments for function {} doesn't match: passed {}, should be {}", getName(),
                                arguments.size(), getNumberOfArguments()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto input_type = arguments[0].type;
        if (!input_type->isNumber() && !input_type->isDecimal())
            throw Exception(fmt::format("Illegal type {} of first argument of function {}", input_type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        // the second argument frac must be integers.
        auto frac_type = arguments[1].type;
        if (!frac_type->isInteger())
            throw Exception(fmt::format("Illegal type {} of second argument of function {}", frac_type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

struct NameRoundToExp2 { static constexpr auto name = "roundToExp2"; };
struct NameRoundDuration { static constexpr auto name = "roundDuration"; };
struct NameRoundAge { static constexpr auto name = "roundAge"; };

struct NameRound { static constexpr auto name = "round"; };
struct NameCeil { static constexpr auto name = "ceil"; };
struct NameFloor { static constexpr auto name = "floor"; };
struct NameTrunc { static constexpr auto name = "trunc"; };

using FunctionRoundToExp2 = FunctionUnaryArithmetic<RoundToExp2Impl, NameRoundToExp2, false>;
using FunctionRoundDuration = FunctionUnaryArithmetic<RoundDurationImpl, NameRoundDuration, false>;
using FunctionRoundAge = FunctionUnaryArithmetic<RoundAgeImpl, NameRoundAge, false>;

using FunctionRound = FunctionRounding<NameRound, RoundingMode::Round>;
using FunctionFloor = FunctionRounding<NameFloor, RoundingMode::Floor>;
using FunctionCeil = FunctionRounding<NameCeil, RoundingMode::Ceil>;
using FunctionTrunc = FunctionRounding<NameTrunc, RoundingMode::Trunc>;


struct PositiveMonotonicity
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return { true };
    }
};

template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundToExp2> : PositiveMonotonicity {};
template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundDuration> : PositiveMonotonicity {};
template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundAge> : PositiveMonotonicity {};

}
