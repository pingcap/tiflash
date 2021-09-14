#include <Functions/DivisionUtils.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
template <typename A, typename B>
struct DivideFloatingImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) / b;
    }

    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct DivideFloatingImpl<A, B, true>
{
    using ResultPrecInferer = DivDecimalInferer;
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) / static_cast<Result>(b);
    }

    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct TiDBDivideFloatingImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) / b;
    }
    template <typename Result = ResultType>
    static Result apply(A a, B b, UInt8 & res_null)
    {
        if (b == 0)
        {
            /// we can check res_null to see if it is DIVISION_BY_ZERO or DIVISION_BY_NULL, when sql mode is ERROR_FOR_DIVISION_BY_ZERO,
            /// inserts and updates involving expressions that perform division by zero should be treated as errors, now only read-only
            /// statement will send to TiFlash, so just return NULL here
            res_null = 1;
            return static_cast<Result>(0);
        }
        return static_cast<Result>(a) / b;
    }
};

template <typename A, typename B>
struct TiDBDivideFloatingImpl<A, B, true>
{
    using ResultPrecInferer = DivDecimalInferer;
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) / static_cast<Result>(b);
    }

    template <typename Result = ResultType>
    static Result apply(A a, B b, UInt8 & res_null)
    {
        if (static_cast<Result>(b) == static_cast<Result>(0))
        {
            /// we can check res_null to see if it is DIVISION_BY_ZERO or DIVISION_BY_NULL, when sql mode is ERROR_FOR_DIVISION_BY_ZERO,
            /// inserts and updates involving expressions that perform division by zero should be treated as errors, now only read-only
            /// statement will send to TiFlash, so just return NULL here
            res_null = 1;
            return static_cast<Result>(0);
        }
        return static_cast<Result>(a) / static_cast<Result>(b);
    }
};

template <typename A, typename B>
struct DivideIntegralImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(a, b);
        return static_cast<Result>(a) / static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct DivideIntegralImpl<A, B, true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>)
        {
            x = static_cast<Result>(a.value);
        }
        else
        {
            x = static_cast<Result>(a);
        }
        if constexpr (IsDecimal<B>)
        {
            y = static_cast<Result>(b.value);
        }
        else
        {
            y = static_cast<Result>(b);
        }
        throwIfDivisionLeadsToFPE(x, y);
        return x / y;
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct DivideIntegralOrZeroImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(unlikely(divisionLeadsToFPE(a, b)) ? 0 : static_cast<Result>(a) / static_cast<Result>(b));
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct DivideIntegralOrZeroImpl<A, B, true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>)
        {
            x = static_cast<Result>(a.value);
        }
        else
        {
            x = static_cast<Result>(a);
        }
        if constexpr (IsDecimal<B>)
        {
            y = static_cast<Result>(b.value);
        }
        else
        {
            y = static_cast<Result>(b);
        }
        throwIfDivisionLeadsToFPE(x, y);
        return unlikely(divisionLeadsToFPE(x, y)) ? 0 : x / y;
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

/// Optimizations for integer division by a constant.

#if __SSE2__
#define LIBDIVIDE_SSE2 1
#endif

#include <libdivide.h>

template <typename A, typename B>
struct DivideIntegralByConstantImpl : BinaryOperationImplBase<A, B, DivideIntegralImpl<A, B>>
{
    using ResultType = typename DivideIntegralImpl<A, B>::ResultType;

    static void vector_constant(const PaddedPODArray<A> & a, B b, PaddedPODArray<ResultType> & c)
    {
        if (unlikely(b == 0))
            throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

        if constexpr (is_signed_v<B>)
        {
            if (unlikely(b == -1))
            {
                size_t size = a.size();
                for (size_t i = 0; i < size; ++i)
                    c[i] = -c[i];
                return;
            }
        }

        libdivide::divider<A> divider(b);

        size_t size = a.size();
        const A * a_pos = &a[0];
        const A * a_end = a_pos + size;
        ResultType * c_pos = &c[0];

#if __SSE2__
        static constexpr size_t values_per_sse_register = 16 / sizeof(A);
        const A * a_end_sse = a_pos + size / values_per_sse_register * values_per_sse_register;

        while (a_pos < a_end_sse)
        {
            _mm_storeu_si128(reinterpret_cast<__m128i *>(c_pos),
                             _mm_loadu_si128(reinterpret_cast<const __m128i *>(a_pos)) / divider);

            a_pos += values_per_sse_register;
            c_pos += values_per_sse_register;
        }
#endif

        while (a_pos < a_end)
        {
            *c_pos = *a_pos / divider;
            ++a_pos;
            ++c_pos;
        }
    }
};

/** Specializations are specified for dividing numbers of the type UInt64 and UInt32 by the numbers of the same sign.
  * Can be expanded to all possible combinations, but more code is needed.
  */

// clang-format off
template <> struct BinaryOperationImpl<UInt64, UInt8, DivideIntegralImpl<UInt64, UInt8>> : DivideIntegralByConstantImpl<UInt64, UInt8> {};
template <> struct BinaryOperationImpl<UInt64, UInt16, DivideIntegralImpl<UInt64, UInt16>> : DivideIntegralByConstantImpl<UInt64, UInt16> {};
template <> struct BinaryOperationImpl<UInt64, UInt32, DivideIntegralImpl<UInt64, UInt32>> : DivideIntegralByConstantImpl<UInt64, UInt32> {};
template <> struct BinaryOperationImpl<UInt64, UInt64, DivideIntegralImpl<UInt64, UInt64>> : DivideIntegralByConstantImpl<UInt64, UInt64> {};

template <> struct BinaryOperationImpl<UInt32, UInt8, DivideIntegralImpl<UInt32, UInt8>> : DivideIntegralByConstantImpl<UInt32, UInt8> {};
template <> struct BinaryOperationImpl<UInt32, UInt16, DivideIntegralImpl<UInt32, UInt16>> : DivideIntegralByConstantImpl<UInt32, UInt16> {};
template <> struct BinaryOperationImpl<UInt32, UInt32, DivideIntegralImpl<UInt32, UInt32>> : DivideIntegralByConstantImpl<UInt32, UInt32> {};
template <> struct BinaryOperationImpl<UInt32, UInt64, DivideIntegralImpl<UInt32, UInt64>> : DivideIntegralByConstantImpl<UInt32, UInt64> {};

template <> struct BinaryOperationImpl<Int64, Int8, DivideIntegralImpl<Int64, Int8>> : DivideIntegralByConstantImpl<Int64, Int8> {};
template <> struct BinaryOperationImpl<Int64, Int16, DivideIntegralImpl<Int64, Int16>> : DivideIntegralByConstantImpl<Int64, Int16> {};
template <> struct BinaryOperationImpl<Int64, Int32, DivideIntegralImpl<Int64, Int32>> : DivideIntegralByConstantImpl<Int64, Int32> {};
template <> struct BinaryOperationImpl<Int64, Int64, DivideIntegralImpl<Int64, Int64>> : DivideIntegralByConstantImpl<Int64, Int64> {};

template <> struct BinaryOperationImpl<Int32, Int8, DivideIntegralImpl<Int32, Int8>> : DivideIntegralByConstantImpl<Int32, Int8> {};
template <> struct BinaryOperationImpl<Int32, Int16, DivideIntegralImpl<Int32, Int16>> : DivideIntegralByConstantImpl<Int32, Int16> {};
template <> struct BinaryOperationImpl<Int32, Int32, DivideIntegralImpl<Int32, Int32>> : DivideIntegralByConstantImpl<Int32, Int32> {};
template <> struct BinaryOperationImpl<Int32, Int64, DivideIntegralImpl<Int32, Int64>> : DivideIntegralByConstantImpl<Int32, Int64> {};
// clang-format on

namespace
{
// clang-format off
struct NameDivideFloating       { static constexpr auto name = "divide"; };
struct NameTiDBDivideFloating   { static constexpr auto name = "tidbDivide"; };
struct NameDivideIntegral       { static constexpr auto name = "intDiv"; };
struct NameDivideIntegralOrZero { static constexpr auto name = "intDivOrZero"; };
// clang-format on

using FunctionDivideFloating = FunctionBinaryArithmetic<DivideFloatingImpl_t, NameDivideFloating>;
using FunctionTiDBDivideFloating = FunctionBinaryArithmetic<TiDBDivideFloatingImpl_t, NameTiDBDivideFloating, false>;
using FunctionDivideIntegral = FunctionBinaryArithmetic<DivideIntegralImpl_t, NameDivideIntegral>;
using FunctionDivideIntegralOrZero = FunctionBinaryArithmetic<DivideIntegralOrZeroImpl_t, NameDivideIntegralOrZero>;

} // namespace

void registerFunctionDivideFloating(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDivideFloating>();
}

void registerFunctionTiDBDivideFloating(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBDivideFloating>();
}

void registerFunctionDivideIntegral(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDivideIntegral>();
}

void registerFunctionDivideIntegralOrZero(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDivideIntegralOrZero>();
}

} // namespace DB