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

#include <Functions/DivisionUtils.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
template <typename A, typename B>
struct ModuloImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        if constexpr (std::is_floating_point_v<Result>)
        {
            auto x = static_cast<Result>(a);
            auto y = static_cast<Result>(b);

            // assert no infinite or NaN values.
            assert(std::isfinite(x) && std::isfinite(y));

            // C++ does not allow operator% between floating point
            // values, so we call into std::fmod.
            return std::fmod(x, y);
        }
        else // both A and B are integrals.
        {
            // decimals are expected to be converted to integers or floating point values before computations.
            static_assert(is_integer_v<Result>);

            // convert to unsigned before computing.
            // we have to prevent wrong result like UInt64(5) = UInt64(5) % Int64(-3).
            // in MySQL, UInt64(5) % Int64(-3) evaluates to UInt64(2).
            auto x = toSafeUnsigned<Result>(a);
            auto y = toSafeUnsigned<Result>(b);

            auto result = static_cast<Result>(x % y);

            // in MySQL, the sign of a % b is the same as that of a.
            // e.g. 5 % -3 = 2, -5 % 3 = -2.
            if constexpr (is_signed_v<Result>)
            {
                if (a < 0)
                    return -result;
                else
                    return result;
            }
            else
                return result;
        }
    }
    template <typename Result = ResultType>
    static Result apply(A a, B b, UInt8 & res_null)
    {
        if (unlikely(b == 0))
        {
            res_null = 1;
            return static_cast<Result>(0);
        }

        return apply(a, b);
    }
};

template <typename A, typename B>
struct ModuloImpl<A, B, true>
{
    using ResultPrecInferer = ModDecimalInferer;
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>)
            x = static_cast<Result>(a.value);
        else
            x = static_cast<Result>(a);
        if constexpr (IsDecimal<B>)
            y = static_cast<Result>(b.value);
        else
            y = static_cast<Result>(b);

        return ModuloImpl<Result, Result>::apply(x, y);
    }
    template <typename Result = ResultType>
    static Result apply(A a, B b, UInt8 & res_null)
    {
        if (unlikely(b == 0))
        {
            res_null = 1;
            return static_cast<Result>(0);
        }

        return apply(a, b);
    }
};

/// Optimizations for integer division by a constant.

#if __SSE2__
#define LIBDIVIDE_SSE2 1
#endif

#include <libdivide.h>

template <typename A, typename B>
struct ModuloByConstantImpl : BinaryOperationImplBase<A, B, ModuloImpl<A, B>>
{
    using ResultType = typename ModuloImpl<A, B>::ResultType;

    static void vectorConstant(const PaddedPODArray<A> & a, B b, PaddedPODArray<ResultType> & c)
    {
        if (unlikely(b == 0))
            throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

        if (unlikely((std::is_signed_v<B> && b == -1) || b == 1))
        {
            size_t size = a.size();
            for (size_t i = 0; i < size; ++i)
                c[i] = 0;
            return;
        }

#pragma GCC diagnostic pop

        libdivide::divider<A> divider(b);

        /// Here we failed to make the SSE variant from libdivide give an advantage.
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = a[i]
                - (a[i] / divider)
                    * b; /// NOTE: perhaps, the division semantics with the remainder of negative numbers is not preserved.
    }
};


/** Specializations are specified for dividing numbers of the type UInt64 and UInt32 by the numbers of the same sign.
  * Can be expanded to all possible combinations, but more code is needed.
  */

// clang-format off
template <> struct BinaryOperationImpl<UInt64, UInt8, ModuloImpl<UInt64, UInt8>> : ModuloByConstantImpl<UInt64, UInt8> {};
template <> struct BinaryOperationImpl<UInt64, UInt16, ModuloImpl<UInt64, UInt16>> : ModuloByConstantImpl<UInt64, UInt16> {};
template <> struct BinaryOperationImpl<UInt64, UInt32, ModuloImpl<UInt64, UInt32>> : ModuloByConstantImpl<UInt64, UInt32> {};
template <> struct BinaryOperationImpl<UInt64, UInt64, ModuloImpl<UInt64, UInt64>> : ModuloByConstantImpl<UInt64, UInt64> {};

template <> struct BinaryOperationImpl<UInt32, UInt8, ModuloImpl<UInt32, UInt8>> : ModuloByConstantImpl<UInt32, UInt8> {};
template <> struct BinaryOperationImpl<UInt32, UInt16, ModuloImpl<UInt32, UInt16>> : ModuloByConstantImpl<UInt32, UInt16> {};
template <> struct BinaryOperationImpl<UInt32, UInt32, ModuloImpl<UInt32, UInt32>> : ModuloByConstantImpl<UInt32, UInt32> {};
template <> struct BinaryOperationImpl<UInt32, UInt64, ModuloImpl<UInt32, UInt64>> : ModuloByConstantImpl<UInt32, UInt64> {};

template <> struct BinaryOperationImpl<Int64, Int8, ModuloImpl<Int64, Int8>> : ModuloByConstantImpl<Int64, Int8> {};
template <> struct BinaryOperationImpl<Int64, Int16, ModuloImpl<Int64, Int16>> : ModuloByConstantImpl<Int64, Int16> {};
template <> struct BinaryOperationImpl<Int64, Int32, ModuloImpl<Int64, Int32>> : ModuloByConstantImpl<Int64, Int32> {};
template <> struct BinaryOperationImpl<Int64, Int64, ModuloImpl<Int64, Int64>> : ModuloByConstantImpl<Int64, Int64> {};

template <> struct BinaryOperationImpl<Int32, Int8, ModuloImpl<Int32, Int8>> : ModuloByConstantImpl<Int32, Int8> {};
template <> struct BinaryOperationImpl<Int32, Int16, ModuloImpl<Int32, Int16>> : ModuloByConstantImpl<Int32, Int16> {};
template <> struct BinaryOperationImpl<Int32, Int32, ModuloImpl<Int32, Int32>> : ModuloByConstantImpl<Int32, Int32> {};
template <> struct BinaryOperationImpl<Int32, Int64, ModuloImpl<Int32, Int64>> : ModuloByConstantImpl<Int32, Int64> {};
// clang-format on

namespace
{
// clang-format off
struct NameModulo               { static constexpr auto name = "modulo"; };
// clang-format on

using FunctionModulo = FunctionBinaryArithmetic<ModuloImpl_t, NameModulo, false>;

} // namespace

void registerFunctionModulo(FunctionFactory & factory)
{
    factory.registerFunction<FunctionModulo>();
}

} // namespace DB