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

#include <Functions/FunctionBinaryArithmetic.h>
#include <common/types.h>

#include <limits>

namespace DB
{
namespace
{
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct BitShiftRightImpl;

template <typename A, typename B>
struct BitShiftRightImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        // It is an undefined behavior for shift operation in c++ that the right operand is negative or greater than
        // or equal to the number of digits of the bits in the (promoted) left operand.
        // See https://en.cppreference.com/w/cpp/language/operator_arithmetic for details.
        if (static_cast<Result>(b) >= std::numeric_limits<decltype(static_cast<Result>(a))>::digits)
        {
            return static_cast<Result>(0);
        }
        // Note that we do not consider the case that the right operand is negative,
        // since other types will all be cast to uint64 before shift operation
        // according to DAGExpressionAnalyzerHelper::buildBitwiseFunction.
        // Therefore, we simply suppress clang-tidy checking here.
        return static_cast<Result>(a)
            >> static_cast<Result>(b); // NOLINT(clang-analyzer-core.UndefinedBinaryOperatorResult)
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BitShiftRightImpl<A, B, true>
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
        return BitShiftRightImpl<Result, Result>::apply(x, y);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

// clang-format off
struct NameBitShiftRight        { static constexpr auto name = "bitShiftRight"; };
// clang-format on

template <typename A, typename B>
using BitShiftRightImpl_t = BitShiftRightImpl<A, B>;
using FunctionBitShiftRight = FunctionBinaryArithmetic<BitShiftRightImpl_t, NameBitShiftRight>;

} // namespace

void registerFunctionBitShiftRight(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitShiftRight>();
}

} // namespace DB
