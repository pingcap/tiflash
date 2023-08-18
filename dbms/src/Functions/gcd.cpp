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
namespace
{
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct GCDImpl;

template <typename A, typename B>
struct GCDImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(
            typename NumberTraits::ToInteger<A>::Type(a),
            typename NumberTraits::ToInteger<B>::Type(b));
        throwIfDivisionLeadsToFPE(
            typename NumberTraits::ToInteger<B>::Type(b),
            typename NumberTraits::ToInteger<A>::Type(a));
        return boost::integer::gcd(
            typename NumberTraits::ToInteger<Result>::Type(a),
            typename NumberTraits::ToInteger<Result>::Type(b));
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct GCDImpl<A, B, true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return GCDImpl<Result, Result>::apply(static_cast<Result>(a), static_cast<Result>(b));
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

// clang-format off
struct NameGCD                  { static constexpr auto name = "gcd"; };
// clang-format on

template <typename A, typename B>
using GCDImpl_t = GCDImpl<A, B>;
using FunctionGCD = FunctionBinaryArithmetic<GCDImpl_t, NameGCD>;

} // namespace

void registerFunctionGCD(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGCD>();
}

} // namespace DB