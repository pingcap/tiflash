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

namespace DB
{
namespace
{
template <typename T>
std::enable_if_t<std::is_integral_v<T> || std::is_same_v<T, Int128> || std::is_same_v<T, Int256>, T> toInteger(T x)
{
    return x;
}

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, Int64> toInteger(T x)
{
    return Int64(x);
}

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct BitTestImpl;

template <typename A, typename B>
struct BitTestImpl<A, B, false>
{
    using ResultType = UInt8;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>((toInteger(a) >> static_cast<int64_t>(toInteger(b))) & 1);
    };
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BitTestImpl<A, B, true>
{
    using ResultType = UInt8;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        if constexpr (!IsDecimal<B>)
        {
            return BitTestImpl<Result, Result>::apply(static_cast<int64_t>(a.value), b);
        }
        else if constexpr (!IsDecimal<A>)
        {
            return BitTestImpl<Result, Result>::apply(a, static_cast<int64_t>(b.value));
        }
        else
            return BitTestImpl<Result, Result>::apply(static_cast<int64_t>(a.value), static_cast<int64_t>(b.value));
        return {};
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

// clang-format off
struct NameBitTest              { static constexpr auto name = "bitTest"; };
// clang-format on

template <typename A, typename B>
using BitTestImpl_t = BitTestImpl<A, B>;
using FunctionBitTest = FunctionBinaryArithmetic<BitTestImpl_t, NameBitTest>;

} // namespace

void registerFunctionBitTest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTest>();
}

} // namespace DB