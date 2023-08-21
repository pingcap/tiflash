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
#include <Functions/LeastGreatest.h>

namespace DB
{
template <typename A, typename B>
struct BinaryGreatestBaseImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfBinaryLeastGreatest<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        const auto tmp_a = static_cast<Result>(a); // NOLINT(bugprone-signed-char-misuse)
        const auto tmp_b = static_cast<Result>(b); // NOLINT(bugprone-signed-char-misuse)
        return accurate::greaterOp(tmp_a, tmp_b) ? tmp_a : tmp_b;
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BinaryGreatestBaseImpl<A, B, true>
{
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;
    using ResultPrecInferer = PlusDecimalInferer;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        const auto tmp_a = static_cast<Result>(a); // NOLINT(bugprone-signed-char-misuse)
        const auto tmp_b = static_cast<Result>(b); // NOLINT(bugprone-signed-char-misuse)
        return tmp_a > tmp_b ? tmp_a : tmp_b;
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

namespace
{
// clang-format off
struct NameGreatest             { static constexpr auto name = "greatest"; };
// clang-format on

using FunctionBinaryGreatest = FunctionBinaryArithmetic<BinaryGreatestBaseImpl_t, NameGreatest>;
using FunctionTiDBGreatest = FunctionVectorizedLeastGreatest<GreatestImpl, FunctionBinaryGreatest>;

} // namespace

void registerFunctionGreatest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBGreatest>();
    factory.registerFunction<FunctionLeastGreatestString<false>>();
}

} // namespace DB
