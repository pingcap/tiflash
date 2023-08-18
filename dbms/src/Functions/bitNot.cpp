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

#include <Functions/FunctionUnaryArithmetic.h>

namespace DB
{
namespace
{
template <typename A>
struct BitNotImpl
{
    using ResultType = typename NumberTraits::ResultOfBitNot<A>::Type;

    static ResultType apply(A a [[maybe_unused]])
    {
        if constexpr (IsDecimal<A>)
            throw Exception("unimplement");
        else
            return ~static_cast<ResultType>(a);
    }
};

// clang-format off
struct NameBitNot               { static constexpr auto name = "bitNot"; };
// clang-format on

using FunctionBitNot = FunctionUnaryArithmetic<BitNotImpl, NameBitNot, true>;

} // namespace

template <>
struct FunctionUnaryArithmeticMonotonicity<NameBitNot>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const Field &, const Field &) { return {}; }
};

void registerFunctionBitNot(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitNot>();
}

} // namespace DB