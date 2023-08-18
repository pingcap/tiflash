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
struct IntExp10Impl
{
    using ResultType = UInt64;

    static ResultType apply(A a) { return intExp10(a); }
};

template <typename A>
struct IntExp10Impl<Decimal<A>>
{
    using ResultType = UInt64;

    static ResultType apply(Decimal<A> a) { return intExp10(a); }
};

// clang-format off
struct NameIntExp10             { static constexpr auto name = "intExp10"; };
// clang-format on

using FunctionIntExp10 = FunctionUnaryArithmetic<IntExp10Impl, NameIntExp10, true>;

} // namespace

template <>
struct FunctionUnaryArithmeticMonotonicity<NameIntExp10>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity()
                                           : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity()
                                             : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if (left_float < 0 || right_float > 19)
            return {};

        return {true};
    }
};

void registerFunctionIntExp10(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIntExp10>();
}

} // namespace DB