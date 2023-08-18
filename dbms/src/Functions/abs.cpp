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
struct AbsImpl
{
    using ResultType = typename NumberTraits::ResultOfAbs<A>::Type;

    static ResultType apply(A a)
    {
        if constexpr (std::is_integral_v<A> && std::is_signed_v<A>)
        {
            // keep the same behavior as mysql and tidb, even though error no is not the same.
            if unlikely (a == INT64_MIN)
            {
                throw Exception("BIGINT value is out of range in 'abs(-9223372036854775808)'");
            }
            return a < 0 ? static_cast<ResultType>(~a) + 1 : a;
        }
        else if constexpr (std::is_integral_v<A> && std::is_unsigned_v<A>)
            return static_cast<ResultType>(a);
        else if constexpr (std::is_floating_point_v<A>)
            return static_cast<ResultType>(std::abs(a));
        else if constexpr (IsDecimal<A>)
            return a.value < 0 ? -a.value : a.value;
    }
};

// clang-format off
struct NameAbs                  { static constexpr auto name = "abs"; };
// clang-format on

using FunctionAbs = FunctionUnaryArithmetic<AbsImpl, NameAbs, false>;

} // namespace

template <>
struct FunctionUnaryArithmeticMonotonicity<NameAbs>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity()
                                           : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity()
                                             : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if ((left_float < 0 && right_float > 0) || (left_float > 0 && right_float < 0))
            return {};

        return {true, (left_float > 0)};
    }
};

void registerFunctionAbs(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAbs>();
}

} // namespace DB