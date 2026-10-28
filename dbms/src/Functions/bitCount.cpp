// Copyright 2022 PingCAP, Ltd.
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

inline Int64 bit_count(Int64 value)
{
    value = value - ((value >> 1) & 0x5555555555555555);
    value = (value & 0x3333333333333333) + ((value >> 2) & 0x3333333333333333);
    value = (value & 0x0f0f0f0f0f0f0f0f) + ((value >> 4) & 0x0f0f0f0f0f0f0f0f);
    value = value + (value >> 8);
    value = value + (value >> 16);
    value = value + (value >> 32);
    value = value & 0x7f;
    return value;
}

namespace DB
{
namespace
{

template <typename A>
struct BitCountImpl
{
    using ResultType = Int64;

    static ResultType apply(A a)
    {
        return bit_count(a);
    }
};

template <typename A>
struct BitCountImpl<Decimal<A>>
{
    using ResultType = Int64;

    static ResultType apply(Decimal<A> a)
    {
        return bit_count(a);
    }
};


struct NameBitCount
{
    static constexpr auto name = "bitCount";
};

using FunctionBitCount = FunctionUnaryArithmetic<BitCountImpl, NameBitCount, false>;

} // namespace

template <>
struct FunctionUnaryArithmeticMonotonicity<NameBitCount>
{
    static bool has() { return false; }

    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return {};
    }
};

void registerFunctionBitCount(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitCount>();
}

} // namespace DB
