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

#pragma once
#include <Functions/FunctionBinaryArithmetic.h>
#include <limits>
#include <type_traits>
#include "common/types.h"

namespace DB
{

// WARNING this is demo
template <typename LeftType, typename RightType, typename ResultType>
void checkPlusOverflow(LeftType left, RightType right)
{
    constexpr bool is_left_signed = std::is_signed_v<LeftType>;
    constexpr bool is_right_signed = std::is_signed_v<RightType>;
    constexpr bool is_result_signed = std::is_signed_v<ResultType>;

    if constexpr (is_left_signed && is_right_signed)
    {
        if ((left > 0 && right > std::numeric_limits<Int64>::max()-left) || (left < 0 && right < std::numeric_limits<Int64>::min() - left))
            throw Exception("overflow");
    }
    else if constexpr (!is_left_signed && !is_right_signed)
    {
        if (left > std::numeric_limits<UInt64>::max() - right)
            throw Exception("overflow");
    }
    else if constexpr (is_left_signed && !is_right_signed)
    {
        if constexpr (is_result_signed)
        {
            if (left > 0 && static_cast<UInt64>(left) > std::numeric_limits<Int64>::max() - right)
            throw Exception("overflow");
        }
        else
        {
            if (left < 0 && (-left) > right )
                throw Exception("overflow");
            if (left > 0 && left > std::numeric_limits<UInt64>::max() - right)
                throw Exception("overflow");
        }
    }
    else if constexpr (!is_left_signed && is_right_signed)
    {
        if constexpr (is_result_signed)
        {
            if (right > 0 && static_cast<UInt64>(right) > std::numeric_limits<Int64>::max() - left)
            throw Exception("overflow");
        }
        else
        {
            if (right < 0 && (-right) > left )
                throw Exception("overflow");
            if (right > 0 && right > std::numeric_limits<UInt64>::max() - left)
                throw Exception("overflow");
        }
    }
}

template <typename A, typename B>
struct PlusImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        // if constexpr ((std::is_same_v<A, Int64> || std::is_same_v<A, UInt64>) && (std::is_same_v<B, Int64> || std::is_same_v<B, UInt64>))
        //     checkPlusOverflow<A, B, ResultType>(a, b);
        
        /// Next everywhere, static_cast - so that there is no wrong result in expressions of the form Int64 c = UInt32(a) * Int32(-1).
        return static_cast<Result>(a) + b;
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct PlusImpl<A, B, true>
{
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;
    using ResultPrecInferer = PlusDecimalInferer;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) + static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

// clang-format off
struct NamePlus                 { static constexpr auto name = "plus"; };
// clang-format on

using FunctionPlus = FunctionBinaryArithmetic<PlusImpl_t, NamePlus>;

} // namespace DB