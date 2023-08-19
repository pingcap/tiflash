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

#include <Common/NaNUtils.h>
#include <Core/Defines.h>
#include <Core/Types.h>
#include <common/DecomposedFloat.h>

#include <cmath>
#include <limits>

/** Preceptually-correct number comparisons.
  * Example: Int8(-1) != UInt8(255)
*/

namespace accurate
{
template <typename A, typename B>
bool lessOp(A a, B b)
{
    if constexpr (std::is_same_v<A, B>)
        return a < b;

    /// float vs float
    if constexpr (std::is_floating_point_v<A> && std::is_floating_point_v<B>)
        return a < b;

    /// anything vs NaN
    if (isNaN(a) || isNaN(b))
        return false;

    /// int vs int (including boost int)
    if constexpr (is_integer_v<A> && is_integer_v<B>)
    {
        /// same signedness
        if constexpr (is_signed_v<A> == is_signed_v<B>)
            return a < b;

        /// different signedness

        if constexpr (is_signed_v<A> && !is_signed_v<B>)
            return a < 0 || static_cast<make_unsigned_t<A>>(a) < b;

        if constexpr (!is_signed_v<A> && is_signed_v<B>)
            return b >= 0 && a < static_cast<make_unsigned_t<B>>(b);
    }

    /// int (including boost int) vs float
    if constexpr (is_integer_v<A> && std::is_floating_point_v<B>)
    {
        if constexpr (std::numeric_limits<A>::digits <= 32)
            return static_cast<double>(a) < static_cast<double>(b);

        return DecomposedFloat<B>(b).greater(a);
    }

    if constexpr (std::is_floating_point_v<A> && is_integer_v<B>)
    {
        if constexpr (std::numeric_limits<B>::digits <= 32)
            return static_cast<double>(a) < static_cast<double>(b);

        return DecomposedFloat<A>(a).less(b);
    }

    static_assert(is_integer_v<A> || std::is_floating_point_v<A>);
    static_assert(is_integer_v<B> || std::is_floating_point_v<B>);
    __builtin_unreachable();
}

template <typename A, typename B>
bool greaterOp(A a, B b)
{
    return lessOp(b, a);
}

template <typename A, typename B>
bool greaterOrEqualsOp(A a, B b)
{
    if (isNaN(a) || isNaN(b))
        return false;

    return !lessOp(a, b);
}

template <typename A, typename B>
bool lessOrEqualsOp(A a, B b)
{
    if (isNaN(a) || isNaN(b))
        return false;

    return !lessOp(b, a);
}

template <typename A, typename B>
bool equalsOp(A a, B b)
{
    if constexpr (std::is_same_v<A, B>)
        return a == b;

    /// float vs float
    if constexpr (std::is_floating_point_v<A> && std::is_floating_point_v<B>)
        return a == b;

    /// anything vs NaN
    if (isNaN(a) || isNaN(b))
        return false;

    /// int vs int (including boost int)
    if constexpr (is_integer_v<A> && is_integer_v<B>)
    {
        /// same signedness
        if constexpr (is_signed_v<A> == is_signed_v<B>)
            return a == b;

        /// different signedness

        if constexpr (is_signed_v<A> && !is_signed_v<B>)
            return a >= 0 && static_cast<make_unsigned_t<A>>(a) == b;

        if constexpr (!is_signed_v<A> && is_signed_v<B>)
            return b >= 0 && a == static_cast<make_unsigned_t<B>>(b);
    }

    /// int (including boost int) vs float
    if constexpr (is_integer_v<A> && std::is_floating_point_v<B>)
    {
        if constexpr (sizeof(A) <= 4)
            return static_cast<double>(a) == static_cast<double>(b);

        return DecomposedFloat<B>(b).equals(a);
    }

    if constexpr (std::is_floating_point_v<A> && is_integer_v<B>)
    {
        if constexpr (sizeof(B) <= 4)
            return static_cast<double>(a) == static_cast<double>(b);

        return DecomposedFloat<A>(a).equals(b);
    }

    /// e.g comparing String with integer.
    return false;
}

template <typename A, typename B>
bool notEqualsOp(A a, B b)
{
    return !equalsOp(a, b);
}

} // namespace accurate


namespace DB
{
template <typename A, typename B>
struct EqualsOp
{
    /// An operation that gives the same result, if arguments are passed in reverse order.
    using SymmetricOp = EqualsOp<B, A>;

    static UInt8 apply(A a, B b) { return accurate::equalsOp(a, b); }
};

template <typename A, typename B>
struct NotEqualsOp
{
    using SymmetricOp = NotEqualsOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::notEqualsOp(a, b); }
};

template <typename A, typename B>
struct GreaterOp;

template <typename A, typename B>
struct LessOp
{
    using SymmetricOp = GreaterOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::lessOp(a, b); }
};

template <typename A, typename B>
struct GreaterOp
{
    using SymmetricOp = LessOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::greaterOp(a, b); }
};

template <typename A, typename B>
struct GreaterOrEqualsOp;

template <typename A, typename B>
struct LessOrEqualsOp
{
    using SymmetricOp = GreaterOrEqualsOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::lessOrEqualsOp(a, b); }
};

template <typename A, typename B>
struct GreaterOrEqualsOp
{
    using SymmetricOp = LessOrEqualsOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::greaterOrEqualsOp(a, b); }
};

template <typename A, typename B>
struct ReversedCmpOp;

template <typename A, typename B>
struct CmpOp
{
    using SymmetricOp = ReversedCmpOp<B, A>;
    static Int8 apply(A a, B b) { return accurate::equalsOp(a, b) ? 0 : (accurate::lessOp(a, b) ? -1 : 1); }
};

template <typename A, typename B>
struct ReversedCmpOp
{
    using SymmetricOp = CmpOp<B, A>;
    static Int8 apply(A a, B b) { return SymmetricOp::apply(b, a); }
};


} // namespace DB
