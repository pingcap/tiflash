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

#include <Common/Decimal.h>
#include <Core/Types.h>

#include <type_traits>


namespace DB
{
/** Allows get the result type of the functions +, -, *, /, %, intDiv (integer division).
  * The rules are different from those used in C++.
  */

namespace NumberTraits
{
struct Error
{
};

constexpr size_t max(size_t x, size_t y)
{
    return x > y ? x : y;
}

constexpr size_t min(size_t x, size_t y)
{
    return x < y ? x : y;
}

constexpr size_t nextSize(size_t size)
{
    return min(size * 2, 8);
}

template <bool is_signed, bool is_floating, size_t size>
struct Construct
{
    using Type = Error;
};

/**
 * TODO:
 * 1. support wide integers (Int128/Int256) needed by Decimal.
 * 2. for floating point numbers Type should always be Float64.
 */
template <>
struct Construct<false, false, 1>
{
    using Type = UInt8;
};
template <>
struct Construct<false, false, 2>
{
    using Type = UInt16;
};
template <>
struct Construct<false, false, 4>
{
    using Type = UInt32;
};
template <>
struct Construct<false, false, 8>
{
    using Type = UInt64;
};
template <>
struct Construct<false, true, 1>
{
    using Type = Float32;
};
template <>
struct Construct<false, true, 2>
{
    using Type = Float32;
};
template <>
struct Construct<false, true, 4>
{
    using Type = Float32;
};
template <>
struct Construct<false, true, 8>
{
    using Type = Float64;
};
template <>
struct Construct<true, false, 1>
{
    using Type = Int8;
};
template <>
struct Construct<true, false, 2>
{
    using Type = Int16;
};
template <>
struct Construct<true, false, 4>
{
    using Type = Int32;
};
template <>
struct Construct<true, false, 8>
{
    using Type = Int64;
};
template <>
struct Construct<true, true, 1>
{
    using Type = Float32;
};
template <>
struct Construct<true, true, 2>
{
    using Type = Float32;
};
template <>
struct Construct<true, true, 4>
{
    using Type = Float32;
};
template <>
struct Construct<true, true, 8>
{
    using Type = Float64;
};


/** The result of addition or multiplication is calculated according to the following rules:
    * - if one of the arguments is floating-point, the result is a floating point, otherwise - the whole;
    * - if one of the arguments is signed, the result is signed, otherwise it is unsigned;
    * - the result contains more bits (not only meaningful) than the maximum in the arguments
    *   (for example, UInt8 + Int32 = Int64).
    */
template <typename A, typename B>
struct ResultOfAdditionMultiplication
{
    using Type = typename Construct<
        std::is_signed_v<A> || std::is_signed_v<B>,
        std::is_floating_point_v<A> || std::is_floating_point_v<B>,
        nextSize(max(sizeof(A), sizeof(B)))>::Type;
};

template <typename A, typename B>
struct ResultOfSubtraction
{
    using Type = typename Construct<
        true,
        std::is_floating_point_v<A> || std::is_floating_point_v<B>,
        nextSize(max(sizeof(A), sizeof(B)))>::Type;
};

/** When dividing, you always get a floating-point number.
    */
template <typename A, typename B>
struct ResultOfFloatingPointDivision
{
    using Type = Float64;
};

/** For integer division, we get a number with the same number of bits as in divisible.
    */
template <typename A, typename B>
struct ResultOfIntegerDivision
{
    using Type = typename Construct<std::is_signed_v<A> || std::is_signed_v<B>, false, sizeof(A)>::Type;
};

template <size_t size>
struct ConstructIntegerBySize
{
    using Type = Error;
};

template <>
struct ConstructIntegerBySize<1>
{
    using Type = Int8;
};
template <>
struct ConstructIntegerBySize<2>
{
    using Type = Int16;
};
template <>
struct ConstructIntegerBySize<4>
{
    using Type = Int32;
};
template <>
struct ConstructIntegerBySize<8>
{
    using Type = Int64;
};
template <>
struct ConstructIntegerBySize<16>
{
    using Type = Int128;
};
template <>
struct ConstructIntegerBySize<32>
{
    using Type = Int256;
};
template <>
struct ConstructIntegerBySize<64>
{
    using Type = Int512;
};

/** Division with remainder you get a number with the same number of bits as in divisor.
    */
template <typename A, typename B>
struct ResultOfModulo
{
    static constexpr auto result_size = std::max(actual_size_v<A>, actual_size_v<B>);

    using IntegerType = typename ConstructIntegerBySize<result_size>::Type;

    /**
     * in MySQL:
     * * if A or B is floating-point, A % B evalutes to Float64.
     * * unsigned int % signed int evaluates to unsigned int, but signed int % unsigned int evaluates to signed int.
     * * the precision of A % B is the maximum precision of A and B.
     */
    using Type = std::conditional_t<
        std::is_floating_point_v<A> || std::is_floating_point_v<B>,
        Float64,
        std::conditional_t<is_signed_v<A>, IntegerType, make_unsigned_t<IntegerType>>>;
};

template <typename A>
struct ResultOfNegate
{
    using Type =
        typename Construct<true, std::is_floating_point_v<A>, std::is_signed_v<A> ? sizeof(A) : nextSize(sizeof(A))>::
            Type;
};

template <typename T>
struct ResultOfNegate<Decimal<T>>
{
    using Type = Decimal<T>;
};

template <typename A>
struct ResultOfAbs
{
    using Type = typename Construct<false, std::is_floating_point_v<A>, sizeof(A)>::Type;
};

template <typename T>
struct ResultOfAbs<Decimal<T>>
{
    using Type = Decimal<T>;
};

/** For bitwise operations, an integer is obtained with number of bits is equal to the maximum of the arguments.
  * todo: note that MySQL handles only unsigned 64-bit integer argument and result values. We should refine the code.
    */
template <typename A, typename B>
struct ResultOfBit
{
    using Type = typename Construct<
        std::is_signed_v<A> || std::is_signed_v<B>,
        false,
        std::is_floating_point_v<A> || std::is_floating_point_v<B> ? 8 : max(sizeof(A), sizeof(B))>::Type;
};

template <typename A>
struct ResultOfBitNot
{
    using Type = typename Construct<std::is_signed_v<A>, false, sizeof(A)>::Type;
};

template <typename T>
struct ResultOfBitNot<Decimal<T>>
{
    using Type = Decimal<T>;
};

/** Type casting for `if` function:
  * UInt<x>,  UInt<y>   ->  UInt<max(x,y)>
  * Int<x>,   Int<y>    ->   Int<max(x,y)>
  * Float<x>, Float<y>  -> Float<max(x, y)>
  * UInt<x>,  Int<y>    ->   Int<max(x*2, y)>
  * Float<x>, [U]Int<y> -> Float<max(x, y*2)>
  * UInt64 ,  Int<x>    -> Error
  * Float<x>, [U]Int64  -> Error
  */
template <typename A, typename B>
struct ResultOfIf
{
    static constexpr bool has_float = std::is_floating_point_v<A> || std::is_floating_point_v<B>;
    static constexpr bool has_integer = std::is_integral_v<A> || std::is_integral_v<B>;
    static constexpr bool has_signed = std::is_signed_v<A> || std::is_signed_v<B>;
    static constexpr bool has_unsigned = !std::is_signed_v<A> || !std::is_signed_v<B>;

    static constexpr size_t max_size_of_unsigned_integer
        = max(std::is_signed_v<A> ? 0 : sizeof(A), std::is_signed_v<B> ? 0 : sizeof(B));
    static constexpr size_t max_size_of_signed_integer
        = max(std::is_signed_v<A> ? sizeof(A) : 0, std::is_signed_v<B> ? sizeof(B) : 0);
    static constexpr size_t max_size_of_integer
        = max(std::is_integral_v<A> ? sizeof(A) : 0, std::is_integral_v<B> ? sizeof(B) : 0);
    static constexpr size_t max_size_of_float
        = max(std::is_floating_point_v<A> ? sizeof(A) : 0, std::is_floating_point_v<B> ? sizeof(B) : 0);

    using Type = typename Construct<
        has_signed,
        has_float,
        ((has_float && has_integer && max_size_of_integer >= max_size_of_float)
         || (has_signed && has_unsigned && max_size_of_unsigned_integer >= max_size_of_signed_integer))
            ? max(sizeof(A), sizeof(B)) * 2
            : max(sizeof(A), sizeof(B))>::Type;
};

/** Before applying bitwise operations, operands are casted to whole numbers. */
template <typename A>
struct ToInteger
{
    using Type = typename Construct<std::is_signed_v<A>, false, std::is_floating_point_v<A> ? 8 : sizeof(A)>::Type;
};

template <>
struct ToInteger<Int256>
{
    using Type = Int256;
};
template <>
struct ToInteger<Int128>
{
    using Type = Int128;
};

// For greatest/least of TiDB:
// float + int/float/decimal = double
// tinyint/smallint/mediumint unsigned + tinyint/smallint/mediumint = bigint
// bigint unsigned + bigint = decimal(20, 0), TiDB will add cast for this situation, so no need handle in tiflash.
template <typename A, typename B>
struct ResultOfBinaryLeastGreatest
{
    static_assert(is_arithmetic_v<A> && is_arithmetic_v<B>);
    using Type = std::conditional_t<
        std::is_floating_point_v<A> || std::is_floating_point_v<B>,
        Float64,
        std::conditional_t<std::is_unsigned_v<A> && std::is_unsigned_v<B>, UInt64, Int64>>;
};

} // namespace NumberTraits

} // namespace DB
