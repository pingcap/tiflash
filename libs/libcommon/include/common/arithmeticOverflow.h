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
#include "types.h"

namespace common
{
template <typename T>
inline bool addOverflow(T x, T y, T & res)
{
    return __builtin_add_overflow(x, y, &res);
}

template <>
inline bool addOverflow(int x, int y, int & res)
{
    return __builtin_sadd_overflow(x, y, &res);
}

template <>
inline bool addOverflow(long x, long y, long & res)
{
    return __builtin_saddl_overflow(x, y, &res);
}

template <>
inline bool addOverflow(long long x, long long y, long long & res)
{
    return __builtin_saddll_overflow(x, y, &res);
}

template <>
inline bool addOverflow(__int128 x, __int128 y, __int128 & res)
{
    static constexpr __int128 min_int128 = __int128(0x8000000000000000ll) << 64;
    static constexpr __int128 max_int128 = (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;
    res = x + y;
    return (y > 0 && x > max_int128 - y) || (y < 0 && x < min_int128 - y);
}

template <typename T>
inline bool subOverflow(T x, T y, T & res)
{
    return __builtin_sub_overflow(x, y, &res);
}

template <>
inline bool subOverflow(int x, int y, int & res)
{
    return __builtin_ssub_overflow(x, y, &res);
}

template <>
inline bool subOverflow(long x, long y, long & res)
{
    return __builtin_ssubl_overflow(x, y, &res);
}

template <>
inline bool subOverflow(long long x, long long y, long long & res)
{
    return __builtin_ssubll_overflow(x, y, &res);
}

template <>
inline bool subOverflow(__int128 x, __int128 y, __int128 & res)
{
    static constexpr __int128 min_int128 = __int128(0x8000000000000000ll) << 64;
    static constexpr __int128 max_int128 = (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;
    res = x - y;
    return (y < 0 && x > max_int128 + y) || (y > 0 && x < min_int128 + y);
}

template <typename T>
inline bool mulOverflow(T x, T y, T & res)
{
    return __builtin_mul_overflow(x, y, &res);
}

template <>
inline bool mulOverflow(int x, int y, int & res)
{
    return __builtin_smul_overflow(x, y, &res);
}

template <>
inline bool mulOverflow(long x, long y, long & res)
{
    return __builtin_smull_overflow(x, y, &res);
}

template <>
inline bool mulOverflow(long long x, long long y, long long & res)
{
    return __builtin_smulll_overflow(x, y, &res);
}

template <>
inline bool mulOverflow(__int128 x, __int128 y, __int128 & res)
{
    res = static_cast<unsigned __int128>(x) * static_cast<unsigned __int128>(y); /// Avoid signed integer overflow.
    if (!x || !y)
        return false;

    return res / x != y; /// whether overflow int128
}

/// Int256 doesn't use the complement representation to express negative values, but uses an extra bit to express the sign flag,
/// the actual range of Int256 is from -(2^256 - 1) to 2^256 - 1, so 2^255 ~ 2^256-1 do not overflow Int256.
template <>
inline bool mulOverflow(Int256 x, Int256 y, Int256 & res)
{
    try
    {
        res = x * y;
    }
    catch (std::overflow_error &)
    {
        return true;
    }
    return false;
}
} // namespace common
