// Modified from: https://github.com/ClickHouse/ClickHouse/blob/30fcaeb2a3fff1bf894aae9c776bed7fd83f783f/libs/libcommon/include/common/intExp.h
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

#include <cstdint>
#include <limits>


/// On overlow, the function returns unspecified value.

inline uint64_t intExp2(int x)
{
    return 1ULL << x;
}

inline uint64_t intExp10(int x)
{
    if (x < 0)
        return 0;
    if (x > 19)
        return std::numeric_limits<uint64_t>::max();

    static const uint64_t table[20]
        = {1ULL,
           10ULL,
           100ULL,
           1000ULL,
           10000ULL,
           100000ULL,
           1000000ULL,
           10000000ULL,
           100000000ULL,
           1000000000ULL,
           10000000000ULL,
           100000000000ULL,
           1000000000000ULL,
           10000000000000ULL,
           100000000000000ULL,
           1000000000000000ULL,
           10000000000000000ULL,
           100000000000000000ULL,
           1000000000000000000ULL,
           10000000000000000000ULL};

    return table[x];
}

constexpr int64_t exp10_i64_table[]
    = {1LL,
       10LL,
       100LL,
       1000LL,
       10000LL,
       100000LL,
       1000000LL,
       10000000LL,
       100000000LL,
       1000000000LL,
       10000000000LL,
       100000000000LL,
       1000000000000LL,
       10000000000000LL,
       100000000000000LL,
       1000000000000000LL,
       10000000000000000LL,
       100000000000000000LL,
       1000000000000000000LL};

constexpr Int128 exp10_i128_table[]
    = {static_cast<Int128>(1LL),
       static_cast<Int128>(10LL),
       static_cast<Int128>(100LL),
       static_cast<Int128>(1000LL),
       static_cast<Int128>(10000LL),
       static_cast<Int128>(100000LL),
       static_cast<Int128>(1000000LL),
       static_cast<Int128>(10000000LL),
       static_cast<Int128>(100000000LL),
       static_cast<Int128>(1000000000LL),
       static_cast<Int128>(10000000000LL),
       static_cast<Int128>(100000000000LL),
       static_cast<Int128>(1000000000000LL),
       static_cast<Int128>(10000000000000LL),
       static_cast<Int128>(100000000000000LL),
       static_cast<Int128>(1000000000000000LL),
       static_cast<Int128>(10000000000000000LL),
       static_cast<Int128>(100000000000000000LL),
       static_cast<Int128>(1000000000000000000LL),
       static_cast<Int128>(1000000000000000000LL) * 10LL,
       static_cast<Int128>(1000000000000000000LL) * 100LL,
       static_cast<Int128>(1000000000000000000LL) * 1000LL,
       static_cast<Int128>(1000000000000000000LL) * 10000LL,
       static_cast<Int128>(1000000000000000000LL) * 100000LL,
       static_cast<Int128>(1000000000000000000LL) * 1000000LL,
       static_cast<Int128>(1000000000000000000LL) * 10000000LL,
       static_cast<Int128>(1000000000000000000LL) * 100000000LL,
       static_cast<Int128>(1000000000000000000LL) * 1000000000LL,
       static_cast<Int128>(1000000000000000000LL) * 10000000000LL,
       static_cast<Int128>(1000000000000000000LL) * 100000000000LL,
       static_cast<Int128>(1000000000000000000LL) * 1000000000000LL,
       static_cast<Int128>(1000000000000000000LL) * 10000000000000LL,
       static_cast<Int128>(1000000000000000000LL) * 100000000000000LL,
       static_cast<Int128>(1000000000000000000LL) * 1000000000000000LL,
       static_cast<Int128>(1000000000000000000LL) * 10000000000000000LL,
       static_cast<Int128>(1000000000000000000LL) * 100000000000000000LL,
       static_cast<Int128>(1000000000000000000LL) * 100000000000000000LL * 10LL,
       static_cast<Int128>(1000000000000000000LL) * 100000000000000000LL * 100LL,
       static_cast<Int128>(1000000000000000000LL) * 100000000000000000LL * 1000LL};

constexpr Int256 i10e18{1000000000000000000ll};
constexpr Int256 exp10_i256_table[] = {
    static_cast<Int256>(1ll),
    static_cast<Int256>(10ll),
    static_cast<Int256>(100ll),
    static_cast<Int256>(1000ll),
    static_cast<Int256>(10000ll),
    static_cast<Int256>(100000ll),
    static_cast<Int256>(1000000ll),
    static_cast<Int256>(10000000ll),
    static_cast<Int256>(100000000ll),
    static_cast<Int256>(1000000000ll),
    static_cast<Int256>(10000000000ll),
    static_cast<Int256>(100000000000ll),
    static_cast<Int256>(1000000000000ll),
    static_cast<Int256>(10000000000000ll),
    static_cast<Int256>(100000000000000ll),
    static_cast<Int256>(1000000000000000ll),
    static_cast<Int256>(10000000000000000ll),
    static_cast<Int256>(100000000000000000ll),
    i10e18,
    i10e18 * 10ll,
    i10e18 * 100ll,
    i10e18 * 1000ll,
    i10e18 * 10000ll,
    i10e18 * 100000ll,
    i10e18 * 1000000ll,
    i10e18 * 10000000ll,
    i10e18 * 100000000ll,
    i10e18 * 1000000000ll,
    i10e18 * 10000000000ll,
    i10e18 * 100000000000ll,
    i10e18 * 1000000000000ll,
    i10e18 * 10000000000000ll,
    i10e18 * 100000000000000ll,
    i10e18 * 1000000000000000ll,
    i10e18 * 10000000000000000ll,
    i10e18 * 100000000000000000ll,
    i10e18 * 100000000000000000ll * 10ll,
    i10e18 * 100000000000000000ll * 100ll,
    i10e18 * 100000000000000000ll * 1000ll,
    i10e18 * 100000000000000000ll * 10000ll,
    i10e18 * 100000000000000000ll * 100000ll,
    i10e18 * 100000000000000000ll * 1000000ll,
    i10e18 * 100000000000000000ll * 10000000ll,
    i10e18 * 100000000000000000ll * 100000000ll,
    i10e18 * 100000000000000000ll * 1000000000ll,
    i10e18 * 100000000000000000ll * 10000000000ll,
    i10e18 * 100000000000000000ll * 100000000000ll,
    i10e18 * 100000000000000000ll * 1000000000000ll,
    i10e18 * 100000000000000000ll * 10000000000000ll,
    i10e18 * 100000000000000000ll * 100000000000000ll,
    i10e18 * 100000000000000000ll * 1000000000000000ll,
    i10e18 * 100000000000000000ll * 10000000000000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 10ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 100ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 1000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 10000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 100000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 1000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 10000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 1000000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 10000000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 1000000000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 10000000000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 1000000000000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 10000000000000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 10ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 100ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 1000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 10000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 100000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 1000000ll,
    i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 10000000ll,
};

constexpr int exp10_i32(int x)
{
    if (x < 0)
        return 0;
    if (x > 9)
        return std::numeric_limits<int>::max();

    constexpr int exp10_i32_table[10] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};
    return exp10_i32_table[x];
}

constexpr int64_t exp10_i64(int x)
{
    if (x < 0)
        return 0;
    if (x > 18)
        return std::numeric_limits<int64_t>::max();

    return exp10_i64_table[x];
}

constexpr Int128 exp10_i128(int x)
{
    if (x < 0)
        return 0;
    if (x > 38)
        return std::numeric_limits<Int128>::max();

    return exp10_i128_table[x];
}

constexpr Int256 exp10_i256(int x)
{
    if (x < 0)
        return 0;
    if (x > 76)
        return std::numeric_limits<Int256>::max();

    return exp10_i256_table[x];
}


/// intExp10 returning the type T.
template <typename T>
T intExp10OfSize(int x)
{
    if constexpr (sizeof(T) <= 4)
        return static_cast<T>(exp10_i32(x));
    else if constexpr (sizeof(T) <= 8)
        return exp10_i64(x);
    else if constexpr (sizeof(T) <= 16)
        return exp10_i128(x);
    else
        return exp10_i256(x);
}
