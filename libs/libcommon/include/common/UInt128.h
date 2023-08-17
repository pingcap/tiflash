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

#include <city.h>

#include <cstdint>
#include <functional>
#include <tuple>


#if __SSE4_2__
#include <nmmintrin.h>
#endif

namespace DB
{
/// For aggregation by SipHash, UUID type or concatenation of several fields.
struct UInt128
{
/// Suppress gcc7 warnings: 'prev_key.DB::UInt128::low' may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    /// This naming assumes little endian.
    uint64_t low;
    uint64_t high;

    UInt128() = default;
    explicit UInt128(const uint64_t rhs)
        : low(rhs)
        , high()
    {}
    UInt128(const uint64_t low, const uint64_t high)
        : low(low)
        , high(high)
    {}

    auto tuple() const { return std::tie(high, low); }

    bool operator==(const UInt128 & rhs) const { return low == rhs.low && high == rhs.high; }
    bool operator!=(const UInt128 & rhs) const { return !(*this == rhs); }
    bool operator<(const UInt128 & rhs) const { return high < rhs.high || (high == rhs.high && low < rhs.low); }
    bool operator<=(const UInt128 & rhs) const { return high < rhs.high || (high == rhs.high && low <= rhs.low); }
    bool operator>(const UInt128 & rhs) const { return !(*this <= rhs); }
    bool operator>=(const UInt128 & rhs) const { return !(*this < rhs); }

    template <typename T>
    __attribute__((always_inline)) bool operator==(T rhs) const
    {
        return *this == UInt128(rhs);
    }
    template <typename T>
    __attribute__((always_inline)) bool operator!=(T rhs) const
    {
        return *this != UInt128(rhs);
    }
    template <typename T>
    __attribute__((always_inline)) bool operator>=(T rhs) const
    {
        return *this >= UInt128(rhs);
    }
    template <typename T>
    __attribute__((always_inline)) bool operator>(T rhs) const
    {
        return *this > UInt128(rhs);
    }
    template <typename T>
    __attribute__((always_inline)) bool operator<=(T rhs) const
    {
        return *this <= UInt128(rhs);
    }
    template <typename T>
    __attribute__((always_inline)) bool operator<(T rhs) const
    {
        return *this < UInt128(rhs);
    }

    template <typename T>
    __attribute__((always_inline)) explicit operator T() const
    {
        return static_cast<T>(low);
    }

#if !__clang__
#pragma GCC diagnostic pop
#endif

    UInt128 & operator=(const uint64_t rhs)
    {
        low = rhs;
        high = 0;
        return *this;
    }
};

template <typename T>
bool inline operator==(T a, const UInt128 & b)
{
    return UInt128(a) == b;
}
template <typename T>
bool inline operator!=(T a, const UInt128 & b)
{
    return UInt128(a) != b;
}
template <typename T>
bool inline operator>=(T a, const UInt128 & b)
{
    return UInt128(a) >= b;
}
template <typename T>
bool inline operator>(T a, const UInt128 & b)
{
    return UInt128(a) > b;
}
template <typename T>
bool inline operator<=(T a, const UInt128 & b)
{
    return UInt128(a) <= b;
}
template <typename T>
bool inline operator<(T a, const UInt128 & b)
{
    return UInt128(a) < b;
}

/** Used for aggregation, for putting a large number of constant-length keys in a hash table.
  */
struct UInt256
{
/// Suppress gcc7 warnings: 'prev_key.DB::UInt256::a' may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    uint64_t a;
    uint64_t b;
    uint64_t c;
    uint64_t d;

    bool operator==(const UInt256 & rhs) const
    {
        return a == rhs.a && b == rhs.b && c == rhs.c && d == rhs.d;

        /* So it's no better.
        return 0xFFFF == _mm_movemask_epi8(_mm_and_si128(
            _mm_cmpeq_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&a)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&rhs.a))),
            _mm_cmpeq_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&c)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&rhs.c)))));*/
    }

    bool operator!=(const UInt256 & rhs) const { return !operator==(rhs); }

    bool operator==(const uint64_t & rhs) const { return a == rhs && b == 0 && c == 0 && d == 0; }
    bool operator!=(const uint64_t & rhs) const { return !operator==(rhs); }

#if !__clang__
#pragma GCC diagnostic pop
#endif

    UInt256 & operator=(const uint64_t & rhs)
    {
        a = rhs;
        b = 0;
        c = 0;
        d = 0;
        return *this;
    }
};
} // namespace DB

/// Overload hash for type casting
namespace std
{
template <>
struct hash<DB::UInt128> // NOLINT(readability-identifier-naming)
{
    size_t operator()(const DB::UInt128 & u) const { return CityHash_v1_0_2::Hash128to64({u.low, u.high}); }
};

} // namespace std
