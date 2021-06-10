#pragma once

#include <tuple>
#include <city.h>

#if __SSE4_2__
#include <nmmintrin.h>
#endif

namespace DB
{

/// For aggregation by SipHash, UUID type or concatenation of several fields.
struct UInt128
{
/// Suppress gcc7 warnings: 'prev_key.DB::UInt128::items' may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    // little endian: items[0] is low, items[1] is high.
    uint64_t items[2];

    UInt128() = default;
    explicit UInt128(uint64_t rhs) : items{rhs, 0} {}
    UInt128(uint64_t low, uint64_t high) : items{low, high} {}

    auto tuple() const { return std::tie(items[1], items[0]); }

    bool operator== (const UInt128 & rhs) const { return items[0] == rhs.items[0] && items[1] == rhs.items[1]; }
    bool operator!= (const UInt128 & rhs) const { return !(*this == rhs); }
    bool operator<  (const UInt128 & rhs) const { return items[1] < rhs.items[1] || (items[1] == rhs.items[1] && items[0] < rhs.items[0]); }
    bool operator<= (const UInt128 & rhs) const { return items[1] < rhs.items[1] || (items[1] == rhs.items[1] && items[0] <= rhs.items[0]); }
    bool operator>  (const UInt128 & rhs) const { return !(*this <= rhs); }
    bool operator>= (const UInt128 & rhs) const { return !(*this < rhs); }

    template <typename T> bool operator== (T rhs) const { return *this == UInt128(rhs); }
    template <typename T> bool operator!= (T rhs) const { return *this != UInt128(rhs); }
    template <typename T> bool operator>= (T rhs) const { return *this >= UInt128(rhs); }
    template <typename T> bool operator>  (T rhs) const { return *this >  UInt128(rhs); }
    template <typename T> bool operator<= (T rhs) const { return *this <= UInt128(rhs); }
    template <typename T> bool operator<  (T rhs) const { return *this <  UInt128(rhs); }

    template <typename T> explicit operator T() const { return static_cast<T>(items[0]); }

#if !__clang__
#pragma GCC diagnostic pop
#endif

    UInt128 & operator= (const uint64_t rhs) { items[0] = rhs; items[1] = 0; return *this; }
};

template <typename T> bool inline operator== (T a, const UInt128 & b) { return UInt128(a) == b; }
template <typename T> bool inline operator!= (T a, const UInt128 & b) { return UInt128(a) != b; }
template <typename T> bool inline operator>= (T a, const UInt128 & b) { return UInt128(a) >= b; }
template <typename T> bool inline operator>  (T a, const UInt128 & b) { return UInt128(a) > b; }
template <typename T> bool inline operator<= (T a, const UInt128 & b) { return UInt128(a) <= b; }
template <typename T> bool inline operator<  (T a, const UInt128 & b) { return UInt128(a) < b; }

/** Used for aggregation, for putting a large number of constant-length keys in a hash table.
  */
struct UInt256
{

/// Suppress gcc7 warnings: 'prev_key.DB::UInt256::items' may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    uint64_t items[4];

    bool operator== (const UInt256 & rhs) const
    {
        return
            items[0] == rhs.items[0] &&
            items[1] == rhs.items[1] &&
            items[2] == rhs.items[2] &&
            items[3] == rhs.items[3];

    /* So it's no better.
        return 0xFFFF == _mm_movemask_epi8(_mm_and_si128(
            _mm_cmpeq_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(items)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(rhs.items))),
            _mm_cmpeq_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(items + 2)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(rhs.items + 2)))));*/
    }

    bool operator!= (const UInt256 & rhs) const { return !operator==(rhs); }

    bool operator== (uint64_t rhs) const { return items[0] == rhs && items[1] == 0 && items[2] == 0 && items[3] == 0; }
    bool operator!= (uint64_t rhs) const { return !operator==(rhs); }

#if !__clang__
#pragma GCC diagnostic pop
#endif

    UInt256 & operator= (uint64_t rhs) { items[0] = rhs; items[1] = 0; items[2] = 0; items[3] = 0; return *this; }
};
} // namespace DB

/// Overload hash for type casting
namespace std
{
template <> struct hash<DB::UInt128>
{
    size_t operator()(const DB::UInt128 & u) const
    {
        return CityHash_v1_0_2::Hash128to64({u.items[0], u.items[1]});
    }
};

}
