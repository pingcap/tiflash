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
#include <common/mem_utils.h>
#include <common/mem_utils_opt.h>
#include <common/types.h>
#include <common/unaligned.h>

#include <cassert>
#include <functional>
#include <iosfwd>
#include <string>
#include <vector>

#if defined(__SSE4_2__)
#include <nmmintrin.h>
#include <smmintrin.h>
#endif


/**
 * The std::string_view-like container to avoid creating strings to find substrings in the hash table.
 */
struct StringRef
{
    const char * data = nullptr;
    size_t size = 0;

    /// Non-constexpr due to reinterpret_cast.
    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    StringRef(const CharT * data_, size_t size_)
        : data(reinterpret_cast<const char *>(data_))
        , size(size_)
    {
        /// Sanity check for overflowed values.
        assert(size < 0x8000000000000000ULL);
    }

    constexpr StringRef(const char * data_, size_t size_)
        : data(data_)
        , size(size_)
    {}

    StringRef(const std::string & s) // NOLINT(google-explicit-constructor)
        : data(s.data())
        , size(s.size())
    {}
    constexpr explicit StringRef(std::string_view s)
        : data(s.data())
        , size(s.size())
    {}
    constexpr StringRef(const char * data_) // NOLINT(google-explicit-constructor)
        : StringRef(std::string_view{data_})
    {}
    constexpr StringRef() = default;

    std::string toString() const { return std::string(data, size); }

    explicit operator std::string() const { return toString(); }
    constexpr explicit operator std::string_view() const { return {data, size}; }

    ALWAYS_INLINE inline int compare(const StringRef & tar) const
    {
        return mem_utils::CompareStrView({*this}, {tar});
    }
};

/// Here constexpr doesn't implicate inline, see https://www.viva64.com/en/w/v1043/
/// nullptr can't be used because the StringRef values are used in SipHash's pointer arithmetic
/// and the UBSan thinks that something like nullptr + 8 is UB.
constexpr const inline char empty_string_ref_addr{};
[[maybe_unused]] constexpr const inline StringRef EMPTY_STRING_REF{&empty_string_ref_addr, 0};

using StringRefs = std::vector<StringRef>;

// According to https://github.com/pingcap/tiflash/pull/5658
// - if size of memory area is bigger than 1M, instructions about avx512 may begin to get better results
// - otherwise, use `mem_utils::avx2_mem_equal`(under x86-64 with avx2)
inline bool operator==(StringRef lhs, StringRef rhs)
{
    return mem_utils::IsStrViewEqual({lhs}, {rhs});
}

inline bool operator!=(StringRef lhs, StringRef rhs)
{
    return !(lhs == rhs);
}

inline bool operator<(StringRef lhs, StringRef rhs)
{
    return lhs.compare(rhs) < 0;
}

inline bool operator>(StringRef lhs, StringRef rhs)
{
    return lhs.compare(rhs) > 0;
}


/** Hash functions.
  * You can use either CityHash64,
  *  or a function based on the crc32 statement,
  *  which is obviously less qualitative, but on real data sets,
  *  when used in a hash table, works much faster.
  * For more information, see hash_map_string_3.cpp
  */

struct StringRefHash64
{
    size_t operator()(StringRef x) const { return CityHash_v1_0_2::CityHash64(x.data, x.size); }
};

#if defined(__SSE4_2__)

/// Parts are taken from CityHash.

inline UInt64 hashLen16(UInt64 u, UInt64 v)
{
    return CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(u, v));
}

inline UInt64 shiftMix(UInt64 val)
{
    return val ^ (val >> 47);
}

inline UInt64 rotateByAtLeast1(UInt64 val, int shift)
{
    return (val >> shift) | (val << (64 - shift));
}

inline size_t hashLessThan8(const char * data, size_t size)
{
    static constexpr UInt64 k2 = 0x9ae16a3b2f90404fULL;
    static constexpr UInt64 k3 = 0xc949d7c7509e6557ULL;

    if (size >= 4)
    {
        UInt64 a = unalignedLoad<uint32_t>(data);
        return hashLen16(size + (a << 3), unalignedLoad<uint32_t>(data + size - 4));
    }

    if (size > 0)
    {
        uint8_t a = data[0];
        uint8_t b = data[size >> 1];
        uint8_t c = data[size - 1];
        uint32_t y = static_cast<uint32_t>(a) + (static_cast<uint32_t>(b) << 8);
        uint32_t z = size + (static_cast<uint32_t>(c) << 2);
        return shiftMix(y * k2 ^ z * k3) * k2;
    }

    return k2;
}

[[maybe_unused]] inline size_t hashLessThan16(const char * data, size_t size)
{
    if (size > 8)
    {
        auto a = unalignedLoad<UInt64>(data);
        auto b = unalignedLoad<UInt64>(data + size - 8);
        return hashLen16(a, rotateByAtLeast1(b + size, size)) ^ b;
    }

    return hashLessThan8(data, size);
}

struct CRC32Hash
{
    size_t operator()(StringRef x) const
    {
        const char * pos = x.data;
        size_t size = x.size;

        if (size == 0)
            return 0;

        if (size < 8)
        {
            return hashLessThan8(x.data, x.size);
        }

        const char * end = pos + size;
        size_t res = -1ULL;

        do
        {
            auto word = unalignedLoad<UInt64>(pos);
            res = _mm_crc32_u64(res, word);

            pos += 8;
        } while (pos + 8 < end);

        auto word = unalignedLoad<UInt64>(end - 8); /// I'm not sure if this is normal.
        res = _mm_crc32_u64(res, word);

        return res;
    }
};

struct StringRefHash : CRC32Hash
{
};

#else

struct CRC32Hash
{
    size_t operator()(StringRef /* x */) const { throw std::logic_error{"Not implemented CRC32Hash without SSE"}; }
};

struct StringRefHash : StringRefHash64
{
};

#endif


namespace std
{
template <>
struct hash<StringRef> : public StringRefHash
{
};
} // namespace std


namespace ZeroTraits
{
inline bool check(const StringRef & x)
{
    return 0 == x.size;
}
inline void set(StringRef & x)
{
    x.size = 0;
}
} // namespace ZeroTraits


std::ostream & operator<<(std::ostream & os, const StringRef & str);
