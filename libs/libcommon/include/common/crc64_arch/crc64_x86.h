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
#ifdef __x86_64__
#include <immintrin.h>

#include <cstddef>
#include <cstdint>
namespace crc64::_detail
{
using simd_t = __m128i;


class SIMD
{
public:
    SIMD(uint64_t high, uint64_t low) noexcept;

    [[nodiscard]] SIMD fold16(SIMD coeff) const noexcept;

    [[nodiscard]] SIMD fold8(uint64_t coeff) const noexcept;

    [[nodiscard]] uint64_t barrett(uint64_t poly, uint64_t mu) const noexcept;

    [[nodiscard]] SIMD bitxor(SIMD that) const noexcept;

    static SIMD aligned(const void * address) noexcept;

    SIMD & operator^=(const SIMD & that) noexcept;

    SIMD operator^(const SIMD & that) const noexcept;

    bool operator==(const SIMD & that) const noexcept;

    explicit SIMD(simd_t val) noexcept;

private:
    simd_t inner{};
};

inline SIMD::SIMD(uint64_t high, uint64_t low) noexcept
{
    inner = _mm_set_epi64x(static_cast<int64_t>(high), static_cast<int64_t>(low));
}

inline SIMD SIMD::bitxor(SIMD that) const noexcept
{
    return SIMD{_mm_xor_si128(inner, that.inner)};
}

inline SIMD SIMD::fold8(uint64_t coeff) const noexcept
{
    auto tmp = SIMD{0, coeff};
    auto h = SIMD{_mm_clmulepi64_si128(inner, tmp.inner, 0x00)};
    auto l = SIMD{_mm_srli_si128(inner, 8)};
    return h.bitxor(l);
}

inline SIMD SIMD::fold16(SIMD coeff) const noexcept
{
    auto h = SIMD{_mm_clmulepi64_si128(inner, coeff.inner, 0x11)};
    auto l = SIMD{_mm_clmulepi64_si128(inner, coeff.inner, 0x00)};
    return h.bitxor(l);
}

inline uint64_t SIMD::barrett(uint64_t poly, uint64_t mu) const noexcept
{
    auto polymu = SIMD{poly, mu};
    auto t1 = _mm_clmulepi64_si128(inner, polymu.inner, 0x00);
    auto h = SIMD{_mm_slli_si128(t1, 8)};
    auto l = SIMD{_mm_clmulepi64_si128(t1, polymu.inner, 0x10)};
    auto reduced = h.bitxor(l).bitxor(*this);
    return static_cast<uint64_t>(_mm_extract_epi64(reduced.inner, 1));
}

inline SIMD::SIMD(simd_t val) noexcept
    : inner(val)
{}

inline SIMD & SIMD::operator^=(const SIMD & that) noexcept
{
    this->inner = this->bitxor(that).inner;
    return *this;
}

inline SIMD SIMD::operator^(const SIMD & that) const noexcept
{
    return bitxor(that);
}

inline bool SIMD::operator==(const SIMD & that) const noexcept
{
    auto tmp = _mm_cmpeq_epi8(inner, that.inner);
    auto mask = _mm_movemask_epi8(tmp);
    return mask == 0xFFFF;
}

inline SIMD SIMD::aligned(const void * address) noexcept
{
    return SIMD{_mm_load_si128(reinterpret_cast<const __m128i *>(address))};
}

} // namespace crc64::_detail
#endif