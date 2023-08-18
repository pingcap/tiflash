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
#if defined(__aarch64__) || defined(__arm64__)
#include <arm_neon.h>

#include <cstdint>
#include <cstring>
#include <utility>

namespace crc64::_detail
{
typedef uint8x16_t simd_t __attribute__((__may_alias__));

class SIMD
{
public:
    using Poly64Pair = std::pair<poly64_t, poly64_t>;

    SIMD(uint64_t high, uint64_t low) noexcept;

    [[nodiscard]] SIMD fold16(SIMD coeff) const noexcept;

    [[nodiscard]] SIMD fold8(uint64_t coeff) const noexcept;

    [[nodiscard]] uint64_t barrett(uint64_t poly, uint64_t mu) const noexcept;

    [[nodiscard]] SIMD bitxor(SIMD that) const noexcept;

    static SIMD aligned(const void * address) noexcept;

    SIMD & operator^=(const SIMD & that) noexcept;

    SIMD operator^(const SIMD & that) const noexcept;

    bool operator==(const SIMD & that) const noexcept;

    explicit SIMD(simd_t inner) noexcept;

private:
    simd_t _inner{};

    static SIMD from_mul(poly64_t a, poly64_t b) noexcept;

    [[nodiscard]] Poly64Pair into_poly64pair() const noexcept;

    [[nodiscard]] poly64_t high64() const noexcept;

    [[nodiscard]] poly64_t low64() const noexcept;
};

inline SIMD::SIMD(uint64_t high, uint64_t low) noexcept
{
    _inner = vcombine_u8(vcreate_u8(low), vcreate_u8(high));
}

inline SIMD SIMD::bitxor(SIMD that) const noexcept
{
    return SIMD{veorq_u8(_inner, that._inner)};
}

inline SIMD SIMD::fold8(uint64_t coeff) const noexcept
{
    auto [x0, x1] = into_poly64pair();
    auto h = SIMD::from_mul(static_cast<poly64_t>(coeff), x0);
    auto l = SIMD{0, static_cast<uint64_t>(x1)};
    return h.bitxor(l);
}

inline SIMD SIMD::fold16(SIMD coeff) const noexcept
{
    /* GCC/Clang does not seem to use pmull2 properly and we hence use
     * inline assembly to improve the performance.
     */

    SIMD h(0, 0), l(0, 0);
    asm("pmull\t%0.1q, %2.1d, %3.1d\n\t"
        "pmull2\t%1.1q, %2.2d, %3.2d"
        : "=&w"(l._inner), "=w"(h._inner)
        : "w"(this->_inner), "w"(coeff._inner));
    return h.bitxor(l);
}

inline uint64_t SIMD::barrett(uint64_t poly, uint64_t mu) const noexcept
{
    auto t1 = SIMD::from_mul(low64(), static_cast<poly64_t>(mu)).low64();
    auto l = SIMD::from_mul(t1, static_cast<poly64_t>(poly));
    auto reduced = static_cast<uint64_t>(bitxor(l).high64());
    return reduced ^ static_cast<uint64_t>(t1);
}

inline SIMD::SIMD(simd_t inner) noexcept
    : _inner(inner)
{}

inline SIMD & SIMD::operator^=(const SIMD & that) noexcept
{
    this->_inner = this->bitxor(that)._inner;
    return *this;
}

inline SIMD SIMD::operator^(const SIMD & that) const noexcept
{
    return bitxor(that);
}

inline bool SIMD::operator==(const SIMD & that) const noexcept
{
    return ::memcmp(&_inner, &that._inner, 16) == 0;
}

inline SIMD SIMD::aligned(const void * address) noexcept
{
    return SIMD{vld1q_u8(reinterpret_cast<const uint8_t *>(address))};
}

inline SIMD SIMD::from_mul(poly64_t a, poly64_t b) noexcept
{
    auto mul = vmull_p64(a, b);
    return SIMD(vreinterpretq_u8_p128(mul));
}

inline SIMD::Poly64Pair SIMD::into_poly64pair() const noexcept
{
    auto y = vreinterpretq_p64_u8(_inner);
    return {vgetq_lane_p64(y, 0), vgetq_lane_p64(y, 1)};
}

inline poly64_t SIMD::high64() const noexcept
{
    auto y = vreinterpretq_p64_u8(_inner);
    return vgetq_lane_p64(y, 1);
}

inline poly64_t SIMD::low64() const noexcept
{
    auto y = vreinterpretq_p64_u8(_inner);
    return vgetq_lane_p64(y, 0);
}
} // namespace crc64::_detail
#endif