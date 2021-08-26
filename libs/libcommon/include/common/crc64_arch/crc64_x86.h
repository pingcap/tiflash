#pragma once
#ifdef __x86_64__
#include <immintrin.h>

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

    explicit SIMD(simd_t inner) noexcept;

private:
    simd_t _inner{};
};

inline SIMD::SIMD(uint64_t high, uint64_t low) noexcept { _inner = _mm_set_epi64x(static_cast<int64_t>(high), static_cast<int64_t>(low)); }

inline SIMD SIMD::bitxor(SIMD that) const noexcept { return SIMD{_mm_xor_si128(_inner, that._inner)}; }

inline SIMD SIMD::fold8(uint64_t coeff) const noexcept
{
    auto tmp = SIMD{0, coeff};
    auto h = SIMD{_mm_clmulepi64_si128(_inner, tmp._inner, 0x00)};
    auto l = SIMD{_mm_srli_si128(_inner, 8)};
    return h.bitxor(l);
}

inline SIMD SIMD::fold16(SIMD coeff) const noexcept
{
    auto h = SIMD{_mm_clmulepi64_si128(_inner, coeff._inner, 0x11)};
    auto l = SIMD{_mm_clmulepi64_si128(_inner, coeff._inner, 0x00)};
    return h.bitxor(l);
}

inline uint64_t SIMD::barrett(uint64_t poly, uint64_t mu) const noexcept
{
    auto polymu = SIMD{poly, mu};
    auto t1 = _mm_clmulepi64_si128(_inner, polymu._inner, 0x00);
    auto h = SIMD{_mm_slli_si128(t1, 8)};
    auto l = SIMD{_mm_clmulepi64_si128(t1, polymu._inner, 0x10)};
    auto reduced = h.bitxor(l).bitxor(*this);
    return static_cast<uint64_t>(_mm_extract_epi64(reduced._inner, 1));
}

inline SIMD::SIMD(simd_t inner) noexcept : _inner(inner) {}

inline SIMD & SIMD::operator^=(const SIMD & that) noexcept
{
    this->_inner = this->bitxor(that)._inner;
    return *this;
}

inline SIMD SIMD::operator^(const SIMD & that) const noexcept { return bitxor(that); }

inline bool SIMD::operator==(const SIMD & that) const noexcept
{
    auto tmp = _mm_cmpeq_epi8(_inner, that._inner);
    auto mask = _mm_movemask_epi8(tmp);
    return mask == 0xFFFF;
}

inline SIMD SIMD::aligned(const void * address) noexcept { return SIMD{_mm_load_si128(reinterpret_cast<const __m128i *>(address))}; }

} // namespace crc64::_detail
#endif