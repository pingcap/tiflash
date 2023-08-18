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

#include <common/crc64_fast.h>
#if defined(TIFLASH_CRC64_HAS_SIMD_SUPPORT) && defined(TIFLASH_ENABLE_AVX512_SUPPORT) \
    && TIFLASH_COMPILER_VPCLMULQDQ_SUPPORT
#include <common/crc64.h>
#include <common/crc64_arch/crc64_x86.h>
#include <common/crc64_table.h>

#include <array>
namespace crc64::_detail
{
using avx512_t = __m512i;

template <size_t N>
using Slice = std::array<avx512_t, N>;

uint64_t update_vpclmulqdq_avx512(uint64_t state, const void * src, size_t length)
{
    if (length == 0)
        return state;

    const auto * ptr = reinterpret_cast<const avx512_t *>(__builtin_assume_aligned(src, 512));

    auto load_slice = [](const avx512_t * address) -> Slice<2> {
        return {_mm512_load_si512(address), _mm512_load_si512(address + 1)};
    };

    auto x = load_slice(ptr);
    ptr += 2;
    x[0] = _mm512_xor_si512(x[0], _mm512_set_epi64(0, 0, 0, 0, 0, 0, 0, static_cast<int64_t>(state)));

    auto coeff = _mm512_set_epi64(
        static_cast<int64_t>(K_1023),
        static_cast<int64_t>(K_1087),
        static_cast<int64_t>(K_1023),
        static_cast<int64_t>(K_1087),
        static_cast<int64_t>(K_1023),
        static_cast<int64_t>(K_1087),
        static_cast<int64_t>(K_1023),
        static_cast<int64_t>(K_1087));

    auto fold = [](avx512_t a, avx512_t b) -> avx512_t {
        auto h = _mm512_clmulepi64_epi128(a, b, 0x11);
        auto l = _mm512_clmulepi64_epi128(a, b, 0x00);
        return _mm512_xor_si512(h, l);
    };

    for (size_t i = 0; i < 3; i += 1, ptr += 2)
    {
        auto chunk = load_slice(ptr);
        for (size_t j = 0; j < 2; ++j)
        {
            auto folded = fold(x[j], coeff);
            x[j] = _mm512_xor_si512(chunk[j], folded);
        }
    }

    for (size_t i = 512; i < length; i += 512, ptr += 8)
    {
        Slice<2> chunk[4] = {load_slice(ptr + 0), load_slice(ptr + 2), load_slice(ptr + 4), load_slice(ptr + 6)};
        {
            auto folded0 = fold(x[0], coeff);
            x[0] = _mm512_xor_si512(chunk[0][0], folded0);
            auto folded1 = fold(x[1], coeff);
            x[1] = _mm512_xor_si512(chunk[0][1], folded1);
        }
        {
            auto folded0 = fold(x[0], coeff);
            x[0] = _mm512_xor_si512(chunk[1][0], folded0);
            auto folded1 = fold(x[1], coeff);
            x[1] = _mm512_xor_si512(chunk[1][1], folded1);
        }
        {
            auto folded0 = fold(x[0], coeff);
            x[0] = _mm512_xor_si512(chunk[2][0], folded0);
            auto folded1 = fold(x[1], coeff);
            x[1] = _mm512_xor_si512(chunk[2][1], folded1);
        }
        {
            auto folded0 = fold(x[0], coeff);
            x[0] = _mm512_xor_si512(chunk[3][0], folded0);
            auto folded1 = fold(x[1], coeff);
            x[1] = _mm512_xor_si512(chunk[3][1], folded1);
        }
    }

    std::array<SIMD, 8> y = {
        SIMD{_mm512_extracti64x2_epi64(x[0], 0)},
        SIMD{_mm512_extracti64x2_epi64(x[0], 1)},
        SIMD{_mm512_extracti64x2_epi64(x[0], 2)},
        SIMD{_mm512_extracti64x2_epi64(x[0], 3)},
        SIMD{_mm512_extracti64x2_epi64(x[1], 0)},
        SIMD{_mm512_extracti64x2_epi64(x[1], 1)},
        SIMD{_mm512_extracti64x2_epi64(x[1], 2)},
        SIMD{_mm512_extracti64x2_epi64(x[1], 3)},
    };

    std::array<SIMD, 7> coeffs = {
        SIMD{K_895, K_959}, // fold by distance of 112 bytes
        SIMD{K_767, K_831}, // fold by distance of 96 bytes
        SIMD{K_639, K_703}, // fold by distance of 80 bytes
        SIMD{K_511, K_575}, // fold by distance of 64 bytes
        SIMD{K_383, K_447}, // fold by distance of 48 bytes
        SIMD{K_255, K_319}, // fold by distance of 32 bytes
        SIMD{K_127, K_191} // fold by distance of 16 bytes
    };

    auto acc = y[7];
    for (size_t i = 0; i < 7; ++i)
    {
        acc ^= y[i].fold16(coeffs[i]);
    }

    return acc.fold8(K_127).barrett(POLY, MU);
}
} // namespace crc64::_detail
#endif