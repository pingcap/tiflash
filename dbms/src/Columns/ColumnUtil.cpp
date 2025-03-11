// Copyright 2024 PingCAP, Inc.
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

#include <Columns/ColumnUtil.h>
#include <common/mem_utils_opt.h>

#if defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#elif defined(__SSE2__) || defined(__AVX2__) || defined(__AVX512F__) && defined(__AVX512BW__)
#include <immintrin.h>
#endif

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
ASSERT_USE_AVX2_COMPILE_FLAG
#endif


namespace DB
{

UInt64 ToBits64(const UInt8 * bytes64)
{
#if defined(__AVX512F__) && defined(__AVX512BW__)
    const __m512i vbytes = _mm512_loadu_si512(reinterpret_cast<const void *>(bytes64));
    UInt64 res = _mm512_testn_epi8_mask(vbytes, vbytes);
#elif defined(__AVX2__)
    const auto check_block = _mm256_setzero_si256();
    uint64_t mask0 = mem_utils::details::get_block32_cmp_eq_mask(bytes64, check_block);
    uint64_t mask1
        = mem_utils::details::get_block32_cmp_eq_mask(bytes64 + mem_utils::details::BLOCK32_SIZE, check_block);
    auto res = mask0 | (mask1 << mem_utils::details::BLOCK32_SIZE);
#elif defined(__SSE2__)
    const auto zero16 = _mm_setzero_si128();
    UInt64 res = static_cast<UInt64>(_mm_movemask_epi8(
                     _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64)), zero16)))
        | (static_cast<UInt64>(_mm_movemask_epi8(
               _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 16)), zero16)))
           << 16)
        | (static_cast<UInt64>(_mm_movemask_epi8(
               _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 32)), zero16)))
           << 32)
        | (static_cast<UInt64>(_mm_movemask_epi8(
               _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 48)), zero16)))
           << 48);
#elif defined(__aarch64__) && defined(__ARM_NEON)
    const uint8x16_t bitmask
        = {0x01, 0x02, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80, 0x01, 0x02, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};
    const auto * src = reinterpret_cast<const unsigned char *>(bytes64);
    const uint8x16_t p0 = vceqzq_u8(vld1q_u8(src));
    const uint8x16_t p1 = vceqzq_u8(vld1q_u8(src + 16));
    const uint8x16_t p2 = vceqzq_u8(vld1q_u8(src + 32));
    const uint8x16_t p3 = vceqzq_u8(vld1q_u8(src + 48));
    uint8x16_t t0 = vandq_u8(p0, bitmask);
    uint8x16_t t1 = vandq_u8(p1, bitmask);
    uint8x16_t t2 = vandq_u8(p2, bitmask);
    uint8x16_t t3 = vandq_u8(p3, bitmask);
    uint8x16_t sum0 = vpaddq_u8(t0, t1);
    uint8x16_t sum1 = vpaddq_u8(t2, t3);
    sum0 = vpaddq_u8(sum0, sum1);
    sum0 = vpaddq_u8(sum0, sum0);
    UInt64 res = vgetq_lane_u64(vreinterpretq_u64_u8(sum0), 0);
#else
    UInt64 res = 0;
    for (size_t i = 0; i < 64; ++i)
        res |= static_cast<UInt64>(0 == bytes64[i]) << i;
#endif
    return ~res;
}

} // namespace DB
