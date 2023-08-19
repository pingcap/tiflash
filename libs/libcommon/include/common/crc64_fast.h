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
#include <common/crc64_table.h>
namespace crc64::_detail
{
#if defined(TIFLASH_ENABLE_ASIMD_SUPPORT) || __SSE2__
#define TIFLASH_CRC64_HAS_SIMD_SUPPORT
// avx2 and avx512 variants
#if TIFLASH_COMPILER_VPCLMULQDQ_SUPPORT
#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
extern uint64_t update_vpclmulqdq_avx512(uint64_t state, const void * src, size_t length);
#endif // TIFLASH_ENABLE_AVX512_SUPPORT
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
extern uint64_t update_vpclmulqdq_avx2(uint64_t state, const void * src, size_t length);
#endif // TIFLASH_ENABLE_AVX_SUPPORT
#endif // TIFLASH_COMPILER_VPCLMULQDQ_SUPPORT

uint64_t update_simd(uint64_t state, const void * src, size_t length);

template <uintptr_t ALIGN = 128, class Fn>
static inline uint64_t update_fast(Fn func, uint64_t state, const void * src, size_t length)
{
    static const uintptr_t MASK = ALIGN - 1;

    if (length == 0)
        return state;

    auto offset = (-reinterpret_cast<uintptr_t>(src)) & MASK;

    if (offset >= length)
    {
        return update_table(state, src, length);
    }

    auto suffix = (length - offset) & MASK;
    auto middle = length - offset - suffix;
    const auto * ptr = reinterpret_cast<const uint8_t *>(src);
    if constexpr (ALIGN > 128)
    {
        state = update_fast<128>(update_simd, state, ptr, offset);
        state = func(state, ptr + offset, middle);
        state = update_fast<128>(update_simd, state, ptr + offset + middle, suffix);
    }
    else
    {
        state = update_table(state, ptr, offset);
        state = func(state, ptr + offset, middle);
        state = update_table(state, ptr + offset + middle, suffix);
    }
    return state;
}
#endif

} // namespace crc64::_detail