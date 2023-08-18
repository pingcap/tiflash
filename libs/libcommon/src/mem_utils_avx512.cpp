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

#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
#include <common/mem_utils.h>
#include <immintrin.h>

#include <cassert>
namespace mem_utils::_detail
{
using VectorType = __m512i;

namespace
{
// As its name indicates, this function compares the memory 256 bytes starting immediately from p1 and p2.
// AVX512 technology is utilized: this function loads 4 vectors from p1 and 4 vectors from p2 and use issue a
// vectorized cmpeq which yields the result as a 64bit mask for every two vectors.
// The final result is bitwise-and together.
//
//           ----------------------------------
// Vector 1: |............|0000_0000|0101_1010|
//           ----------------------------------
//                            |          |
//                           equ        neq
//                            |          |
//           ----------------------------------
// Vector 2: |............|0000_0000|1101_1010|
//           ----------------------------------
//                             |         |
//                            / ---------/
//                            | |
// Mask:     <-higher bits->  1 0
//              /
//             |
// Result:   mask0 & mask1 & mask & .......
//
// There is no assumption on alignment.

__attribute__((pure, always_inline)) inline bool memoryEqualAVX512x4(const char * p1, const char * p2)
{
    const auto * v1 = reinterpret_cast<const VectorType *>(p1);
    const auto * v2 = reinterpret_cast<const VectorType *>(p2);
    return 0xFFFFFFFFFFFFFFFF
        == (_mm512_cmpeq_epi8_mask(_mm512_loadu_si512(v1), _mm512_loadu_si512(v2))
            & _mm512_cmpeq_epi8_mask(_mm512_loadu_si512(v1 + 1), _mm512_loadu_si512(v2 + 1))
            & _mm512_cmpeq_epi8_mask(_mm512_loadu_si512(v1 + 2), _mm512_loadu_si512((v2 + 2)))
            & _mm512_cmpeq_epi8_mask(_mm512_loadu_si512(v1 + 3), _mm512_loadu_si512(v2 + 3)));
}

} // namespace

bool memoryEqualAVX512x4Loop(ConstBytePtr & p1, ConstBytePtr & p2, size_t & size)
{
    static constexpr size_t group_size = 4 * sizeof(VectorType);
    while (size >= group_size)
    {
        __builtin_prefetch(p1 + group_size);
        __builtin_prefetch(p2 + group_size);
        if (!memoryEqualAVX512x4(p1, p2))
        {
            return false;
        }
        size -= group_size;
        p1 += group_size;
        p2 += group_size;
    }
    return true;
}

template <size_t N>
__attribute__((always_inline, pure)) inline bool compareArrayAVX512(
    const VectorType (&data)[N],
    VectorType filled_vector)
{
    static_assert(N >= 1 && N <= 4, "compare array can only be used within range");

    __mmask64 compared [[maybe_unused]][N - 1]{};

    if constexpr (N >= 4)
        compared[2] = _mm512_cmpeq_epi8_mask(filled_vector, data[3]);
    if constexpr (N >= 3)
        compared[1] = _mm512_cmpeq_epi8_mask(filled_vector, data[2]);
    if constexpr (N >= 2)
        compared[0] = _mm512_cmpeq_epi8_mask(filled_vector, data[1]);

    auto mask = _mm512_cmpeq_epu8_mask(filled_vector, data[0]);

    if constexpr (N >= 4)
        mask = mask & compared[2];
    if constexpr (N >= 3)
        mask = mask & compared[1];
    if constexpr (N >= 2)
        mask = mask & compared[0];

    return mask == 0xFFFF'FFFF'FFFF'FFFF;
}

// see `memoryIsByteSSE2` for detailed description
// Another thing to notice is that, for AVX2 and AVX512, GCC is doing even
// better here: under `-O3`, instead of using aligned loading, it uses the address directly, generating
// something like:
//     vpcmpeqb 0x80(%rdx),%zmm0,%k1
//     vpcmpeqb 0xc0(%rdx),%zmm0,%k1{%k1}
//     vpcmpeqb 0x40(%rdx),%zmm0,%k1{%k1}
//     vpcmpub  $0x0,(%rdx),%zmm0,%k4{%k1}
//     kmovq    %k4,%rcx
//     cmp      $0xffffffffffffffff,%rcx
__attribute__((pure)) bool memoryIsByteAVX512(const void * data, size_t size, std::byte target)
{
    static constexpr size_t vector_length = sizeof(VectorType);
    static constexpr size_t group_size = vector_length * 4;
    size_t remaining = size;
    auto filled_vector = _mm512_set1_epi8(static_cast<char>(target));
    const auto * current_address = reinterpret_cast<const VectorType *>(data);
    const auto * byte_address = reinterpret_cast<const uint8_t *>(data);

    if (!compareArrayAVX512<1>({_mm512_loadu_si512(current_address)}, filled_vector))
    {
        return false;
    }

    auto numeric_address = reinterpret_cast<uintptr_t>(data);
    auto alignment_offset = (-numeric_address) & (vector_length - 1);
    current_address = reinterpret_cast<const VectorType *>(byte_address + alignment_offset);
    remaining -= alignment_offset;

    while (remaining >= group_size)
    {
        if (compareArrayAVX512(
                {
                    _mm512_load_si512(current_address + 0),
                    _mm512_load_si512(current_address + 1),
                    _mm512_load_si512(current_address + 2),
                    _mm512_load_si512(current_address + 3),
                },
                filled_vector))
        {
            remaining -= group_size;
            current_address += 4;
        }
        else
        {
            return false;
        }
    }

    auto tail = _mm512_loadu_si512(reinterpret_cast<const VectorType *>(byte_address + size - vector_length));
    assert(remaining / vector_length <= 3);
    bool result = true;
    switch (remaining / vector_length)
    {
    case 3:
        result = compareArrayAVX512<4>(
            {_mm512_load_si512(current_address + 0),
             _mm512_load_si512(current_address + 1),
             _mm512_load_si512(current_address + 2),
             tail},
            filled_vector);
        break;
    case 2:
        result = compareArrayAVX512<3>(
            {_mm512_load_si512(current_address + 0), _mm512_load_si512(current_address + 1), tail},
            filled_vector);
        break;
    case 1:
        result = compareArrayAVX512<2>({_mm512_load_si512(current_address + 0), tail}, filled_vector);
        break;
    case 0:
        result = compareArrayAVX512<1>({tail}, filled_vector);
        break;
    }
    return result;
}

} // namespace mem_utils::_detail

#endif