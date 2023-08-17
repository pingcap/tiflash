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

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
#include <common/mem_utils.h>
#include <immintrin.h>

#include <cassert>
#include <cstdint>
namespace mem_utils::_detail
{
using VectorType = __m256i;

namespace
{
// Both `memoryEqualAVX2x4HalfAligned` and `memoryEqualAVX2x4FullAligned` compares the memory 128 bytes
// starting immediately from p1 and p2.
// AVX2 technology is utilized: this function loads 4 vectors from p1 and 4 vectors from p2 and use issue a
// vectorized cmpeq. The compared results is still in the vector format and can be bitwise-and together to
// generate a final result.
// The final result is bitwise-and together.
//
//               ----------------------------------                      ----------------------------------
// Vector P1[0]: |............|0000_0000|0101_1010|        Vector P1[1]: |............|0000_0000|0101_1010|
//               ----------------------------------                      ----------------------------------
//                                 |          |                                            |         |
//                                equ        neq                                          equ       equ
//                                 |          |                                            |         |
//               ----------------------------------                      ----------------------------------
// Vector P2[0]: |............|0000_0000|1101_1010|        Vector P2[1]: |............|0000_0000|0101_1010|
//               ----------------------------------                      ----------------------------------
//                                 |         |                                             |         |
//                                 |         |                                             |         |
//               ----------------------------------                      ----------------------------------
// Compare[0]:   |............|1111_1111|0000_0000|        Compare[1]:   |............|1111_1111|1111_1111|
//               ----------------------------------                      ----------------------------------
//                                                     |
// AND:                                                |
//                                                     |
//                                      ----------------------------------
//                                      |............|1111_1111|0000_0000|
//                                      ----------------------------------
//                                                     |            |
// MOVMASK                                             |  ----------/
//                                                     | /
// Result:                            <-higher bits->  1 0
//
//
// Unlike AVX512, experiments shows that AVX2 variant seems to benefit from aligned loading,
// so here we bake both half aligned version and full aligned version for the comparison.

// P1 must be aligned to 32-byte boundary
__attribute__((always_inline, pure)) inline bool memoryEqualAVX2x4HalfAligned(const char * p1, const char * p2)
{
    const auto * v1 = reinterpret_cast<const VectorType *>(p1);
    const auto * v2 = reinterpret_cast<const VectorType *>(p2);
    // clang-format off
    return 0xFFFFFFFF == static_cast<unsigned>(_mm256_movemask_epi8(
        _mm256_and_si256(
            _mm256_and_si256(
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(v1 + 0),
                    _mm256_loadu_si256(v2 + 0)),
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(v1 + 1),
                    _mm256_loadu_si256(v2 + 1))),
            _mm256_and_si256(
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(v1 + 2),
                    _mm256_loadu_si256(v2 + 2)),
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(v1 + 3),
                    _mm256_loadu_si256(v2 + 3))))));
    // clang-format on
}

// Both P1 and P2 must be aligned to 32-byte boundary
__attribute__((always_inline, pure)) inline bool memoryEqualAVX2x4FullAligned(const char * p1, const char * p2)
{
    const auto * const v1 = reinterpret_cast<const VectorType *>(p1);
    const auto * const v2 = reinterpret_cast<const VectorType *>(p2);
    // clang-format off
    return 0xFFFFFFFF == static_cast<unsigned>(_mm256_movemask_epi8(
        _mm256_and_si256(
            _mm256_and_si256(
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(v1 + 0),
                    _mm256_load_si256(v2 + 0)),
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(v1 + 1),
                    _mm256_load_si256(v2 + 1))),
            _mm256_and_si256(
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(v1 + 2),
                    _mm256_load_si256(v2 + 2)),
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(v1 + 3),
                    _mm256_load_si256(v2 + 3))))));
    // clang-format on
}

// This function has a similar functionality as the above twos, but it has no alignment assumption and it only compare 32 bytes.
__attribute__((always_inline, pure)) inline bool memoryEqualAVX2x1(const char * p1, const char * p2)
{
    return 0xFFFFFFFF
        == static_cast<unsigned>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(
            _mm256_loadu_si256(reinterpret_cast<const VectorType *>(p1)),
            _mm256_loadu_si256(reinterpret_cast<const VectorType *>(p2)))));
}

} // namespace

bool memoryEqualAVX2x4Loop(ConstBytePtr & p1, ConstBytePtr & p2, size_t & size)
{
    static constexpr auto vector_size = sizeof(VectorType);

    // The following code tries to align up the pointer to 32 word boundary
    // There are two cases:
    //     ------------------------------       ------------------------------
    //     |           Case 1           |       |           Case 2           |
    //     ------------------------------       ------------------------------
    //     | p1       = 0b..01101_10001 |       | p1       = 0b..10000_00000 |
    //     | p2       = 0b..00010_10001 |       | p2       = 0b..10000_00001 |
    //     | p1 ^ p2  = 0b..01111_00000 |       | p1 ^ p2  = 0b..01111_00001 |
    //     | mod(32)  = 0b0000000000000 |       | mod(32)  = 0b..00000_00001 |
    //     ------------------------------       ------------------------------
    // In case 1, the remainder of (p1 ^ p2) is zero; that is if we compare
    // several bytes first, then both p1 and p2 can be aligned to 32-byte boundary.
    // Case 2, however, does not have the nice property: the best thing we can do
    // is to align only one of the two pointers.
    //
    // Next, consider the calculation:
    //
    //          p1               = 0b..000000_11111  (original binary)
    //          -p1              = 0b..111111_00001  (two's complement)
    //          (-p1) & (32 - 1) = 0b..000000_00001  (remainder of 32)
    //
    // The above calculation directly gives the offset of p1 to the nearest 32-byte
    // boundary.
    //
    // But what about the bytes before the boundary? We simply do a unaligned load and
    // compare at the beginning, then we can safely handle the remaining part from the
    // aligned boundary (because the offset to the boundary cannot be larger than one
    // YMM word size).
    //
    //             One YMM word
    //           |_______________|      The remaining part
    //                   |_________________________________________________|

    auto p1num = reinterpret_cast<uintptr_t>(p1);
    auto p2num = reinterpret_cast<uintptr_t>(p2);
    auto total_aligned = 0 == ((p1num ^ p2num) & (vector_size - 1));
    auto p1_aligned_offset = (-p1num) & (vector_size - 1);
    auto group_size = 4 * vector_size;

    if (size >= group_size)
    {
        if (!memoryEqualAVX2x1(p1, p2))
        {
            return false;
        }
        p1 += p1_aligned_offset;
        p2 += p1_aligned_offset;
        size -= p1_aligned_offset;
    }

    if (total_aligned)
    {
        while (size >= group_size)
        {
            __builtin_prefetch(p1 + group_size);
            __builtin_prefetch(p2 + group_size);
            if (!memoryEqualAVX2x4FullAligned(p1, p2))
            {
                return false;
            }
            size -= group_size;
            p1 += group_size;
            p2 += group_size;
        }
    }
    else
    {
        while (size >= group_size)
        {
            __builtin_prefetch(p1 + group_size);
            __builtin_prefetch(p2 + group_size);
            if (!memoryEqualAVX2x4HalfAligned(p1, p2))
            {
                return false;
            }
            size -= group_size;
            p1 += group_size;
            p2 += group_size;
        }
    }

    // GCC should be happy to add its own VZEROUPPER at epilogue area.
    return true;
}

template <size_t N>
__attribute__((always_inline, pure)) inline bool compareArrayAVX2(const VectorType (&data)[N], VectorType filled_vector)
{
    static_assert(N >= 1 && N <= 4, "compare array can only be used within range");

    VectorType compared [[maybe_unused]][N - 1]{};

    if constexpr (N >= 4)
        compared[2] = _mm256_cmpeq_epi8(filled_vector, data[3]);
    if constexpr (N >= 3)
        compared[1] = _mm256_cmpeq_epi8(filled_vector, data[2]);
    if constexpr (N >= 2)
        compared[0] = _mm256_cmpeq_epi8(filled_vector, data[1]);

    auto combined = _mm256_cmpeq_epi8(filled_vector, data[0]);

    if constexpr (N >= 4)
        combined = _mm256_and_si256(combined, compared[2]);
    if constexpr (N >= 3)
        combined = _mm256_and_si256(combined, compared[1]);
    if constexpr (N >= 2)
        combined = _mm256_and_si256(combined, compared[0]);

    auto mask = _mm256_movemask_epi8(combined);
    return static_cast<unsigned>(mask) == 0xFFFF'FFFF;
}

// see `memoryIsByteSSE2` for detailed description
// Another thing to notice is that, for AVX2 and AVX512, GCC is doing even
// better here: under `-O3`, instead of using aligned loading, it uses the address directly, generating
// something like:
//     vmovdqa   0x60(%rdx),%ymm2
//     vpcmpeqb  (%rdx),%ymm1,%ymm0
//     vpcmpeqb  %ymm1,%ymm2,%ymm2
//     vpand     %ymm2,%ymm0,%ymm0
//     vpcmpeqb  0x40(%rdx),%ymm1,%ymm2
//     vpand     %ymm2,%ymm0,%ymm0
//     vpcmpeqb  0x20(%rdx),%ymm1,%ymm2
//     vpand     %ymm2,%ymm0,%ymm0
//     vpmovmskb %ymm0,%eax
__attribute__((pure)) bool memoryIsByteAVX2(const void * data, size_t size, std::byte target)
{
    static constexpr size_t vector_length = sizeof(VectorType);
    static constexpr size_t group_size = vector_length * 4;
    size_t remaining = size;
    auto filled_vector = _mm256_set1_epi8(static_cast<char>(target));
    const auto * current_address = reinterpret_cast<const VectorType *>(data);
    const auto * byte_address = reinterpret_cast<const uint8_t *>(data);

    if (!compareArrayAVX2<1>({_mm256_loadu_si256(current_address)}, filled_vector))
    {
        return false;
    }

    auto numeric_address = reinterpret_cast<uintptr_t>(data);
    auto alignment_offset = (-numeric_address) & (vector_length - 1);
    current_address = reinterpret_cast<const VectorType *>(byte_address + alignment_offset);
    remaining -= alignment_offset;

    while (remaining >= group_size)
    {
        if (compareArrayAVX2(
                {
                    _mm256_load_si256(current_address + 0),
                    _mm256_load_si256(current_address + 1),
                    _mm256_load_si256(current_address + 2),
                    _mm256_load_si256(current_address + 3),
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

    auto tail = _mm256_loadu_si256(reinterpret_cast<const VectorType *>(byte_address + size - vector_length));
    assert(remaining / vector_length <= 3);
    bool result = true;
    switch (remaining / vector_length)
    {
    case 3:
        result = compareArrayAVX2<4>(
            {_mm256_load_si256(current_address + 0),
             _mm256_load_si256(current_address + 1),
             _mm256_load_si256(current_address + 2),
             tail},
            filled_vector);
        break;
    case 2:
        result = compareArrayAVX2<3>(
            {_mm256_load_si256(current_address + 0), _mm256_load_si256(current_address + 1), tail},
            filled_vector);
        break;
    case 1:
        result = compareArrayAVX2<2>({_mm256_load_si256(current_address + 0), tail}, filled_vector);
        break;
    case 0:
        result = compareArrayAVX2<1>({tail}, filled_vector);
        break;
    }
    return result;
}

} // namespace mem_utils::_detail

#endif