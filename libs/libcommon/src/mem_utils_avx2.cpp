#ifdef TIFLASH_ENABLE_AVX_SUPPORT
#include <common/mem_utils.h>
#include <immintrin.h>

#include <cstdint>
namespace mem_utils::_detail
{

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
    auto p1_ = reinterpret_cast<const __m256i *>(p1);
    auto p2_ = reinterpret_cast<const __m256i *>(p2);
    // clang-format off
    return 0xFFFFFFFF == static_cast<unsigned>(_mm256_movemask_epi8(
        _mm256_and_si256(
            _mm256_and_si256(
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(p1_ + 0),
                    _mm256_loadu_si256(p2_ + 0)),
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(p1_ + 1),
                    _mm256_loadu_si256(p2_ + 1))),
            _mm256_and_si256(
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(p1_ + 2),
                    _mm256_loadu_si256(p2_ + 2)),
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(p1_ + 3),
                    _mm256_loadu_si256(p2_ + 3))))));
    // clang-format on
}

// Both P1 and P2 must be aligned to 32-byte boundary
__attribute__((always_inline, pure)) inline bool memoryEqualAVX2x4FullAligned(const char * p1, const char * p2)
{
    auto p1_ = reinterpret_cast<const __m256i *>(p1);
    auto p2_ = reinterpret_cast<const __m256i *>(p2);
    // clang-format off
    return 0xFFFFFFFF == static_cast<unsigned>(_mm256_movemask_epi8(
        _mm256_and_si256(
            _mm256_and_si256(
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(p1_ + 0),
                    _mm256_load_si256(p2_ + 0)),
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(p1_ + 1),
                    _mm256_load_si256(p2_ + 1))),
            _mm256_and_si256(
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(p1_ + 2),
                    _mm256_load_si256(p2_ + 2)),
                _mm256_cmpeq_epi8(
                    _mm256_load_si256(p1_ + 3),
                    _mm256_load_si256(p2_ + 3))))));
    // clang-format on
}

// This function has a similar functionality as the above twos, but it has no alignment assumption and it only compare 32 bytes.
__attribute__((always_inline, pure)) inline bool memoryEqualAVX2x1(const char * p1, const char * p2)
{
    return 0xFFFFFFFF
        == static_cast<unsigned>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(
            _mm256_loadu_si256(reinterpret_cast<const __m256i *>(p1)), _mm256_loadu_si256(reinterpret_cast<const __m256i *>(p2)))));
}

} // namespace

bool memoryEqualAVX2x4Loop(ConstBytePtr & p1, ConstBytePtr & p2, size_t & size)
{
    static constexpr auto vector_size = sizeof(__m256i);

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
} // namespace mem_utils::_detail

#endif