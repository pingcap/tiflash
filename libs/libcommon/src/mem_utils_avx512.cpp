#ifdef TIFLASH_ENABLE_AVX512_SUPPORT

#include <common/mem_utils.h>
#include <immintrin.h>

namespace mem_utils::_detail
{

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
    auto p1_ = reinterpret_cast<const __m512i *>(p1);
    auto p2_ = reinterpret_cast<const __m512i *>(p2);
    return 0xFFFFFFFFFFFFFFFF
        == (_mm512_cmpeq_epi8_mask(_mm512_loadu_si512(p1_), _mm512_loadu_si512(p2_))
            & _mm512_cmpeq_epi8_mask(_mm512_loadu_si512(p1_ + 1), _mm512_loadu_si512(p2_ + 1))
            & _mm512_cmpeq_epi8_mask(_mm512_loadu_si512(p1_ + 2), _mm512_loadu_si512((p2_ + 2)))
            & _mm512_cmpeq_epi8_mask(_mm512_loadu_si512(p1_ + 3), _mm512_loadu_si512(p2_ + 3)));
}

} // namespace

bool memoryEqualAVX512x4Loop(ConstBytePtr & p1, ConstBytePtr & p2, size_t & size)
{
    static constexpr size_t group_size = 4 * sizeof(__m512i);
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
} // namespace mem_utils::_detail

#endif