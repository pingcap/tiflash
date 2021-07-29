#ifdef __x86_64__
#include <DataTypes/DataTypeString.h>
#include <immintrin.h>

#define IMPLEMENT_DESERIALIZE_BIN_AVX2(UNROLL)                                                          \
    void NO_INLINE deserializeBinaryAVX2##By##UNROLL(                                                   \
        ColumnString::Chars_t & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit) \
    {                                                                                                   \
        deserializeBinaryAVX2<UNROLL>(data, offsets, istr, limit);                                      \
    }

namespace DB
{

template <int UNROLL_TIMES>
static inline void deserializeBinaryAVX2(ColumnString::Chars_t & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit)
{
    size_t offset = data.size();
    for (size_t i = 0; i < limit; ++i)
    {
        if (istr.eof())
            break;

        UInt64 size;
        readVarUInt(size, istr);

        offset += size + 1;
        offsets.push_back(offset);

        data.resize(offset);

        if (size)
        {
            /// An optimistic branch in which more efficient copying is possible.
            if (offset + 32 * UNROLL_TIMES <= data.capacity() && istr.position() + size + 32 * UNROLL_TIMES <= istr.buffer().end())
            {
                const __m256i * avx2_src_pos = reinterpret_cast<const __m256i *>(istr.position());
                const __m256i * avx2_src_end = avx2_src_pos + (size + (32 * UNROLL_TIMES - 1)) / 32 / UNROLL_TIMES * UNROLL_TIMES;
                __m256i * avx2_dst_pos = reinterpret_cast<__m256i *>(&data[offset - size - 1]);

                while (avx2_src_pos < avx2_src_end)
                {

                    avx2_src_pos += UNROLL_TIMES;
                    avx2_dst_pos += UNROLL_TIMES;

                    // GCC 7 does not seem to have a good support for AVX2 codegen here, we still need to use ASM

                    if constexpr (UNROLL_TIMES >= 4)
                        __asm__("vmovdqu %0, %%ymm0" ::"m"(avx2_src_pos[-4]));
                    if constexpr (UNROLL_TIMES >= 3)
                        __asm__("vmovdqu %0, %%ymm1" ::"m"(avx2_src_pos[-3]));
                    if constexpr (UNROLL_TIMES >= 2)
                        __asm__("vmovdqu %0, %%ymm2" ::"m"(avx2_src_pos[-2]));
                    if constexpr (UNROLL_TIMES >= 1)
                        __asm__("vmovdqu %0, %%ymm3" ::"m"(avx2_src_pos[-1]));

                    if constexpr (UNROLL_TIMES >= 4)
                        __asm__("vmovdqu %%ymm0, %0" : "=m"(avx2_dst_pos[-4]));
                    if constexpr (UNROLL_TIMES >= 3)
                        __asm__("vmovdqu %%ymm1, %0" : "=m"(avx2_dst_pos[-3]));
                    if constexpr (UNROLL_TIMES >= 2)
                        __asm__("vmovdqu %%ymm2, %0" : "=m"(avx2_dst_pos[-2]));
                    if constexpr (UNROLL_TIMES >= 1)
                        __asm__("vmovdqu %%ymm3, %0" : "=m"(avx2_dst_pos[-1]));

                    __builtin_prefetch(avx2_src_pos);
                }

                _mm256_zeroupper();

                istr.position() += size;
            }
            else
            {
                istr.readStrict(reinterpret_cast<char *>(&data[offset - size - 1]), size);
            }
        }

        data[offset - 1] = 0;
    }
}
IMPLEMENT_DESERIALIZE_BIN_AVX2(1)
IMPLEMENT_DESERIALIZE_BIN_AVX2(2)
IMPLEMENT_DESERIALIZE_BIN_AVX2(3)
IMPLEMENT_DESERIALIZE_BIN_AVX2(4)

} // namespace DB
#endif