#ifdef DBMS_ENABLE_AVX_SUPPORT
#include <DataTypes/DataTypeString.h>
#include <immintrin.h>


namespace DB
{


#define IMPLEMENT_DESERIALIZE_BIN_AVX2(UNROLL)             \
    template void NO_INLINE deserializeBinaryAVX2<UNROLL>( \
        ColumnString::Chars_t & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit);

template <int UNROLL_TIMES>
void deserializeBinaryAVX2(ColumnString::Chars_t & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit)
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

        constexpr auto vector_length = sizeof(__m256i);

        if (size)
        {
            /// An optimistic branch in which more efficient copying is possible.
            if (offset + vector_length * UNROLL_TIMES <= data.capacity()
                && istr.position() + size + vector_length * UNROLL_TIMES <= istr.buffer().end())
            {
                const auto * avx2_src_pos = reinterpret_cast<const __m256i *>(istr.position());
                // truncate to the multiple of (UNROLL_TIMES * vector length)
                const auto shift_limit = (size + (vector_length * UNROLL_TIMES - 1)) / vector_length / UNROLL_TIMES * UNROLL_TIMES;
                const __m256i * avx2_src_end = avx2_src_pos + shift_limit;
                auto * avx2_dst_pos = reinterpret_cast<__m256i *>(&data[offset - size - 1]);

                while (avx2_src_pos < avx2_src_end)
                {

                    avx2_src_pos += UNROLL_TIMES;
                    avx2_dst_pos += UNROLL_TIMES;

                    // GCC 7 does not seem to have a good support for AVX2 codegen here,
                    // we still need to use ASM.
                    // GCC 7 does not generate code with `vmovdqu` with `ymm` registers,
                    // but instead rely on `xmm` registers (`vmovdqu` xmm and `inserti` it to ymm).
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

                _mm256_zeroupper(); // VEX encoded register state transfer (ymm to xmm)

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