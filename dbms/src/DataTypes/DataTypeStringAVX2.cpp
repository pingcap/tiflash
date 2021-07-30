#ifdef DBMS_ENABLE_AVX_SUPPORT
#include <DataTypes/DataTypeString.h>
#include <immintrin.h>


namespace DB
{

namespace
{

#define IMPLEMENT_DESERIALIZE_BIN_AVX2(UNROLL)                                                          \
    void NO_INLINE deserializeBinaryAVX2##ByUnRoll##UNROLL(                                             \
        ColumnString::Chars_t & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit) \
    {                                                                                                   \
        deserializeBinaryAVX2<UNROLL>(data, offsets, istr, limit);                                      \
    }

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
                const auto shift_limit = (size + (vector_length * UNROLL_TIMES - 1)) / vector_length / UNROLL_TIMES * UNROLL_TIMES;
                const __m256i * avx2_src_end = avx2_src_pos + shift_limit;
                auto * avx2_dst_pos = reinterpret_cast<__m256i *>(&data[offset - size - 1]);

                while (avx2_src_pos < avx2_src_end)
                {

                    avx2_src_pos += UNROLL_TIMES;
                    avx2_dst_pos += UNROLL_TIMES;

                    __m256i vector[UNROLL_TIMES];

                    if constexpr (UNROLL_TIMES >= 4)
                        vector[3] = _mm256_loadu_si256(avx2_src_pos - 4);
                    if constexpr (UNROLL_TIMES >= 3)
                        vector[2] = _mm256_loadu_si256(avx2_src_pos - 3);
                    if constexpr (UNROLL_TIMES >= 2)
                        vector[1] = _mm256_loadu_si256(avx2_src_pos - 2);
                    if constexpr (UNROLL_TIMES >= 1)
                        vector[0] = _mm256_loadu_si256(avx2_src_pos - 1);

                    if constexpr (UNROLL_TIMES >= 4)
                        _mm256_storeu_si256(avx2_dst_pos - 4, vector[3]);
                    if constexpr (UNROLL_TIMES >= 3)
                        _mm256_storeu_si256(avx2_dst_pos - 3, vector[2]);
                    if constexpr (UNROLL_TIMES >= 2)
                        _mm256_storeu_si256(avx2_dst_pos - 2, vector[1]);
                    if constexpr (UNROLL_TIMES >= 1)
                        _mm256_storeu_si256(avx2_dst_pos - 1, vector[0]);

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

} // namespace

IMPLEMENT_DESERIALIZE_BIN_AVX2(1)
IMPLEMENT_DESERIALIZE_BIN_AVX2(2)
IMPLEMENT_DESERIALIZE_BIN_AVX2(3)
IMPLEMENT_DESERIALIZE_BIN_AVX2(4)

} // namespace DB
#endif