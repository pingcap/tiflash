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

#include <IO/Compression/EncodingUtil.h>

#if defined(__AVX2__)
#include <immintrin.h>
#endif

namespace DB::Compression
{

template <std::integral T>
void applyFrameOfReference(T * dst, T frame_of_reference, UInt32 count)
{
    if (frame_of_reference == 0)
        return;

    UInt32 i = 0;
#if defined(__AVX2__)
    UInt32 aligned_count = count - count % (sizeof(__m256i) / sizeof(T));
    for (; i < aligned_count; i += (sizeof(__m256i) / sizeof(T)))
    {
        // Load the data using SIMD
        __m256i value = _mm256_loadu_si256(reinterpret_cast<__m256i *>(dst + i));
        // Perform vectorized addition
        if constexpr (sizeof(T) == 1)
        {
            value = _mm256_add_epi8(value, _mm256_set1_epi8(frame_of_reference));
        }
        else if constexpr (sizeof(T) == 2)
        {
            value = _mm256_add_epi16(value, _mm256_set1_epi16(frame_of_reference));
        }
        else if constexpr (sizeof(T) == 4)
        {
            value = _mm256_add_epi32(value, _mm256_set1_epi32(frame_of_reference));
        }
        else if constexpr (sizeof(T) == 8)
        {
            value = _mm256_add_epi64(value, _mm256_set1_epi64x(frame_of_reference));
        }
        // Store the result back to memory
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(dst + i), value);
    }
#endif
    for (; i < count; ++i)
    {
        dst[i] += frame_of_reference;
    }
}

template void applyFrameOfReference<UInt8>(UInt8 *, UInt8, UInt32);
template void applyFrameOfReference<UInt16>(UInt16 *, UInt16, UInt32);
template void applyFrameOfReference<UInt32>(UInt32 *, UInt32, UInt32);
template void applyFrameOfReference<UInt64>(UInt64 *, UInt64, UInt32);
template void applyFrameOfReference<Int8>(Int8 *, Int8, UInt32);
template void applyFrameOfReference<Int16>(Int16 *, Int16, UInt32);
template void applyFrameOfReference<Int32>(Int32 *, Int32, UInt32);
template void applyFrameOfReference<Int64>(Int64 *, Int64, UInt32);

template <std::integral T>
void subtractFrameOfReference(T * dst, T frame_of_reference, UInt32 count)
{
    if (frame_of_reference == 0)
        return;

    UInt32 i = 0;
#if defined(__AVX2__)
    UInt32 aligned_count = count - count % (sizeof(__m256i) / sizeof(T));
    for (; i < aligned_count; i += (sizeof(__m256i) / sizeof(T)))
    {
        // Load the data using SIMD
        __m256i value = _mm256_loadu_si256(reinterpret_cast<__m256i *>(dst + i));
        // Perform vectorized addition
        if constexpr (sizeof(T) == 1)
        {
            value = _mm256_sub_epi8(value, _mm256_set1_epi8(frame_of_reference));
        }
        else if constexpr (sizeof(T) == 2)
        {
            value = _mm256_sub_epi16(value, _mm256_set1_epi16(frame_of_reference));
        }
        else if constexpr (sizeof(T) == 4)
        {
            value = _mm256_sub_epi32(value, _mm256_set1_epi32(frame_of_reference));
        }
        else if constexpr (sizeof(T) == 8)
        {
            value = _mm256_sub_epi64(value, _mm256_set1_epi64x(frame_of_reference));
        }
        // Store the result back to memory
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(dst + i), value);
    }
#endif
    for (; i < count; ++i)
    {
        dst[i] -= frame_of_reference;
    }
}

template void subtractFrameOfReference<Int8>(Int8 *, Int8, UInt32);
template void subtractFrameOfReference<Int16>(Int16 *, Int16, UInt32);
template void subtractFrameOfReference<Int32>(Int32 *, Int32, UInt32);
template void subtractFrameOfReference<Int64>(Int64 *, Int64, UInt32);
template void subtractFrameOfReference<UInt8>(UInt8 *, UInt8, UInt32);
template void subtractFrameOfReference<UInt16>(UInt16 *, UInt16, UInt32);
template void subtractFrameOfReference<UInt32>(UInt32 *, UInt32, UInt32);
template void subtractFrameOfReference<UInt64>(UInt64 *, UInt64, UInt32);

template <std::integral T>
UInt8 FOREncodingWidth(std::vector<T> & values, T frame_of_reference)
{
    if constexpr (std::is_signed_v<T>)
    {
        // For signed types, after subtracting frame of reference, the range of values is not always [0, max_value - min_value].
        // For example, we have a sequence of Int8 values [-128, 1, 127], after subtracting frame of reference -128, the values are [0, -127, -1].
        // The minimum bit width required to store the values is 8 rather than the width of `max_value - min_value = -1`.
        // So we need to calculate the minimum bit width of the values after subtracting frame of reference.
        subtractFrameOfReference<T>(values.data(), frame_of_reference, values.size());
        auto [min_value, max_value] = std::minmax_element(values.cbegin(), values.cend());
        return BitpackingPrimitives::minimumBitWidth<T>(*min_value, *max_value);
    }
    else
    {
        T max_value = *std::max_element(values.cbegin(), values.cend());
        return BitpackingPrimitives::minimumBitWidth<T>(max_value - frame_of_reference);
    }
}

template UInt8 FOREncodingWidth<Int8>(std::vector<Int8> &, Int8);
template UInt8 FOREncodingWidth<Int16>(std::vector<Int16> &, Int16);
template UInt8 FOREncodingWidth<Int32>(std::vector<Int32> &, Int32);
template UInt8 FOREncodingWidth<Int64>(std::vector<Int64> &, Int64);
template UInt8 FOREncodingWidth<UInt8>(std::vector<UInt8> &, UInt8);
template UInt8 FOREncodingWidth<UInt16>(std::vector<UInt16> &, UInt16);
template UInt8 FOREncodingWidth<UInt32>(std::vector<UInt32> &, UInt32);
template UInt8 FOREncodingWidth<UInt64>(std::vector<UInt64> &, UInt64);

template <std::integral T>
void deltaDecoding(const char * source, UInt32 source_size, char * dest)
{
    ordinaryDeltaDecoding<T>(source, source_size, dest);
}

#if defined(__AVX2__)
// Note: using SIMD to rewrite compress does not improve performance.

template <>
void deltaDecoding<UInt32>(const char * __restrict__ raw_source, UInt32 raw_source_size, char * __restrict__ raw_dest)
{
    const auto * source = reinterpret_cast<const UInt32 *>(raw_source);
    auto source_size = raw_source_size / sizeof(UInt32);
    auto * dest = reinterpret_cast<UInt32 *>(raw_dest);
    __m128i prev = _mm_setzero_si128();
    size_t i = 0;
    for (; i < source_size / 4; i++)
    {
        auto curr = _mm_lddqu_si128(reinterpret_cast<const __m128i *>(source) + i);
        const auto tmp1 = _mm_add_epi32(_mm_slli_si128(curr, 8), curr);
        const auto tmp2 = _mm_add_epi32(_mm_slli_si128(tmp1, 4), tmp1);
        prev = _mm_add_epi32(tmp2, _mm_shuffle_epi32(prev, 0xff));
        _mm_storeu_si128(reinterpret_cast<__m128i *>(dest) + i, prev);
    }
    uint32_t lastprev = _mm_extract_epi32(prev, 3);
    for (i = 4 * i; i < source_size; ++i)
    {
        lastprev = lastprev + source[i];
        dest[i] = lastprev;
    }
}

template <>
void deltaDecoding<UInt64>(const char * __restrict__ raw_source, UInt32 raw_source_size, char * __restrict__ raw_dest)
{
    const auto * source = reinterpret_cast<const UInt64 *>(raw_source);
    auto source_size = raw_source_size / sizeof(UInt64);
    auto * dest = reinterpret_cast<UInt64 *>(raw_dest);
    // AVX2 does not support shffule across 128-bit lanes, so we need to use permute.
    __m256i prev = _mm256_setzero_si256();
    __m256i zero = _mm256_setzero_si256();
    size_t i = 0;
    for (; i < source_size / 4; ++i)
    {
        // curr = {a0, a1, a2, a3}
        auto curr = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(source) + i);
        // x0 = {0, a0, a1, a2}
        auto x0 = _mm256_blend_epi32(_mm256_permute4x64_epi64(curr, 0b10010011), zero, 0b00000011);
        // x1 = {a0, a01, a12, a23}
        auto x1 = _mm256_add_epi64(curr, x0);
        // x2 = {0, 0, a0, a01}
        auto x2 = _mm256_permute2f128_si256(x1, x1, 0b00101000);
        // prev = prev + {a0, a01, a012, a0123}
        prev = _mm256_add_epi64(prev, _mm256_add_epi64(x1, x2));
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(dest) + i, prev);
        // prev = {prev[3], prev[3], prev[3], prev[3]}
        prev = _mm256_permute4x64_epi64(prev, 0b11111111);
    }
    UInt64 lastprev = _mm256_extract_epi64(prev, 3);
    for (i = 4 * i; i < source_size; ++i)
    {
        lastprev += source[i];
        dest[i] = lastprev;
    }
}

#endif

template <std::integral T>
void deltaFORDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    static_assert(std::is_integral<T>::value, "Integral required.");
    ordinaryDeltaFORDecoding<T>(src, source_size, dest, dest_size);
}

template <>
void deltaFORDecoding<UInt32>(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    const auto deltas_count = dest_size / sizeof(UInt32) - 1;
    if (unlikely(deltas_count == 0))
    {
        memcpy(dest, src, sizeof(UInt32));
        return;
    }
    auto round_size = BitpackingPrimitives::roundUpToAlgorithmGroupSize(deltas_count);
    // Reserve enough space for the temporary buffer.
    const auto required_size = round_size * sizeof(UInt32) + sizeof(UInt32);
    char tmp_buffer[required_size];
    memset(tmp_buffer, 0, required_size);
    // copy the first value to the temporary buffer
    memcpy(tmp_buffer, src, sizeof(UInt32));
    FORDecoding<Int32>(
        src + sizeof(UInt32),
        source_size - sizeof(UInt32),
        tmp_buffer + sizeof(UInt32),
        required_size - sizeof(UInt32));
    deltaDecoding<UInt32>(reinterpret_cast<const char *>(tmp_buffer), dest_size, dest);
}

template <>
void deltaFORDecoding<UInt64>(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    const auto deltas_count = dest_size / sizeof(UInt64) - 1;
    if (unlikely(deltas_count == 0))
    {
        memcpy(dest, src, sizeof(UInt64));
        return;
    }
    const auto round_size = BitpackingPrimitives::roundUpToAlgorithmGroupSize(deltas_count);
    // Reserve enough space for the temporary buffer.
    const auto required_size = round_size * sizeof(UInt64) + sizeof(UInt64);
    char tmp_buffer[required_size];
    memset(tmp_buffer, 0, required_size);
    // copy the first value to the temporary buffer
    memcpy(tmp_buffer, src, sizeof(UInt64));
    FORDecoding<Int64>(
        src + sizeof(UInt64),
        source_size - sizeof(UInt64),
        tmp_buffer + sizeof(UInt64),
        required_size - sizeof(UInt64));
    deltaDecoding<UInt64>(reinterpret_cast<const char *>(tmp_buffer), dest_size, dest);
}

template void deltaFORDecoding<UInt8>(const char *, UInt32, char *, UInt32);
template void deltaFORDecoding<UInt16>(const char *, UInt32, char *, UInt32);

} // namespace DB::Compression
