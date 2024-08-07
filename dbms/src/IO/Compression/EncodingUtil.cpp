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
char * writeSameValueMultipleTime(T value, UInt32 count, char * dest)
{
    if (unlikely(count == 0))
        return dest;
    if constexpr (sizeof(T) == 1)
    {
        memset(dest, value, count);
        dest += count;
    }
    else
    {
        UInt32 j = 0;
#if defined(__AVX2__)
        // avx2
        while (j + sizeof(__m256i) / sizeof(T) <= count)
        {
            if constexpr (sizeof(T) == 2)
            {
                auto value_avx2 = _mm256_set1_epi16(value);
                _mm256_storeu_si256(reinterpret_cast<__m256i *>(dest), value_avx2);
            }
            else if constexpr (sizeof(T) == 4)
            {
                auto value_avx2 = _mm256_set1_epi32(value);
                _mm256_storeu_si256(reinterpret_cast<__m256i *>(dest), value_avx2);
            }
            else if constexpr (sizeof(T) == 8)
            {
                auto value_avx2 = _mm256_set1_epi64x(value);
                _mm256_storeu_si256(reinterpret_cast<__m256i *>(dest), value_avx2);
            }
            j += sizeof(__m256i) / sizeof(T);
            dest += sizeof(__m256i);
        }
#endif
        // sse
        while (j + sizeof(__m128i) / sizeof(T) <= count)
        {
            if constexpr (sizeof(T) == 2)
            {
                auto value_sse = _mm_set1_epi16(value);
                _mm_storeu_si128(reinterpret_cast<__m128i *>(dest), value_sse);
            }
            else if constexpr (sizeof(T) == 4)
            {
                auto value_sse = _mm_set1_epi32(value);
                _mm_storeu_si128(reinterpret_cast<__m128i *>(dest), value_sse);
            }
            else if constexpr (sizeof(T) == 8)
            {
                auto value_sse = _mm_set1_epi64x(value);
                _mm_storeu_si128(reinterpret_cast<__m128i *>(dest), value_sse);
            }
            j += sizeof(__m128i) / sizeof(T);
            dest += sizeof(__m128i);
        }
        // scalar
        for (; j < count; ++j)
        {
            unalignedStore<T>(dest, value);
            dest += sizeof(T);
        }
    }
    return dest;
}

template char * writeSameValueMultipleTime<UInt8>(UInt8, UInt32, char *);
template char * writeSameValueMultipleTime<UInt16>(UInt16, UInt32, char *);
template char * writeSameValueMultipleTime<UInt32>(UInt32, UInt32, char *);
template char * writeSameValueMultipleTime<UInt64>(UInt64, UInt32, char *);

/// Constant encoding

template <std::integral T>
void constantDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    if (unlikely(source_size < sizeof(T)))
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot use Constant decoding, data size {} is too small",
            source_size);

    T constant = unalignedLoad<T>(src);
    writeSameValueMultipleTime<T>(constant, dest_size / sizeof(T), dest);
}

template void constantDecoding<UInt8>(const char *, UInt32, char *, UInt32);
template void constantDecoding<UInt16>(const char *, UInt32, char *, UInt32);
template void constantDecoding<UInt32>(const char *, UInt32, char *, UInt32);
template void constantDecoding<UInt64>(const char *, UInt32, char *, UInt32);

/// ConstantDelta encoding

template <std::integral T>
void constantDeltaDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    if (unlikely(source_size < sizeof(T) + sizeof(T)))
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot use ConstantDelta decoding, data size {} is too small",
            source_size);

    T first_value = unalignedLoad<T>(src);
    T constant_delta = unalignedLoad<T>(src + sizeof(T));
    for (size_t i = 0; i < dest_size / sizeof(T); ++i)
    {
        unalignedStore<T>(dest, first_value);
        first_value += constant_delta;
        dest += sizeof(T);
    }
}

template void constantDeltaDecoding<UInt8>(const char *, UInt32, char *, UInt32);
template void constantDeltaDecoding<UInt16>(const char *, UInt32, char *, UInt32);
template void constantDeltaDecoding<UInt32>(const char *, UInt32, char *, UInt32);
template void constantDeltaDecoding<UInt64>(const char *, UInt32, char *, UInt32);

/// Delta encoding

template <std::integral T>
void deltaEncoding(const T * source, UInt32 count, T * dest)
{
    T prev = 0;
    for (UInt32 i = 0; i < count; ++i)
    {
        T curr = source[i];
        dest[i] = curr - prev;
        prev = curr;
    }
}

template void deltaEncoding<Int8>(const Int8 *, UInt32, Int8 *);
template void deltaEncoding<Int16>(const Int16 *, UInt32, Int16 *);
template void deltaEncoding<Int32>(const Int32 *, UInt32, Int32 *);
template void deltaEncoding<Int64>(const Int64 *, UInt32, Int64 *);

/// Delta + FrameOfReference encoding

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

    using TU = std::make_unsigned_t<T>;
    auto * unsigned_dst = reinterpret_cast<TU *>(dst);
    auto unsigned_frame_of_reference = static_cast<TU>(frame_of_reference);

    UInt32 i = 0;
#if defined(__AVX2__)
    UInt32 aligned_count = count - count % (sizeof(__m256i) / sizeof(T));
    for (; i < aligned_count; i += (sizeof(__m256i) / sizeof(T)))
    {
        // Load the data using SIMD
        __m256i value = _mm256_loadu_si256(reinterpret_cast<__m256i *>(unsigned_dst + i));
        // Perform vectorized addition
        if constexpr (sizeof(T) == 1)
        {
            value = _mm256_sub_epi8(value, _mm256_set1_epi8(unsigned_frame_of_reference));
        }
        else if constexpr (sizeof(T) == 2)
        {
            value = _mm256_sub_epi16(value, _mm256_set1_epi16(unsigned_frame_of_reference));
        }
        else if constexpr (sizeof(T) == 4)
        {
            value = _mm256_sub_epi32(value, _mm256_set1_epi32(unsigned_frame_of_reference));
        }
        else if constexpr (sizeof(T) == 8)
        {
            value = _mm256_sub_epi64(value, _mm256_set1_epi64x(unsigned_frame_of_reference));
        }
        // Store the result back to memory
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(unsigned_dst + i), value);
    }
#endif
    for (; i < count; ++i)
    {
        unsigned_dst[i] -= unsigned_frame_of_reference;
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
void deltaDecoding(const char * source, UInt32 source_size, char * dest)
{
    ordinaryDeltaDecoding<T>(source, source_size, dest);
}

#if defined(__AVX2__)

/**
 * 1. According to microbenchmark, the performance of SIMD encoding is not better than the ordinary implementation.
 * 2. The SIMD implementation of UInt16 and UInt8 is too complex, and the performance is not better than the ordinary implementation.
 */

template <>
void deltaDecoding<Int32>(const char * __restrict__ raw_source, UInt32 raw_source_size, char * __restrict__ raw_dest)
{
    const auto * source = reinterpret_cast<const Int32 *>(raw_source);
    auto source_size = raw_source_size / sizeof(Int32);
    auto * dest = reinterpret_cast<Int32 *>(raw_dest);
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
    Int32 lastprev = _mm_extract_epi32(prev, 3);
    for (i = 4 * i; i < source_size; ++i)
    {
        lastprev = lastprev + source[i];
        dest[i] = lastprev;
    }
}

template <>
void deltaDecoding<Int64>(const char * __restrict__ raw_source, UInt32 raw_source_size, char * __restrict__ raw_dest)
{
    const auto * source = reinterpret_cast<const Int64 *>(raw_source);
    auto source_size = raw_source_size / sizeof(Int64);
    auto * dest = reinterpret_cast<Int64 *>(raw_dest);
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
    Int64 lastprev = _mm256_extract_epi64(prev, 3);
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

// For UInt8/UInt16, the default implement has better performance
template void deltaFORDecoding<UInt8>(const char *, UInt32, char *, UInt32);
template void deltaFORDecoding<UInt16>(const char *, UInt32, char *, UInt32);

template <>
void deltaFORDecoding<UInt32>(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    static constexpr auto TYPE_BYTE_SIZE = sizeof(UInt32);
    assert(source_size >= TYPE_BYTE_SIZE);
    assert(dest_size >= TYPE_BYTE_SIZE);

    const auto deltas_count = dest_size / TYPE_BYTE_SIZE - 1;
    if (unlikely(deltas_count == 0))
    {
        memcpy(dest, src, TYPE_BYTE_SIZE);
        return;
    }
    auto round_size = BitpackingPrimitives::roundUpToAlgorithmGroupSize(deltas_count);
    // Reserve enough space for the temporary buffer.
    const auto required_size = round_size * TYPE_BYTE_SIZE + TYPE_BYTE_SIZE;
    char tmp_buffer[required_size];
    memset(tmp_buffer, 0, required_size);
    // copy the first value to the temporary buffer
    memcpy(tmp_buffer, src, TYPE_BYTE_SIZE);
    FORDecoding<UInt32>(
        src + TYPE_BYTE_SIZE,
        source_size - TYPE_BYTE_SIZE,
        tmp_buffer + TYPE_BYTE_SIZE,
        required_size - TYPE_BYTE_SIZE);
    deltaDecoding<Int32>(tmp_buffer, dest_size, dest);
}

template <>
void deltaFORDecoding<UInt64>(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    static constexpr auto TYPE_BYTE_SIZE = sizeof(UInt64);
    assert(source_size >= TYPE_BYTE_SIZE);
    assert(dest_size >= TYPE_BYTE_SIZE);

    const auto deltas_count = dest_size / TYPE_BYTE_SIZE - 1;
    if (unlikely(deltas_count == 0))
    {
        memcpy(dest, src, TYPE_BYTE_SIZE);
        return;
    }
    const auto round_size = BitpackingPrimitives::roundUpToAlgorithmGroupSize(deltas_count);
    // Reserve enough space for the temporary buffer.
    const auto required_size = round_size * TYPE_BYTE_SIZE + TYPE_BYTE_SIZE;
    char tmp_buffer[required_size];
    memset(tmp_buffer, 0, required_size);
    // copy the first value to the temporary buffer
    memcpy(tmp_buffer, src, TYPE_BYTE_SIZE);
    FORDecoding<UInt64>(
        src + TYPE_BYTE_SIZE,
        source_size - TYPE_BYTE_SIZE,
        tmp_buffer + TYPE_BYTE_SIZE,
        required_size - TYPE_BYTE_SIZE);
    deltaDecoding<Int64>(tmp_buffer, dest_size, dest);
}

/// Run-length encoding

template <std::integral T>
size_t runLengthEncodedApproximateSize(const T * source, UInt32 source_size)
{
    T prev_value = source[0];
    size_t pair_count = 1;

    for (UInt32 i = 1; i < source_size; ++i)
    {
        T value = source[i];
        if (prev_value != value)
        {
            ++pair_count;
            prev_value = value;
        }
    }
    return pair_count * RunLengthPairLength<T>;
}

template size_t runLengthEncodedApproximateSize<UInt8>(const UInt8 *, UInt32);
template size_t runLengthEncodedApproximateSize<UInt16>(const UInt16 *, UInt32);
template size_t runLengthEncodedApproximateSize<UInt32>(const UInt32 *, UInt32);
template size_t runLengthEncodedApproximateSize<UInt64>(const UInt64 *, UInt32);

template <std::integral T>
size_t runLengthEncoding(const T * source, UInt32 source_size, char * dest)
{
    T prev_value = source[0];
    memcpy(dest, source, sizeof(T));
    dest += sizeof(T);

    std::vector<UInt8> counts;
    counts.reserve(source_size);
    UInt8 count = 1;

    for (UInt32 i = 1; i < source_size; ++i)
    {
        T value = source[i];
        if (prev_value == value && count < std::numeric_limits<UInt8>::max())
        {
            ++count;
        }
        else
        {
            counts.push_back(count);
            unalignedStore<T>(dest, value);
            dest += sizeof(T);
            prev_value = value;
            count = 1;
        }
    }
    counts.push_back(count);

    memcpy(dest, counts.data(), counts.size());
    return counts.size() * RunLengthPairLength<T>;
}

template size_t runLengthEncoding<UInt8>(const UInt8 *, UInt32, char *);
template size_t runLengthEncoding<UInt16>(const UInt16 *, UInt32, char *);
template size_t runLengthEncoding<UInt32>(const UInt32 *, UInt32, char *);
template size_t runLengthEncoding<UInt64>(const UInt64 *, UInt32, char *);

template <std::integral T>
void runLengthDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    if (unlikely(source_size % RunLengthPairLength<T> != 0))
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot use RunLength decoding, data size {} is not aligned to {}",
            source_size,
            RunLengthPairLength<T>);

    const auto pair_count = source_size / RunLengthPairLength<T>;
    const char * count_src = src + pair_count * sizeof(T);

    const char * dest_end = dest + dest_size;
    for (UInt32 i = 0; i < pair_count; ++i)
    {
        T value = unalignedLoad<T>(src);
        src += sizeof(T);
        auto count = unalignedLoad<UInt8>(count_src);
        count_src += sizeof(UInt8);
        if (unlikely(dest + count * sizeof(T) > dest_end))
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot use RunLength decoding, data is too large, value={}, count={} elem_byte={}",
                value,
                count,
                sizeof(T));
        dest = writeSameValueMultipleTime(value, count, dest);
    }
}

template void runLengthDecoding<UInt8>(const char *, UInt32, char *, UInt32);
template void runLengthDecoding<UInt16>(const char *, UInt32, char *, UInt32);
template void runLengthDecoding<UInt32>(const char *, UInt32, char *, UInt32);
template void runLengthDecoding<UInt64>(const char *, UInt32, char *, UInt32);

} // namespace DB::Compression
