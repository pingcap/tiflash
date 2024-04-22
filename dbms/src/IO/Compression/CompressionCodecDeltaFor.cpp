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

#include <Common/BitpackingPrimitives.h>
#include <Common/Exception.h>
#include <IO/Compression/CompressionCodecDeltaFor.h>
#include <IO/Compression/CompressionInfo.h>
#include <common/likely.h>
#include <common/unaligned.h>

#include <cstring>

#if defined(__AVX2__)
#include <immintrin.h>
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

CompressionCodecDeltaFor::CompressionCodecDeltaFor(UInt8 bytes_size_)
    : bytes_size(bytes_size_)
{}

UInt8 CompressionCodecDeltaFor::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::DeltaFor);
}

UInt32 CompressionCodecDeltaFor::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // 1 byte for bytes_size, x bytes for frame of reference, 1 byte for width.
    const size_t count = uncompressed_size / bytes_size;
    return 1 + bytes_size + sizeof(UInt8) + BitpackingPrimitives::getRequiredSize(count, bytes_size * 8);
}

namespace
{
template <typename T>
UInt32 compressData(const char * source, UInt32 source_size, char * dest)
{
    static_assert(std::is_integral<T>::value, "Integral required.");
    const auto count = source_size / sizeof(T);
    using ST = typename std::make_signed<T>::type;
    std::vector<ST> deltas;
    deltas.reserve(count);
    T prev_src = 0;
    const char * const source_end = source + source_size;
    while (source < source_end)
    {
        T curr_src = unalignedLoad<T>(source);
        deltas.push_back(static_cast<ST>(curr_src - prev_src));
        prev_src = curr_src;
        source += sizeof(T);
    }
    ST frame_of_reference = *std::min_element(deltas.cbegin(), deltas.cend());
    // store frame of reference
    unalignedStore<ST>(dest, frame_of_reference);
    dest += sizeof(ST);
    if (frame_of_reference != 0)
    {
        for (auto & delta : deltas)
            delta -= frame_of_reference;
    }
    ST max_value = *std::max_element(deltas.cbegin(), deltas.cend());
    UInt8 width = BitpackingPrimitives::minimumBitWidth(max_value);
    // store width
    unalignedStore<UInt8>(dest, width);
    dest += sizeof(UInt8);
    // if width == 0, skip bitpacking
    if (width == 0)
        return sizeof(ST) + sizeof(UInt8);
    auto required_size = BitpackingPrimitives::getRequiredSize(count, width);
    // after applying frame of reference, all deltas are bigger than 0.
    BitpackingPrimitives::packBuffer(
        reinterpret_cast<unsigned char *>(dest),
        reinterpret_cast<const T *>(deltas.data()),
        count,
        width);
    return sizeof(ST) + sizeof(UInt8) + required_size;
}

template <class T>
void ApplyFrameOfReference(T * dst, T frame_of_reference, UInt32 count)
{
    if (!frame_of_reference)
        return;

    UInt32 i = 0;
    UInt32 misaligned_count = count;
#if defined(__AVX2__)
    static_assert(sizeof(T) < sizeof(__m256i) && sizeof(__m256i) % sizeof(T) == 0);
    misaligned_count = count % (sizeof(__m256i) / sizeof(T));
#endif
    for (; i < misaligned_count; ++i)
    {
        dst[i] += frame_of_reference;
    }
#if defined(__AVX2__)
    for (; i < count; i += (sizeof(__m256i) / sizeof(T)))
    {
        // Load the data using SIMD
        __m256i delta = _mm256_loadu_si256(reinterpret_cast<__m256i *>(dst + i));
        // Perform vectorized addition
        if constexpr (sizeof(T) == 1)
        {
            delta = _mm256_add_epi8(delta, _mm256_set1_epi8(frame_of_reference));
        }
        else if constexpr (sizeof(T) == 2)
        {
            delta = _mm256_add_epi16(delta, _mm256_set1_epi16(frame_of_reference));
        }
        else if constexpr (sizeof(T) == 4)
        {
            delta = _mm256_add_epi32(delta, _mm256_set1_epi32(frame_of_reference));
        }
        else if constexpr (sizeof(T) == 8)
        {
            delta = _mm256_add_epi64(delta, _mm256_set1_epi64x(frame_of_reference));
        }
        // Store the result back to memory
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(dst + i), delta);
    }
#endif
}

template <typename T>
void ordinaryDeltaDecode(const char * source, UInt32 source_size, char * dest)
{
    T accumulator{};
    const char * const source_end = source + source_size;
    while (source < source_end)
    {
        accumulator += unalignedLoad<T>(source);
        unalignedStore<T>(dest, accumulator);

        source += sizeof(T);
        dest += sizeof(T);
    }
}

template <typename T>
void DeltaDecode(const char * source, UInt32 source_size, char * dest)
{
    ordinaryDeltaDecode<T>(source, source_size, dest);
}

#if defined(__AVX2__)
// Note: using SIMD to rewrite compress does not improve performance.

template <>
void DeltaDecode<UInt32>(const char * __restrict__ raw_source, UInt32 raw_source_size, char * __restrict__ raw_dest)
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
void DeltaDecode<UInt64>(const char * __restrict__ raw_source, UInt32 raw_source_size, char * __restrict__ raw_dest)
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

template <typename T>
void ForDecode(const char * source, UInt32 source_size, unsigned char * dest, UInt32 count)
{
    using ST = typename std::make_signed<T>::type;
    ST frame_of_reference = unalignedLoad<ST>(source);
    source += sizeof(ST);
    auto width = unalignedLoad<UInt8>(source);
    source += sizeof(UInt8);
    const auto required_size = source_size - sizeof(ST) - sizeof(UInt8);
    RUNTIME_CHECK(BitpackingPrimitives::getRequiredSize(count, width) == required_size);
    if (width != 0)
        BitpackingPrimitives::unPackBuffer<T>(dest, reinterpret_cast<const unsigned char *>(source), count, width);
    else
        memset(dest, 0, sizeof(T) * count);
    ApplyFrameOfReference(reinterpret_cast<ST *>(dest), frame_of_reference, count);
}

template <typename T>
void ordinaryDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    const auto count = output_size / sizeof(T);
    auto round_size = BitpackingPrimitives::roundUpToAlgorithmGroupSize(count);
    if (round_size != count)
    {
        // Reserve enough space for the temporary buffer.
        unsigned char tmp_buffer[round_size * sizeof(T)];
        ForDecode<T>(source, source_size, tmp_buffer, count);
        ordinaryDeltaDecode<T>(reinterpret_cast<const char *>(tmp_buffer), output_size, dest);
    }
    else
    {
        ForDecode<T>(source, source_size, reinterpret_cast<unsigned char *>(dest), count);
        ordinaryDeltaDecode<T>(dest, output_size, dest);
    }
}

template <typename T>
void decompressData(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    ordinaryDecompressData<T>(source, source_size, dest, output_size);
}

template <>
void decompressData<UInt32>(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    const auto count = output_size / sizeof(UInt32);
    auto round_size = BitpackingPrimitives::roundUpToAlgorithmGroupSize(count);
    // Reserve enough space for the temporary buffer.
    unsigned char tmp_buffer[round_size * sizeof(UInt32)];
    ForDecode<UInt32>(source, source_size, tmp_buffer, count);
    DeltaDecode<UInt32>(reinterpret_cast<const char *>(tmp_buffer), output_size, dest);
}

template <>
void decompressData<UInt64>(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    const auto count = output_size / sizeof(UInt64);
    auto round_size = BitpackingPrimitives::roundUpToAlgorithmGroupSize(count);
    // Reserve enough space for the temporary buffer.
    unsigned char tmp_buffer[round_size * sizeof(UInt64)];
    ForDecode<UInt64>(source, source_size, tmp_buffer, count);
    DeltaDecode<UInt64>(reinterpret_cast<const char *>(tmp_buffer), output_size, dest);
}

} // namespace

UInt32 CompressionCodecDeltaFor::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if unlikely (source_size % bytes_size != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "source size {} is not aligned to {}", source_size, bytes_size);
    dest[0] = bytes_size;
    size_t start_pos = 1;
    switch (bytes_size)
    {
    case 1:
        return 1 + compressData<UInt8>(source, source_size, &dest[start_pos]);
    case 2:
        return 1 + compressData<UInt16>(source, source_size, &dest[start_pos]);
    case 4:
        return 1 + compressData<UInt32>(source, source_size, &dest[start_pos]);
    case 8:
        return 1 + compressData<UInt64>(source, source_size, &dest[start_pos]);
    default:
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress Delta-encoded data. Unsupported bytes size");
    }
}

void CompressionCodecDeltaFor::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if unlikely (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data. File has wrong header");

    if (uncompressed_size == 0)
        return;

    UInt8 bytes_size = source[0];
    if unlikely (uncompressed_size % bytes_size != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "uncompressed size {} is not aligned to {}",
            uncompressed_size,
            bytes_size);

    UInt32 source_size_no_header = source_size - 1;
    switch (bytes_size)
    {
    case 1:
        decompressData<UInt8>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case 2:
        decompressData<UInt16>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case 4:
        decompressData<UInt32>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case 8:
        decompressData<UInt64>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    default:
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Delta-encoded data. Unsupported bytes size");
    }
}

void CompressionCodecDeltaFor::ordinaryDecompress(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 dest_size)
{
    if unlikely (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data. File has wrong header");

    if (dest_size == 0)
        return;

    UInt8 bytes_size = source[0];
    if unlikely (dest_size % bytes_size != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "uncompressed size {} is not aligned to {}",
            dest_size,
            bytes_size);

    UInt32 source_size_no_header = source_size - 1;
    switch (bytes_size)
    {
    case 1:
        ordinaryDecompressData<UInt8>(&source[1], source_size_no_header, dest, dest_size);
        break;
    case 2:
        ordinaryDecompressData<UInt16>(&source[1], source_size_no_header, dest, dest_size);
        break;
    case 4:
        ordinaryDecompressData<UInt32>(&source[1], source_size_no_header, dest, dest_size);
        break;
    case 8:
        ordinaryDecompressData<UInt64>(&source[1], source_size_no_header, dest, dest_size);
        break;
    default:
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Delta-encoded data. Unsupported bytes size");
    }
}

} // namespace DB
