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
#include <IO/Compression/CompressionCodecFor.h>
#include <IO/Compression/CompressionInfo.h>
#include <common/likely.h>
#include <common/unaligned.h>

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
void DeltaEncode(const T * source, UInt32 count, T * dest)
{
    T prev = 0;
    for (UInt32 i = 0; i < count; ++i)
    {
        T curr = source[i];
        dest[i] = curr - prev;
        prev = curr;
    }
}

template <typename T>
UInt32 compressData(const char * source, UInt32 source_size, char * dest)
{
    static_assert(std::is_integral<T>::value, "Integral required.");
    const auto count = source_size / sizeof(T);
    DeltaEncode<T>(reinterpret_cast<const T *>(source), count, reinterpret_cast<T *>(dest));
    return CompressionCodecFor::compressData<T>(dest, source_size, dest);
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
void ordinaryDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    CompressionCodecFor::decompressData<T>(source, source_size, dest, output_size);
    ordinaryDeltaDecode<T>(dest, output_size, dest);
}

template <typename T>
void decompressData(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    static_assert(std::is_integral<T>::value, "Integral required.");
    ordinaryDecompressData<T>(source, source_size, dest, output_size);
}

template <>
void decompressData<UInt32>(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    const auto count = output_size / sizeof(UInt32);
    auto round_size = BitpackingPrimitives::roundUpToAlgorithmGroupSize(count);
    // Reserve enough space for the temporary buffer.
    const auto required_size = round_size * sizeof(UInt32);
    char tmp_buffer[required_size];
    CompressionCodecFor::decompressData<UInt32>(source, source_size, tmp_buffer, required_size);
    DeltaDecode<UInt32>(reinterpret_cast<const char *>(tmp_buffer), output_size, dest);
}

template <>
void decompressData<UInt64>(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    const auto count = output_size / sizeof(UInt64);
    const auto round_size = BitpackingPrimitives::roundUpToAlgorithmGroupSize(count);
    // Reserve enough space for the temporary buffer.
    const auto required_size = round_size * sizeof(UInt64);
    char tmp_buffer[required_size];
    CompressionCodecFor::decompressData<UInt64>(source, source_size, tmp_buffer, required_size);
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
