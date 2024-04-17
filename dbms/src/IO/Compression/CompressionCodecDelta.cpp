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

#include <Common/Exception.h>
#include <DataTypes/IDataType.h>
#include <IO/Compression/CompressionCodecDelta.h>
#include <IO/Compression/CompressionInfo.h>
#include <common/likely.h>
#include <common/unaligned.h>

#if defined(__x86_64__) && defined(__AVX2__)
#include <immintrin.h>
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

CompressionCodecDelta::CompressionCodecDelta(UInt8 delta_bytes_size_)
    : delta_bytes_size(delta_bytes_size_)
{}

UInt8 CompressionCodecDelta::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::Delta);
}

namespace
{
template <typename T>
void compressData(const char * source, UInt32 source_size, char * dest)
{
    if (source_size % sizeof(T) != 0)
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with Delta codec, data size {} is not aligned to {}",
            source_size,
            sizeof(T));

    T prev_src = 0;
    const char * const source_end = source + source_size;
    while (source < source_end)
    {
        T curr_src = unalignedLoad<T>(source);
        unalignedStore<T>(dest, curr_src - prev_src);
        prev_src = curr_src;

        source += sizeof(T);
        dest += sizeof(T);
    }
}

template <typename T>
void ordinaryDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    if (source_size % sizeof(T) != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress Delta-encoded data, data size {} is not aligned to {}",
            source_size,
            sizeof(T));

    const char * const output_end = dest + output_size;
    T accumulator{};
    const char * const source_end = source + source_size;
    while (source < source_end)
    {
        accumulator += unalignedLoad<T>(source);
        if unlikely (dest + sizeof(accumulator) > output_end)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data");
        unalignedStore<T>(dest, accumulator);

        source += sizeof(T);
        dest += sizeof(T);
    }
}

template <typename T>
void decompressData(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    ordinaryDecompressData<T>(source, source_size, dest, output_size);
}

#if defined(__x86_64__) && defined(__AVX2__)
// Note: using SIMD to rewrite compress does not improve performance.

template <>
void decompressData<UInt32>(
    const char * __restrict__ raw_source,
    UInt32 raw_source_size,
    char * __restrict__ raw_dest,
    UInt32 /*raw_output_size*/)
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
void decompressData<UInt64>(
    const char * __restrict__ raw_source,
    UInt32 raw_source_size,
    char * __restrict__ raw_dest,
    UInt32 /*raw_output_size*/)
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

} // namespace

UInt32 CompressionCodecDelta::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if unlikely (source_size % delta_bytes_size != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "source size {} is not aligned to {}",
            source_size,
            delta_bytes_size);
    dest[0] = delta_bytes_size;
    size_t start_pos = 1;
    switch (delta_bytes_size)
    {
    case 1:
        compressData<UInt8>(source, source_size, &dest[start_pos]);
        break;
    case 2:
        compressData<UInt16>(source, source_size, &dest[start_pos]);
        break;
    case 4:
        compressData<UInt32>(source, source_size, &dest[start_pos]);
        break;
    case 8:
        compressData<UInt64>(source, source_size, &dest[start_pos]);
        break;
    default:
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress Delta-encoded data. Unsupported bytes size");
    }
    return 1 + source_size;
}

void CompressionCodecDelta::doDecompressData(
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
    UInt32 output_size = uncompressed_size;

    UInt32 source_size_no_header = source_size - 1;
    switch (bytes_size)
    {
    case 1:
        decompressData<UInt8>(&source[1], source_size_no_header, dest, output_size);
        break;
    case 2:
        decompressData<UInt16>(&source[1], source_size_no_header, dest, output_size);
        break;
    case 4:
        decompressData<UInt32>(&source[1], source_size_no_header, dest, output_size);
        break;
    case 8:
        decompressData<UInt64>(&source[1], source_size_no_header, dest, output_size);
        break;
    default:
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Delta-encoded data. Unsupported bytes size");
    }
}

void CompressionCodecDelta::ordinaryDecompress(const char * source, UInt32 source_size, char * dest, UInt32 dest_size)
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
