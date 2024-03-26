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

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
#include <common/mem_utils_opt.h>
ASSERT_USE_AVX2_COMPILE_FLAG
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
void compressDataForType(const char * source, UInt32 source_size, char * dest)
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

#if defined(__x86_64__) && defined(__AVX2__)
void compressDataFor32bits(const UInt32 * __restrict__ source, UInt32 source_size, UInt32 * __restrict__ dest)
{
    __m256i prev = _mm256_setzero_si256();
    size_t i = 0;
    for (; i < source_size / 8; ++i)
    {
        // curr = {a0, a1, a2, a3, a4, a5, a6, a7}
        auto curr = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(source) + i);
        // x0 = {prev[7], a0, a1, a2, a3, a4, a5, a6}
        const __m256i p_curr = {0x0000000000000007, 0x0000000200000001, 0x0000000400000003, 0x0000000600000005};
        const __m256i p_prev = {0x0000000700000007, 0x0000000700000007, 0x0000000700000007, 0x0000000700000007};
        auto x0 = _mm256_blend_epi32(
            _mm256_permutevar8x32_epi32(curr, p_curr), // {a7, a0, a1, a2, a3, a4, a5, a6}
            _mm256_permutevar8x32_epi32(prev, p_prev), // {prev[7], prev[7], ...}
            0b00000001);
        auto delta = _mm256_sub_epi32(curr, x0);
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(dest) + i, delta);
        prev = curr;
    }
    UInt32 lastprev = _mm256_extract_epi32(prev, 7);
    for (i = 8 * i; i < source_size; ++i)
    {
        UInt32 curr = source[i];
        dest[i] = curr - lastprev;
        lastprev = curr;
    }
}

void decompressDataFor32bits(const UInt32 * __restrict__ source, UInt32 source_size, UInt32 * __restrict__ dest)
{
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

void compressDataFor64bits(const UInt64 * __restrict__ source, UInt32 source_size, UInt64 * __restrict__ dest)
{
    __m256i prev = _mm256_setzero_si256();
    size_t i = 0;
    for (; i < source_size / 4; ++i)
    {
        // curr = {a0, a1, a2, a3}
        auto curr = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(source) + i);
        // x0 = {prev[3], a0, a1, a2}
        auto x0 = _mm256_blend_epi32(
            _mm256_permute4x64_epi64(curr, 0b10010011), // {a3, a0, a1, a2}
            _mm256_permute4x64_epi64(prev, 0b11111111), // {prev[3], prev[3], prev[3], prev[3]}
            0b00000011);
        auto delta = _mm256_sub_epi64(curr, x0);
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(dest) + i, delta);
        prev = curr;
    }
    UInt64 lastprev = _mm256_extract_epi64(prev, 3);
    for (i = 4 * i; i < source_size; ++i)
    {
        UInt64 curr = source[i];
        dest[i] = curr - lastprev;
        lastprev = curr;
    }
}

void decompressDataFor64bits(const UInt64 * __restrict__ source, UInt32 source_size, UInt64 * __restrict__ dest)
{
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
void decompressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    const char * const output_end = dest + output_size;

    if (source_size % sizeof(T) != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress Delta-encoded data, data size {} is not aligned to {}",
            source_size,
            sizeof(T));

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

} // namespace

UInt32 CompressionCodecDelta::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % delta_bytes_size;
    dest[0] = delta_bytes_size;
    memcpy(&dest[1], source, bytes_to_skip);
    size_t start_pos = 1 + bytes_to_skip;
    switch (delta_bytes_size)
    {
    case 1:
        compressDataForType<UInt8>(source + bytes_to_skip, source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 2:
        compressDataForType<UInt16>(source + bytes_to_skip, source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 4:
#if defined(__x86_64__) && defined(__AVX2__)
        compressDataFor32bits(
            reinterpret_cast<const UInt32 *>(source) + bytes_to_skip,
            (source_size - bytes_to_skip) / 4,
            reinterpret_cast<UInt32 *>(&dest[start_pos]));
#else
        compressDataForType<UInt32>(source + bytes_to_skip, source_size - bytes_to_skip, &dest[start_pos]);
#endif
        break;
    case 8:
#if defined(__x86_64__) && defined(__AVX2__)
        compressDataFor64bits(
            reinterpret_cast<const UInt64 *>(source) + bytes_to_skip,
            (source_size - bytes_to_skip) / 8,
            reinterpret_cast<UInt64 *>(&dest[start_pos]));
#else
        compressDataForType<UInt64>(source + bytes_to_skip, source_size - bytes_to_skip, &dest[start_pos]);
#endif
        break;
    default:
        __builtin_unreachable();
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

    if unlikely (bytes_size != 1 && bytes_size != 2 && bytes_size != 4 && bytes_size != 8)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data. File has wrong header");

    UInt8 bytes_to_skip = uncompressed_size % bytes_size;
    UInt32 output_size = uncompressed_size - bytes_to_skip;

    if unlikely (static_cast<UInt32>(1 + bytes_to_skip) > source_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data. File has wrong header");

    memcpy(dest, &source[1], bytes_to_skip);
    UInt32 source_size_no_header = source_size - bytes_to_skip - 1;
    switch (bytes_size) // NOLINT(bugprone-switch-missing-default-case)
    {
    case 1:
        decompressDataForType<UInt8>(
            &source[1 + bytes_to_skip],
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    case 2:
        decompressDataForType<UInt16>(
            &source[1 + bytes_to_skip],
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    case 4:
#if defined(__x86_64__) && defined(__AVX2__)
        decompressDataFor32bits(
            reinterpret_cast<const UInt32 *>(&source[1 + bytes_to_skip]),
            source_size_no_header / 4,
            reinterpret_cast<UInt32 *>(&dest[bytes_to_skip]));

#else
        decompressDataForType<UInt32>(
            &source[1 + bytes_to_skip],
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
#endif
        break;
    case 8:
#if defined(__x86_64__) && defined(__AVX2__)
        decompressDataFor64bits(
            reinterpret_cast<const UInt64 *>(&source[1 + bytes_to_skip]),
            source_size_no_header / 8,
            reinterpret_cast<UInt64 *>(&dest[bytes_to_skip]));
#else
        decompressDataForType<UInt64>(
            &source[1 + bytes_to_skip],
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
#endif
        break;
    default:
        __builtin_unreachable();
    }
}

} // namespace DB
