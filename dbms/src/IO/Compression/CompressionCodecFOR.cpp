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
#include <IO/Compression/CompressionCodecFOR.h>
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

CompressionCodecFOR::CompressionCodecFOR(UInt8 bytes_size_)
    : bytes_size(bytes_size_)
{}

UInt8 CompressionCodecFOR::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::FOR);
}

UInt32 CompressionCodecFOR::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    /**
     *|bytes_of_original_type|frame_of_reference|width(bits)  |bitpacked data|
     *|1 bytes               |bytes_size        |sizeof(UInt8)|required size |
     */
    const size_t count = uncompressed_size / bytes_size;
    return 1 + bytes_size + sizeof(UInt8) + BitpackingPrimitives::getRequiredSize(count, bytes_size * 8);
}

template <std::integral T>
UInt32 CompressionCodecFOR::compressData(const T * source, UInt32 count, char * dest)
{
    RUNTIME_CHECK(count > 0, count);
    std::vector<T> values(count);
    for (UInt32 i = 0; i < count; ++i)
        values[i] = source[i];
    T frame_of_reference = *std::min_element(values.cbegin(), values.cend());
    // store frame of reference
    unalignedStore<T>(dest, frame_of_reference);
    dest += sizeof(T);
    if (frame_of_reference != 0)
    {
        for (auto & value : values)
            value -= frame_of_reference;
    }
    T max_value = *std::max_element(values.cbegin(), values.cend());
    UInt8 width = BitpackingPrimitives::minimumBitWidth(max_value);
    // store width
    unalignedStore<UInt8>(dest, width);
    dest += sizeof(UInt8);
    // if width == 0, skip bitpacking
    if (width == 0)
        return sizeof(T) + sizeof(UInt8);
    auto required_size = BitpackingPrimitives::getRequiredSize(count, width);
    // after applying frame of reference, all values are bigger than 0.
    BitpackingPrimitives::packBuffer(reinterpret_cast<unsigned char *>(dest), values.data(), count, width);
    return sizeof(T) + sizeof(UInt8) + required_size;
}

template <std::integral T>
void CompressionCodecFOR::decompressData(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    const auto count = output_size / sizeof(T);
    T frame_of_reference = unalignedLoad<T>(source);
    source += sizeof(T);
    auto width = unalignedLoad<UInt8>(source);
    source += sizeof(UInt8);
    const auto required_size = source_size - sizeof(T) - sizeof(UInt8);
    RUNTIME_CHECK(BitpackingPrimitives::getRequiredSize(count, width) == required_size);
    auto round_size = BitpackingPrimitives::roundUpToAlgorithmGroupSize(count);
    if (round_size != count)
    {
        // Reserve enough space for the temporary buffer.
        unsigned char tmp_buffer[round_size * sizeof(T)];
        BitpackingPrimitives::unPackBuffer<T>(
            tmp_buffer,
            reinterpret_cast<const unsigned char *>(source),
            count,
            width);
        CompressionCodecFOR::applyFrameOfReference(reinterpret_cast<T *>(tmp_buffer), frame_of_reference, count);
        memcpy(dest, tmp_buffer, output_size);
        return;
    }
    BitpackingPrimitives::unPackBuffer<T>(
        reinterpret_cast<unsigned char *>(dest),
        reinterpret_cast<const unsigned char *>(source),
        count,
        width);
    CompressionCodecFOR::applyFrameOfReference(reinterpret_cast<T *>(dest), frame_of_reference, count);
}

template <std::integral T>
void CompressionCodecFOR::applyFrameOfReference(T * dst, T frame_of_reference, UInt32 count)
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

UInt32 CompressionCodecFOR::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if unlikely (source_size % bytes_size != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "source size {} is not aligned to {}", source_size, bytes_size);
    dest[0] = bytes_size;
    auto count = source_size / bytes_size;
    switch (bytes_size)
    {
    case 1:
        return 1 + compressData<UInt8>(reinterpret_cast<const UInt8 *>(source), count, &dest[1]);
    case 2:
        return 1 + compressData<UInt16>(reinterpret_cast<const UInt16 *>(source), count, &dest[1]);
    case 4:
        return 1 + compressData<UInt32>(reinterpret_cast<const UInt32 *>(source), count, &dest[1]);
    case 8:
        return 1 + compressData<UInt64>(reinterpret_cast<const UInt64 *>(source), count, &dest[1]);
    default:
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress For-encoded data. Unsupported bytes size: {}",
            bytes_size);
    }
}

void CompressionCodecFOR::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if unlikely (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress For-encoded data. File has wrong header");

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
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress For-encoded data. Unsupported bytes size: {}",
            bytes_size);
    }
}


// The following instantiations are used in CompressionCodecDeltaFor.cpp

template UInt32 CompressionCodecFOR::compressData<Int8>(const Int8 * source, UInt32 count, char * dest);
template UInt32 CompressionCodecFOR::compressData<Int16>(const Int16 * source, UInt32 count, char * dest);
template UInt32 CompressionCodecFOR::compressData<Int32>(const Int32 * source, UInt32 count, char * dest);
template UInt32 CompressionCodecFOR::compressData<Int64>(const Int64 * source, UInt32 count, char * dest);

template void CompressionCodecFOR::decompressData<Int8>(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size);
template void CompressionCodecFOR::decompressData<Int16>(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size);
template void CompressionCodecFOR::decompressData<Int32>(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size);
template void CompressionCodecFOR::decompressData<Int64>(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size);

} // namespace DB
