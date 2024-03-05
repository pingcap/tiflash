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
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <IO/Buffer/BufferWithOwnMemory.h>
#include <IO/Buffer/ReadBuffer.h>
#include <IO/Compression/CompressedReadBufferBase.h>
#include <IO/Compression/CompressedStream.h>
#include <IO/WriteHelpers.h>
#include <city.h>
#include <common/unaligned.h>
#include <lz4.h>
#include <string.h>
#include <zstd.h>

#include <vector>

#if USE_QPL
#include <IO/Compression/CodecDeflateQpl.h>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_COMPRESSION_METHOD;
extern const int TOO_LARGE_SIZE_COMPRESSED;
extern const int CHECKSUM_DOESNT_MATCH;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes


/// Read compressed data into compressed_buffer. Get size of decompressed data from block header. Checksum if need.
/// Returns number of compressed bytes read.
template <bool has_legacy_checksum>
size_t CompressedReadBufferBase<has_legacy_checksum>::readCompressedData(
    size_t & size_decompressed,
    size_t & size_compressed_without_checksum)
{
    if (compressed_in->eof())
        return 0;

    CityHash_v1_0_2::uint128 checksum;
    if constexpr (has_legacy_checksum)
    {
        compressed_in->readStrict(reinterpret_cast<char *>(&checksum), sizeof(checksum));
    }

    own_compressed_buffer.resize(COMPRESSED_BLOCK_HEADER_SIZE);
    compressed_in->readStrict(&own_compressed_buffer[0], COMPRESSED_BLOCK_HEADER_SIZE);

    UInt8 method = own_compressed_buffer[0]; /// See CompressedWriteBuffer.h

    if (method == static_cast<UInt8>(CompressionMethodByte::COL_END))
        return 0;

    size_t & size_compressed = size_compressed_without_checksum;

#if USE_QPL
    if (method == static_cast<UInt8>(CompressionMethodByte::LZ4)
        || method == static_cast<UInt8>(CompressionMethodByte::QPL)
        || method == static_cast<UInt8>(CompressionMethodByte::ZSTD)
        || method == static_cast<UInt8>(CompressionMethodByte::NONE))
#else
    if (method == static_cast<UInt8>(CompressionMethodByte::LZ4)
        || method == static_cast<UInt8>(CompressionMethodByte::ZSTD)
        || method == static_cast<UInt8>(CompressionMethodByte::NONE))
#endif
    {
        size_compressed = unalignedLoad<UInt32>(&own_compressed_buffer[1]);
        size_decompressed = unalignedLoad<UInt32>(&own_compressed_buffer[5]);
    }
    else
        throw Exception("Unknown compression method: " + toString(method), ErrorCodes::UNKNOWN_COMPRESSION_METHOD);

    if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
        throw Exception(
            "Too large size_compressed. Most likely corrupted data.",
            ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

    /// Is whole compressed block located in 'compressed_in' buffer?
    if (compressed_in->offset() >= COMPRESSED_BLOCK_HEADER_SIZE
        && compressed_in->position() + size_compressed - COMPRESSED_BLOCK_HEADER_SIZE <= compressed_in->buffer().end())
    {
        compressed_in->position() -= COMPRESSED_BLOCK_HEADER_SIZE;
        compressed_buffer = compressed_in->position();
        compressed_in->position() += size_compressed;
    }
    else
    {
        own_compressed_buffer.resize(size_compressed);
        compressed_buffer = &own_compressed_buffer[0];
        compressed_in->readStrict(
            compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE,
            size_compressed - COMPRESSED_BLOCK_HEADER_SIZE);
    }

    if constexpr (has_legacy_checksum)
    {
        if (!disable_checksum[0] && checksum != CityHash_v1_0_2::CityHash128(compressed_buffer, size_compressed))
            throw Exception("Checksum doesn't match: corrupted data.", ErrorCodes::CHECKSUM_DOESNT_MATCH);
        return size_compressed + sizeof(checksum);
    }
    else
    {
        return size_compressed;
    }
}

template <bool has_legacy_checksum>
void CompressedReadBufferBase<has_legacy_checksum>::decompress(
    char * to,
    size_t size_decompressed,
    size_t size_compressed_without_checksum)
{
    UInt8 method = compressed_buffer[0]; /// See CompressedWriteBuffer.h

    if (method == static_cast<UInt8>(CompressionMethodByte::LZ4))
    {
        if (unlikely(
                LZ4_decompress_safe(
                    compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE,
                    to,
                    size_compressed_without_checksum - COMPRESSED_BLOCK_HEADER_SIZE,
                    size_decompressed)
                < 0))
            throw Exception("Cannot LZ4_decompress_safe", ErrorCodes::CANNOT_DECOMPRESS);
    }
    else if (method == static_cast<UInt8>(CompressionMethodByte::ZSTD))
    {
        size_t res = ZSTD_decompress(
            to,
            size_decompressed,
            compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE,
            size_compressed_without_checksum - COMPRESSED_BLOCK_HEADER_SIZE);

        if (ZSTD_isError(res))
            throw Exception(
                "Cannot ZSTD_decompress: " + std::string(ZSTD_getErrorName(res)),
                ErrorCodes::CANNOT_DECOMPRESS);
    }
#if USE_QPL
    else if (method == static_cast<UInt8>(CompressionMethodByte::QPL))
    {
        if (unlikely(
                QPL::QPL_decompress(
                    compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE,
                    size_compressed_without_checksum - COMPRESSED_BLOCK_HEADER_SIZE,
                    to,
                    size_decompressed)
                < 0))
            throw Exception("Cannot QplDecompressData", ErrorCodes::CANNOT_DECOMPRESS);
    }
#endif
    else if (method == static_cast<UInt8>(CompressionMethodByte::NONE))
    {
        memcpy(to, &compressed_buffer[COMPRESSED_BLOCK_HEADER_SIZE], size_decompressed);
    }
    else
        throw Exception("Unknown compression method: " + toString(method), ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
}


/// 'compressed_in' could be initialized lazily, but before first call of 'readCompressedData'.
template <bool has_legacy_checksum>
CompressedReadBufferBase<has_legacy_checksum>::CompressedReadBufferBase(ReadBuffer * in)
    : compressed_in(in)
    , own_compressed_buffer(COMPRESSED_BLOCK_HEADER_SIZE)
{}

/// Proper destruction of unique_ptr of forward-declared type.
template <bool has_legacy_checksum>
CompressedReadBufferBase<has_legacy_checksum>::~CompressedReadBufferBase() = default;


template class CompressedReadBufferBase<true>;
template class CompressedReadBufferBase<false>;

} // namespace DB
