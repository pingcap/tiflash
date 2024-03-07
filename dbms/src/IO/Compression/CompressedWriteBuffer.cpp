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

#include <Common/config.h>
#include <Core/Types.h>
#include <IO/Compression/CompressedWriteBuffer.h>
#include <city.h>
#include <common/unaligned.h>
#include <lz4.h>
#include <lz4hc.h>
#include <string.h>
#include <zstd.h>

#include <memory>
#if USE_QPL
#include "CodecDeflateQpl.h"
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int UNKNOWN_COMPRESSION_METHOD;
} // namespace ErrorCodes

template <typename Buffer>
size_t CompressionEncode(
    std::string_view source,
    const CompressionSettings & compression_settings,
    Buffer & compressed_buffer)
{
    size_t compressed_size = 0;

    /** The format of compressed block - see CompressedStream.h
      */

    switch (compression_settings.method)
    {
    case CompressionMethod::LZ4:
    case CompressionMethod::LZ4HC:
    {
        static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);
        compressed_buffer.resize(header_size + LZ4_COMPRESSBOUND(source.size()));
        compressed_buffer[0] = static_cast<UInt8>(CompressionMethodByte::LZ4);

        if (compression_settings.method == CompressionMethod::LZ4)
            compressed_size = header_size
                + LZ4_compress_fast(
                                  source.data(),
                                  &compressed_buffer[header_size],
                                  source.size(),
                                  LZ4_COMPRESSBOUND(source.size()),
                                  compression_settings.level);
        else
            compressed_size = header_size
                + LZ4_compress_HC(source.data(),
                                  &compressed_buffer[header_size],
                                  source.size(),
                                  LZ4_COMPRESSBOUND(source.size()),
                                  compression_settings.level);

        UInt32 compressed_size_32 = compressed_size;
        UInt32 uncompressed_size_32 = source.size();

        unalignedStore<UInt32>(&compressed_buffer[1], compressed_size_32);
        unalignedStore<UInt32>(&compressed_buffer[5], uncompressed_size_32);

        break;
    }
    case CompressionMethod::ZSTD:
    {
        static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);
        compressed_buffer.resize(header_size + ZSTD_compressBound(source.size()));
        compressed_buffer[0] = static_cast<UInt8>(CompressionMethodByte::ZSTD);

        size_t res = ZSTD_compress(
            &compressed_buffer[header_size],
            compressed_buffer.size() - header_size,
            source.data(),
            source.size(),
            compression_settings.level);

        if (ZSTD_isError(res))
            throw Exception(
                "Cannot compress block with ZSTD: " + std::string(ZSTD_getErrorName(res)),
                ErrorCodes::CANNOT_COMPRESS);

        compressed_size = header_size + res;

        UInt32 compressed_size_32 = compressed_size;
        UInt32 uncompressed_size_32 = source.size();

        unalignedStore<UInt32>(&compressed_buffer[1], compressed_size_32);
        unalignedStore<UInt32>(&compressed_buffer[5], uncompressed_size_32);

        break;
    }
    case CompressionMethod::NONE:
    {
        static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

        compressed_size = header_size + source.size();
        UInt32 uncompressed_size_32 = source.size();
        UInt32 compressed_size_32 = compressed_size;

        compressed_buffer.resize(compressed_size);

        compressed_buffer[0] = static_cast<UInt8>(CompressionMethodByte::NONE);

        unalignedStore<UInt32>(&compressed_buffer[1], compressed_size_32);
        unalignedStore<UInt32>(&compressed_buffer[5], uncompressed_size_32);
        memcpy(&compressed_buffer[9], source.data(), source.size());

        break;
    }
#if USE_QPL
    case CompressionMethod::QPL:
    {
        static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

        compressed_buffer.resize(header_size + QPL_Compressbound(source.size()));

        compressed_buffer[0] = static_cast<UInt8>(CompressionMethodByte::QPL);
        int res = QPL::QPL_compress(
            source.data(),
            source.size(),
            &compressed_buffer[header_size],
            QPL_Compressbound(source.size()));

        if (res == -1)
            throw Exception("Cannot compress block with QplCompressData", ErrorCodes::CANNOT_COMPRESS);

        compressed_size = header_size + res;

        UInt32 compressed_size_32 = compressed_size;
        UInt32 uncompressed_size_32 = source.size();

        unalignedStore<UInt32>(&compressed_buffer[1], compressed_size_32);
        unalignedStore<UInt32>(&compressed_buffer[5], uncompressed_size_32);

        break;
    }
#endif
    default:
        throw Exception("Unknown compression method", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
    }

    return compressed_size;
}

template <bool add_legacy_checksum>
void CompressedWriteBuffer<add_legacy_checksum>::nextImpl()
{
    if (!offset())
        return;

    const char * source = working_buffer.begin();
    const size_t source_size = offset();
    size_t compressed_size = CompressionEncode({source, source_size}, compression_settings, compressed_buffer);
    const auto * compressed_buffer_ptr = &compressed_buffer[0];

    if constexpr (add_legacy_checksum)
    {
        CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(compressed_buffer_ptr, compressed_size);
        out.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));
    }

    out.write(compressed_buffer_ptr, compressed_size);
}

template <bool add_legacy_checksum>
CompressedWriteBuffer<add_legacy_checksum>::CompressedWriteBuffer(
    WriteBuffer & out_,
    CompressionSettings compression_settings_,
    size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size)
    , out(out_)
    , compression_settings(compression_settings_)
{}

template <bool add_legacy_checksum>
CompressedWriteBuffer<add_legacy_checksum>::~CompressedWriteBuffer()
{
    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

template class CompressedWriteBuffer<true>;
template class CompressedWriteBuffer<false>;
template size_t CompressionEncode<PODArray<char>>(std::string_view, const CompressionSettings &, PODArray<char> &);
template size_t CompressionEncode<String>(std::string_view, const CompressionSettings &, String &);
} // namespace DB
