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
#include <IO/Buffer/BufferWithOwnMemory.h>
#include <IO/Buffer/ReadBuffer.h>
#include <IO/Compression/CompressedReadBufferBase.h>
#include <IO/Compression/CompressionFactory.h>
#include <IO/Compression/CompressionMethod.h>
#include <IO/WriteHelpers.h>
#include <city.h>


namespace DB
{
namespace ErrorCodes
{
extern const int TOO_LARGE_SIZE_COMPRESSED;
extern const int CHECKSUM_DOESNT_MATCH;
extern const int CANNOT_DECOMPRESS;
extern const int CORRUPTED_DATA;
} // namespace ErrorCodes


static void readHeaderAndGetCodec(const char * compressed_buffer, CompressionCodecPtr & codec)
{
    UInt8 method_byte = ICompressionCodec::readMethod(compressed_buffer);

    if (!codec)
    {
        codec = CompressionFactory::create(method_byte);
    }
    else if (codec->getMethodByte() != method_byte)
    {
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Data compressed with different methods, given method "
            "byte {#x}, previous method byte {#x}",
            method_byte,
            codec->getMethodByte());
    }
}

static void readHeaderAndGetCodecAndSize(
    const char * compressed_buffer,
    UInt8 header_size,
    CompressionCodecPtr & codec,
    size_t & size_decompressed,
    size_t & size_compressed_without_checksum)
{
    readHeaderAndGetCodec(compressed_buffer, codec);

    size_compressed_without_checksum = codec->readCompressedBlockSize(compressed_buffer);
    size_decompressed = codec->readDecompressedBlockSize(compressed_buffer);

    /// This is for clang static analyzer.
    assert(size_decompressed > 0);

    if (size_compressed_without_checksum > DBMS_MAX_COMPRESSED_SIZE)
        throw Exception(
            ErrorCodes::TOO_LARGE_SIZE_COMPRESSED,
            "Too large size_compressed_without_checksum: {}. "
            "Most likely corrupted data.",
            size_compressed_without_checksum);

    if (size_compressed_without_checksum < header_size)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Can't decompress data: "
            "the compressed data size ({}, this should include header size) is less than the header size ({})",
            size_compressed_without_checksum,
            static_cast<size_t>(header_size));
}

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

    constexpr UInt8 header_size = ICompressionCodec::getHeaderSize();

    own_compressed_buffer.resize(header_size);
    compressed_in->readStrict(own_compressed_buffer.data(), header_size);

    if (ICompressionCodec::readMethod(own_compressed_buffer.data())
        == static_cast<UInt8>(CompressionMethodByte::COL_END))
        return 0;

    readHeaderAndGetCodecAndSize(
        own_compressed_buffer.data(),
        header_size,
        codec,
        size_decompressed,
        size_compressed_without_checksum);

    size_t & size_compressed = size_compressed_without_checksum;
    /// Is whole compressed block located in 'compressed_in' buffer?
    if (compressed_in->offset() >= header_size
        && compressed_in->position() + size_compressed - header_size <= compressed_in->buffer().end())
    {
        compressed_in->position() -= header_size;
        compressed_buffer = compressed_in->position();
        compressed_in->position() += size_compressed;
    }
    else
    {
        own_compressed_buffer.resize(size_compressed);
        compressed_buffer = own_compressed_buffer.data();
        compressed_in->readStrict(compressed_buffer + header_size, size_compressed - header_size);
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
    readHeaderAndGetCodec(compressed_buffer, codec);
    codec->decompress(compressed_buffer, size_compressed_without_checksum, to, size_decompressed);
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
