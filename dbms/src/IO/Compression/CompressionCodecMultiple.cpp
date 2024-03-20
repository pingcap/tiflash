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

#include <Common/PODArray.h>
#include <IO/Compression/CompressionCodecMultiple.h>
#include <IO/Compression/CompressionFactory.h>
#include <IO/Compression/CompressionInfo.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <common/unaligned.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CORRUPTED_DATA;
}

CompressionCodecMultiple::CompressionCodecMultiple(Codecs codecs_)
    : codecs(codecs_)
{}

UInt8 CompressionCodecMultiple::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::Multiple);
}

UInt32 CompressionCodecMultiple::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    UInt32 compressed_size = uncompressed_size;
    for (const auto & codec : codecs)
        compressed_size = codec->getCompressedReserveSize(compressed_size);

    ///    TotalCodecs  ByteForEachCodec       data
    return static_cast<UInt32>(sizeof(UInt8) + codecs.size() + compressed_size);
}

UInt32 CompressionCodecMultiple::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    PODArray<char> compressed_buf;
    PODArray<char> uncompressed_buf(source, source + source_size);

    dest[0] = static_cast<UInt8>(codecs.size());

    size_t codecs_byte_pos = 1;
    for (size_t idx = 0; idx < codecs.size(); ++idx, ++codecs_byte_pos)
    {
        const auto codec = codecs[idx];
        dest[codecs_byte_pos] = codec->getMethodByte();
        compressed_buf.resize(codec->getCompressedReserveSize(source_size));

        UInt32 size_compressed = codec->compress(uncompressed_buf.data(), source_size, compressed_buf.data());

        uncompressed_buf.swap(compressed_buf);
        source_size = size_compressed;
    }

    memcpy(&dest[1 + codecs.size()], uncompressed_buf.data(), source_size);

    return static_cast<UInt32>(1 + codecs.size() + source_size);
}

void CompressionCodecMultiple::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 decompressed_size) const
{
    if (source_size < 1 || !source[0])
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Wrong compression methods list");

    UInt8 compression_methods_size = source[0];

    /// Insert all data into compressed_buf
    PODArray<char> compressed_buf(&source[compression_methods_size + 1], &source[source_size]);
    PODArray<char> uncompressed_buf;
    source_size -= (compression_methods_size + 1);

    for (int idx = compression_methods_size - 1; idx >= 0; --idx)
    {
        UInt8 compression_method = source[idx + 1];
        const auto codec = CompressionFactory::create(compression_method);

        UInt32 uncompressed_size = readDecompressedBlockSize(compressed_buf.data());

        if (idx == 0 && uncompressed_size != decompressed_size)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Wrong final decompressed size in codec Multiple, got {}, expected {}",
                uncompressed_size,
                decompressed_size);

        uncompressed_buf.resize(uncompressed_size);
        codec->decompress(compressed_buf.data(), source_size, uncompressed_buf.data(), uncompressed_size);
        uncompressed_buf.swap(compressed_buf);
        source_size = uncompressed_size;
    }

    memcpy(dest, compressed_buf.data(), decompressed_size);
}

std::vector<UInt8> CompressionCodecMultiple::getCodecsBytesFromData(const char * source)
{
    std::vector<UInt8> result;
    UInt8 compression_methods_size = source[0];
    for (size_t i = 0; i < compression_methods_size; ++i)
        result.push_back(source[1 + i]);
    return result;
}

bool CompressionCodecMultiple::isCompression() const
{
    for (const auto & codec : codecs)
        if (codec->isCompression())
            return true;
    return false;
}

} // namespace DB
