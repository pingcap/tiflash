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
#include <IO/Compression/CompressionCodecLZ4.h>
#include <IO/Compression/CompressionInfo.h>
#include <lz4.h>
#include <lz4hc.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
extern const int ILLEGAL_CODEC_PARAMETER;
} // namespace ErrorCodes

CompressionCodecLZ4::CompressionCodecLZ4(int level_)
    : level(level_)
{}

UInt8 CompressionCodecLZ4::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::LZ4);
}

UInt32 CompressionCodecLZ4::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return LZ4_COMPRESSBOUND(uncompressed_size);
}

UInt32 CompressionCodecLZ4::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    return LZ4_compress_fast(source, dest, source_size, LZ4_COMPRESSBOUND(source_size), level);
}

void CompressionCodecLZ4::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if (unlikely(LZ4_decompress_safe(source, dest, source_size, uncompressed_size) < 0))
        throw Exception("Cannot LZ4_decompress_safe", ErrorCodes::CANNOT_DECOMPRESS);
}

UInt32 CompressionCodecLZ4HC::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    auto success = LZ4_compress_HC(source, dest, source_size, LZ4_COMPRESSBOUND(source_size), level);

    if (!success)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with LZ4 codec");

    return success;
}

CompressionCodecLZ4HC::CompressionCodecLZ4HC(int level_)
    : CompressionCodecLZ4(level_)
{}


CompressionCodecPtr getCompressionCodecLZ4(int level)
{
    return std::make_shared<CompressionCodecLZ4HC>(level);
}

} // namespace DB
