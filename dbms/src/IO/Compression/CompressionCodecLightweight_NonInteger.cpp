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

#include <Common/Exception.h>
#include <IO/Compression/CompressionCodecLightweight.h>
#include <IO/Compression/CompressionSettings.h>
#include <lz4.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

size_t CompressionCodecLightweight::compressDataForNonInteger(const char * source, UInt32 source_size, char * dest)
{
    auto success = LZ4_compress_fast(
        source,
        dest,
        source_size,
        LZ4_COMPRESSBOUND(source_size),
        CompressionSetting::getDefaultLevel(CompressionMethod::LZ4));
    if (!success)
        throw Exception("Cannot LZ4_compress_fast", ErrorCodes::CANNOT_COMPRESS);
    return success;
}


void CompressionCodecLightweight::decompressDataForNonInteger(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size)
{
    if (unlikely(LZ4_decompress_safe(source, dest, source_size, output_size) < 0))
        throw Exception("Cannot LZ4_decompress_safe", ErrorCodes::CANNOT_DECOMPRESS);
}

} // namespace DB
