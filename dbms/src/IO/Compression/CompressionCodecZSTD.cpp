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
#include <IO/Compression/CompressionCodecZSTD.h>
#include <IO/Compression/CompressionInfo.h>
#include <zstd.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

UInt8 CompressionCodecZSTD::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::ZSTD);
}

UInt32 CompressionCodecZSTD::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return static_cast<UInt32>(ZSTD_compressBound(uncompressed_size));
}

UInt32 CompressionCodecZSTD::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    ZSTD_CCtx * cctx = ZSTD_createCCtx();
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, level);
    size_t compressed_size = ZSTD_compress2(cctx, dest, ZSTD_compressBound(source_size), source, source_size);
    ZSTD_freeCCtx(cctx);

    if (ZSTD_isError(compressed_size))
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with ZSTD codec: {}",
            ZSTD_getErrorName(compressed_size));

    return static_cast<UInt32>(compressed_size);
}

void CompressionCodecZSTD::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    size_t res = ZSTD_decompress(dest, uncompressed_size, source, source_size);

    if (ZSTD_isError(res))
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ZSTD-encoded data: {}",
            std::string(ZSTD_getErrorName(res)));
}

CompressionCodecZSTD::CompressionCodecZSTD(int level_)
    : level(level_)
{}

CompressionCodecPtr getCompressionCodecZSTD(int level)
{
    return std::make_shared<CompressionCodecZSTD>(level);
}

} // namespace DB
