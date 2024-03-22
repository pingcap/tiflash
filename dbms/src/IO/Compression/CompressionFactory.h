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

#pragma once

#include <Common/config.h>
#include <IO/Compression/CompressionCodecDelta.h>
#include <IO/Compression/CompressionCodecLZ4.h>
#include <IO/Compression/CompressionCodecNone.h>
#include <IO/Compression/CompressionCodecRLE.h>
#include <IO/Compression/CompressionCodecZSTD.h>
#include <IO/Compression/CompressionSettings.h>
#include <IO/Compression/ICompressionCodec.h>

#if USE_QPL
#include <IO/Compression/CompressionCodecDeflateQpl.h>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_COMPRESSION_METHOD;
}

class CompressionFactory
{
public:
    // Create codec for compressing data with specified settings.
    static CompressionCodecPtr createForCompress(const CompressionSettings & settings)
    {
        switch (settings.method)
        {
        case CompressionMethod::LZ4:
            return std::make_shared<CompressionCodecLZ4>(settings.level);
        case CompressionMethod::LZ4HC:
            return std::make_shared<CompressionCodecLZ4HC>(settings.level);
        case CompressionMethod::ZSTD:
            return std::make_shared<CompressionCodecZSTD>(settings.level);
#if USE_QPL
        case CompressionMethod::QPL:
            return std::make_shared<CompressionCodecDeflateQpl>();
#endif
        default:
            break;
        }
        switch (settings.method_byte)
        {
        case CompressionMethodByte::Delta:
            return std::make_shared<CompressionCodecDelta>(settings.type_bytes_size);
        case CompressionMethodByte::RLE:
            return std::make_shared<CompressionCodecRLE>(settings.type_bytes_size);
        case CompressionMethodByte::NONE:
            return std::make_shared<CompressionCodecNone>();
        default:
            throw Exception(
                ErrorCodes::UNKNOWN_COMPRESSION_METHOD,
                "Unknown compression method byte: {:02x}",
                static_cast<UInt16>(settings.method_byte));
        }
    }

    // Create codec for decompressing data with specified method byte.
    // All decompression codecs are stateless, so we don't need to pass settings.
    static CompressionCodecPtr createForDecompress(UInt8 method_byte)
    {
        CompressionSettings settings(static_cast<CompressionMethodByte>(method_byte));
        return createForCompress(settings);
    }
};

} // namespace DB
