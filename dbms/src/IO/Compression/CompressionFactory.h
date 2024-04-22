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
#include <IO/Compression/CompressionCodecDeltaFor.h>
#include <IO/Compression/CompressionCodecLZ4.h>
#include <IO/Compression/CompressionCodecMultiple.h>
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
    static CompressionCodecPtr create(const CompressionSetting & setting)
    {
        switch (setting.method)
        {
        case CompressionMethod::LZ4:
            return std::make_unique<CompressionCodecLZ4>(setting.level);
        case CompressionMethod::LZ4HC:
            return std::make_unique<CompressionCodecLZ4HC>(setting.level);
        case CompressionMethod::ZSTD:
            return std::make_unique<CompressionCodecZSTD>(setting.level);
#if USE_QPL
        case CompressionMethod::QPL:
            return std::make_unique<CompressionCodecDeflateQpl>();
#endif
        default:
            break;
        }
        switch (setting.method_byte)
        {
        case CompressionMethodByte::DeltaFor:
            return std::make_unique<CompressionCodecDeltaFor>(setting.type_bytes_size);
        case CompressionMethodByte::RLE:
            return std::make_unique<CompressionCodecRLE>(setting.type_bytes_size);
        case CompressionMethodByte::NONE:
            return std::make_unique<CompressionCodecNone>();
        default:
            throw Exception(
                ErrorCodes::UNKNOWN_COMPRESSION_METHOD,
                "Unknown compression method byte: {:02x}",
                static_cast<UInt16>(setting.method_byte));
        }
    }

    // Create codec for compressing/decompressing data with specified settings.
    static CompressionCodecPtr create(const CompressionSettings & settings)
    {
        RUNTIME_CHECK(!settings.settings.empty());
        return settings.settings.size() > 1 ? std::make_unique<CompressionCodecMultiple>(createCodecs(settings))
                                            : create(settings.settings.front());
    }

    // Create codec for decompressing data with specified method byte.
    // All decompression codecs are stateless, so we don't need to pass settings.
    static CompressionCodecPtr createForDecompress(UInt8 method_byte)
    {
        CompressionSetting setting(static_cast<CompressionMethodByte>(method_byte));
        return create(setting);
    }

private:
    static Codecs createCodecs(const CompressionSettings & settings)
    {
        RUNTIME_CHECK(settings.settings.size() > 1);
        Codecs codecs;
        codecs.reserve(settings.settings.size());
        for (const auto & setting : settings.settings)
        {
            codecs.push_back(create(setting));
        }
        return codecs;
    }
};

} // namespace DB
