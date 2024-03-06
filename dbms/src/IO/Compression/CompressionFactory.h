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
#include <IO/Compression/CompressionCodecLZ4.h>
#include <IO/Compression/CompressionCodecNone.h>
#include <IO/Compression/CompressionCodecZSTD.h>
#include <IO/Compression/CompressionSettings.h>
#include <IO/Compression/ICompressionCodec.h>

#include <magic_enum.hpp>

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
    static CompressionCodecPtr create(const CompressionSettings & settings)
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
        case CompressionMethod::NONE:
            return std::make_shared<CompressionCodecNone>();
        default:
            throw Exception(
                ErrorCodes::UNKNOWN_COMPRESSION_METHOD,
                "Unknown compression method: {}",
                static_cast<int>(settings.method));
        }
    }

    static CompressionCodecPtr create(UInt8 method_byte)
    {
        CompressionSettings settings;
        auto mb = magic_enum::enum_cast<CompressionMethodByte>(method_byte);
        if (!mb.has_value())
            throw Exception(ErrorCodes::UNKNOWN_COMPRESSION_METHOD, "Unknown compression method byte: {}", method_byte);
        switch (mb.value())
        {
        case CompressionMethodByte::LZ4:
            settings.method = CompressionMethod::LZ4;
            break;
        case CompressionMethodByte::ZSTD:
            settings.method = CompressionMethod::ZSTD;
            break;
#if USE_QPL
        case CompressionMethodByte::QPL:
            settings.method = CompressionMethod::QPL;
            break;
#endif
        case CompressionMethodByte::NONE:
            settings.method = CompressionMethod::NONE;
            break;
        default:
            throw Exception(ErrorCodes::UNKNOWN_COMPRESSION_METHOD, "Unknown compression method byte: {}", method_byte);
        }
        return create(settings);
    }
};

} // namespace DB
