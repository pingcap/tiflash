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

#pragma once

#include <Common/config.h>
#include <IO/Compression/CompressionCodecDeltaFOR.h>
#include <IO/Compression/CompressionCodecFOR.h>
#include <IO/Compression/CompressionCodecLZ4.h>
#include <IO/Compression/CompressionCodecLightweight.h>
#include <IO/Compression/CompressionCodecMultiple.h>
#include <IO/Compression/CompressionCodecNone.h>
#include <IO/Compression/CompressionCodecRunLength.h>
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

class CompressionCodecFactory
{
public:
    template <bool IS_DECOMPRESS = false>
    static CompressionCodecPtr create(const CompressionSetting & setting);

    // Create codec for compressing/decompressing data with specified settings.
    static CompressionCodecPtr create(const CompressionSettings & settings);

    // Create codec for decompressing data with specified method byte.
    // All decompression codecs are stateless, so we don't need to pass settings.
    static CompressionCodecPtr createForDecompress(UInt8 method_byte);

private:
    static Codecs createCodecs(const CompressionSettings & settings);

    // Most codecs are stateless and can be shared.
    // This function returns a shared pointer to static codec instance.
    template <typename T>
    static CompressionCodecPtr getStaticCodec(const CompressionSetting & setting);
};

} // namespace DB
