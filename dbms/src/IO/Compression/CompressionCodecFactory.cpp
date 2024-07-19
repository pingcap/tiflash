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

#include <IO/Compression/CompressionCodecFactory.h>


namespace DB
{

template <typename T>
CompressionCodecPtr CompressionCodecFactory::getStaticCodec(const CompressionSetting & setting)
{
    switch (setting.data_type)
    {
    case CompressionDataType::Int8:
    {
        static auto codec = std::make_shared<T>(CompressionDataType::Int8);
        return codec;
    }
    case CompressionDataType::Int16:
    {
        static auto codec = std::make_shared<T>(CompressionDataType::Int16);
        return codec;
    }
    case CompressionDataType::Int32:
    {
        static auto codec = std::make_shared<T>(CompressionDataType::Int32);
        return codec;
    }
    case CompressionDataType::Int64:
    {
        static auto codec = std::make_shared<T>(CompressionDataType::Int64);
        return codec;
    }
    default:
        __builtin_unreachable();
    }
}

template CompressionCodecPtr CompressionCodecFactory::getStaticCodec<CompressionCodecFOR>(
    const CompressionSetting & setting);
template CompressionCodecPtr CompressionCodecFactory::getStaticCodec<CompressionCodecDeltaFOR>(
    const CompressionSetting & setting);
template CompressionCodecPtr CompressionCodecFactory::getStaticCodec<CompressionCodecRunLength>(
    const CompressionSetting & setting);

template <>
CompressionCodecPtr CompressionCodecFactory::getStaticCodec<CompressionCodecLZ4>(const CompressionSetting & setting)
{
    static constexpr auto MAX_LZ4_MAP_SIZE = 10;
    static std::unordered_map<int, CompressionCodecPtr> lz4_map;
    auto it = lz4_map.find(setting.level);
    if (it != lz4_map.end())
        return it->second;
    if (lz4_map.size() >= MAX_LZ4_MAP_SIZE)
        lz4_map.clear();
    lz4_map.emplace(setting.level, std::make_shared<CompressionCodecLZ4>(setting.level));
    return lz4_map[setting.level];
}

template <>
CompressionCodecPtr CompressionCodecFactory::getStaticCodec<CompressionCodecLZ4HC>(const CompressionSetting & setting)
{
    static constexpr auto MAX_LZ4HC_MAP_SIZE = 10;
    static std::unordered_map<int, CompressionCodecPtr> lz4hc_map;
    auto it = lz4hc_map.find(setting.level);
    if (it != lz4hc_map.end())
        return it->second;
    if (lz4hc_map.size() >= MAX_LZ4HC_MAP_SIZE)
        lz4hc_map.clear();
    lz4hc_map.emplace(setting.level, std::make_shared<CompressionCodecLZ4HC>(setting.level));
    return lz4hc_map[setting.level];
}

template <>
CompressionCodecPtr CompressionCodecFactory::getStaticCodec<CompressionCodecZSTD>(const CompressionSetting & setting)
{
    // ZSTD level is in the range [1, 22], the maximum size of the map is 22.
    static std::vector<CompressionCodecPtr> zstd_map = {
        std::make_shared<CompressionCodecZSTD>(1),  std::make_shared<CompressionCodecZSTD>(2),
        std::make_shared<CompressionCodecZSTD>(3),  std::make_shared<CompressionCodecZSTD>(4),
        std::make_shared<CompressionCodecZSTD>(5),  std::make_shared<CompressionCodecZSTD>(6),
        std::make_shared<CompressionCodecZSTD>(7),  std::make_shared<CompressionCodecZSTD>(8),
        std::make_shared<CompressionCodecZSTD>(9),  std::make_shared<CompressionCodecZSTD>(10),
        std::make_shared<CompressionCodecZSTD>(11), std::make_shared<CompressionCodecZSTD>(12),
        std::make_shared<CompressionCodecZSTD>(13), std::make_shared<CompressionCodecZSTD>(14),
        std::make_shared<CompressionCodecZSTD>(15), std::make_shared<CompressionCodecZSTD>(16),
        std::make_shared<CompressionCodecZSTD>(17), std::make_shared<CompressionCodecZSTD>(18),
        std::make_shared<CompressionCodecZSTD>(19), std::make_shared<CompressionCodecZSTD>(20),
        std::make_shared<CompressionCodecZSTD>(21), std::make_shared<CompressionCodecZSTD>(22),
    };
    return zstd_map[setting.level - 1];
}

template <>
CompressionCodecPtr CompressionCodecFactory::getStaticCodec<CompressionCodecNone>(const CompressionSetting &)
{
    static auto none = std::make_shared<CompressionCodecNone>();
    return none;
}

#if USE_QPL
template <>
CompressionCodecPtr CompressionCodecFactory::getStaticCodec<CompressionCodecDeflateQpl>(const CompressionSetting &)
{
    static auto qpl = std::make_shared<CompressionCodecDeflateQpl>();
    return qpl;
}
#endif


template <bool IS_DECOMPRESS>
CompressionCodecPtr CompressionCodecFactory::create(const CompressionSetting & setting)
{
    // LZ4 and LZ4HC have the same format, the difference is only in compression.
    // So they have the same method byte.
    if (setting.method == CompressionMethod::LZ4HC)
        return getStaticCodec<CompressionCodecLZ4HC>(setting);
    if (setting.method_byte == CompressionMethodByte::LZ4)
        return getStaticCodec<CompressionCodecLZ4>(setting);
    if (setting.method_byte == CompressionMethodByte::ZSTD)
        return getStaticCodec<CompressionCodecZSTD>(setting);

#if USE_QPL
    if (setting.method_byte == CompressionMethodByte::QPL)
        return getStaticCodec<CompressionCodecDeflateQpl>(setting);
#endif

    // For non-integral types, we always use LZ4 now.
    if constexpr (!IS_DECOMPRESS)
    {
        if (setting.data_type == CompressionDataType::String || setting.data_type == CompressionDataType::Float32
            || setting.data_type == CompressionDataType::Float64 || setting.data_type == CompressionDataType::Unknown)
            return getStaticCodec<CompressionCodecLZ4>(setting);
    }

    switch (setting.method_byte)
    {
    case CompressionMethodByte::Lightweight:
        return std::make_unique<CompressionCodecLightweight>(setting.data_type, setting.level);
    case CompressionMethodByte::DeltaFOR:
        return getStaticCodec<CompressionCodecDeltaFOR>(setting);
    case CompressionMethodByte::RunLength:
        return getStaticCodec<CompressionCodecRunLength>(setting);
    case CompressionMethodByte::FOR:
        return getStaticCodec<CompressionCodecFOR>(setting);
    case CompressionMethodByte::NONE:
        return getStaticCodec<CompressionCodecNone>(setting);
    default:
        throw Exception(
            ErrorCodes::UNKNOWN_COMPRESSION_METHOD,
            "Unknown compression method byte: {:02x}",
            static_cast<UInt16>(setting.method_byte));
    }
}

CompressionCodecPtr CompressionCodecFactory::create(const CompressionSettings & settings)
{
    RUNTIME_CHECK(!settings.settings.empty());
    return settings.settings.size() > 1 ? std::make_unique<CompressionCodecMultiple>(createCodecs(settings))
                                        : create(settings.settings.front());
}

CompressionCodecPtr CompressionCodecFactory::createForDecompress(UInt8 method_byte)
{
    CompressionSetting setting(static_cast<CompressionMethodByte>(method_byte));
    setting.data_type = CompressionDataType::Int8;
    return create</*IS_DECOMPRESS*/ true>(setting);
}

Codecs CompressionCodecFactory::createCodecs(const CompressionSettings & settings)
{
    Codecs codecs;
    codecs.reserve(settings.settings.size());
    for (const auto & setting : settings.settings)
    {
        codecs.push_back(create(setting));
    }
    return codecs;
}

} // namespace DB
