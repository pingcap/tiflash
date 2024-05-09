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

#include <Common/BitpackingPrimitives.h>
#include <Common/Exception.h>
#include <IO/Compression/CompressionCodecIntegerLightweight.h>
#include <IO/Compression/CompressionInfo.h>
#include <IO/Compression/CompressionSettings.h>
#include <IO/Compression/EncodingUtil.h>
#include <common/likely.h>
#include <common/unaligned.h>
#include <lz4.h>

#include <algorithm>
#include <limits>


namespace DB
{

// TODO: metrics

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

CompressionCodecIntegerLightweight::CompressionCodecIntegerLightweight(UInt8 bytes_size_)
    : bytes_size(bytes_size_)
{}

UInt8 CompressionCodecIntegerLightweight::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::Lightweight);
}

UInt32 CompressionCodecIntegerLightweight::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // 1 byte for bytes_size, 1 byte for mode, and the rest for compressed data
    return 1 + 1 + uncompressed_size;
}

template <typename T>
size_t CompressionCodecIntegerLightweight::compressDataForType(const char * source, UInt32 source_size, char * dest)
    const
{
    if (source_size % sizeof(T) != 0)
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with lightweight codec, data size {} is not aligned to {}",
            source_size,
            sizeof(T));

    // Load values
    const size_t count = source_size / sizeof(T);
    std::vector<T> values(count);
    for (size_t i = 0; i < count; ++i)
    {
        values[i] = unalignedLoad<T>(source + i * sizeof(T));
    }

    // Analyze
    State<T> state;
    ctx.analyze<T>(values, state);

    // Compress
    unalignedStore<UInt8>(dest, static_cast<UInt8>(ctx.mode));
    dest += sizeof(UInt8);
    size_t compressed_size = 1;
    switch (ctx.mode)
    {
    case Mode::CONSTANT:
    {
        compressed_size += Compression::ConstantEncoding(std::get<0>(state), dest);
        break;
    }
    case Mode::CONSTANT_DELTA:
    {
        compressed_size += Compression::ConstantDeltaEncoding(values[0], std::get<0>(state), dest);
        break;
    }
    case Mode::RLE:
    {
        compressed_size += Compression::RLEEncoding<T>(std::get<1>(state), dest);
        break;
    }
    case Mode::FOR:
    {
        FORState for_state = std::get<2>(state);
        compressed_size += Compression::FOREncoding(values, for_state.min_value, for_state.bit_width, dest);
        break;
    }
    case Mode::DELTA_FOR:
    {
        DeltaFORState delta_for_state = std::get<3>(state);
        compressed_size += Compression::FOREncoding<typename std::make_signed_t<T>, true>(
            delta_for_state.deltas,
            delta_for_state.min_delta_value,
            delta_for_state.bit_width,
            dest);
        break;
    }
    case Mode::LZ4:
    {
        auto success = LZ4_compress_fast(
            source,
            dest,
            source_size,
            LZ4_COMPRESSBOUND(source_size),
            CompressionSetting::getDefaultLevel(CompressionMethod::LZ4));
        if (!success)
            throw Exception("Cannot LZ4_compress_fast", ErrorCodes::CANNOT_COMPRESS);
        compressed_size += success;
        break;
    }
    default:
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with lightweight codec, unknown mode {}",
            static_cast<int>(ctx.mode));
    }

    // Update statistics
    ctx.update(source_size, compressed_size);

    return compressed_size;
}

template <typename T>
void CompressionCodecIntegerLightweight::decompressDataForType(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size) const
{
    auto mode = static_cast<Mode>(unalignedLoad<UInt8>(source));
    source += sizeof(UInt8);
    source_size -= sizeof(UInt8);
    switch (mode)
    {
    case Mode::CONSTANT:
        Compression::ConstantDecoding<T>(source, source_size, dest, output_size);
        break;
    case Mode::CONSTANT_DELTA:
        Compression::ConstantDeltaDecoding<T>(source, source_size, dest, output_size);
        break;
    case Mode::RLE:
        Compression::RLEDecoding<T>(source, source_size, dest, output_size);
        break;
    case Mode::FOR:
        Compression::FORDecoding<T>(source, source_size, dest, output_size);
        break;
    case Mode::DELTA_FOR:
        Compression::DeltaFORDecoding<T>(source, source_size, dest, output_size);
        break;
    case Mode::LZ4:
        if (unlikely(LZ4_decompress_safe(source, dest, source_size, output_size) < 0))
            throw Exception("Cannot LZ4_decompress_safe", ErrorCodes::CANNOT_DECOMPRESS);
        break;
    default:
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress with lightweight codec, unknown mode {}",
            static_cast<int>(mode));
    }
}

void CompressionCodecIntegerLightweight::CompressContext::update(size_t uncompressed_size, size_t compressed_size)
{
    if (mode == Mode::LZ4)
    {
        lz4_uncompressed_size += uncompressed_size;
        lz4_compressed_size += compressed_size;
        ++lz4_counter;
    }
    else
    {
        lw_uncompressed_size += uncompressed_size;
        lw_compressed_size += compressed_size;
        ++lw_counter;
    }
}

bool CompressionCodecIntegerLightweight::CompressContext::needAnalyze() const
{
    // lightweight codec is never used, do not analyze anymore
    if (lz4_counter > 5 && lw_counter == 0)
        return false;
    // if lz4 is used more than 5 times and the compression ratio is better than lightweight codec, do not analyze anymore
    if (lz4_counter > 5 && lz4_uncompressed_size / lz4_compressed_size > lw_compressed_size / lw_uncompressed_size)
        return false;
    return true;
}

template <typename T>
void CompressionCodecIntegerLightweight::CompressContext::analyze(std::vector<T> & values, State<T> & state)
{
    if (!needAnalyze())
        return;

    if (values.empty())
    {
        mode = Mode::Invalid;
        return;
    }

    // Check CONSTANT
    std::vector<std::pair<T, UInt8>> rle;
    rle.reserve(values.size());
    rle.emplace_back(values[0], 1);
    for (size_t i = 1; i < values.size(); ++i)
    {
        if (values[i] != values[i - 1] || rle.back().second == std::numeric_limits<UInt8>::max())
            rle.emplace_back(values[i], 1);
        else
            ++rle.back().second;
    }
    T min_value = *std::min_element(values.cbegin(), values.cend());
    T max_value = *std::max_element(values.cbegin(), values.cend());
    if (rle.size() == 1)
    {
        state = rle[0].first;
        mode = Mode::CONSTANT;
        return;
    }

    // Check CONSTANT_DELTA
    using TS = std::make_signed_t<T>;
    std::vector<TS> deltas;
    deltas.reserve(values.size());
    deltas.push_back(values[0]);
    for (size_t i = 1; i < values.size(); ++i)
    {
        deltas.push_back(values[i] - values[i - 1]);
    }
    TS min_delta = *std::min_element(deltas.cbegin(), deltas.cend());
    TS max_delta = *std::max_element(deltas.cbegin(), deltas.cend());
    if (min_delta == max_delta)
    {
        state = static_cast<T>(min_delta);
        mode = Mode::CONSTANT_DELTA;
        return;
    }

    UInt8 delta_for_width = Compression::FOREncodingWidth(deltas, min_delta);
    // additional T bytes for min_delta, and 1 byte for width
    size_t delta_for_size
        = BitpackingPrimitives::getRequiredSize(deltas.size(), delta_for_width) + sizeof(T) + sizeof(UInt8);
    UInt8 for_width = BitpackingPrimitives::minimumBitWidth<T>(max_value - min_value);
    // additional T bytes for min_value, and 1 byte for width
    size_t for_size = BitpackingPrimitives::getRequiredSize(values.size(), for_width) + sizeof(T) + sizeof(UInt8);
    size_t origin_size = values.size() * sizeof(T);
    size_t rle_size = Compression::RLEPairsSize(rle);
    if (rle_size < delta_for_size && rle_size < for_size && rle_size < origin_size)
    {
        state = std::move(rle);
        mode = Mode::RLE;
    }
    else if (for_size < delta_for_size && for_size < origin_size)
    {
        state = FORState<T>{min_value, for_width};
        mode = Mode::FOR;
    }
    else if (delta_for_size < origin_size)
    {
        state = DeltaFORState<T>{deltas, min_delta, delta_for_width};
        mode = Mode::DELTA_FOR;
    }
    else
    {
        mode = Mode::LZ4;
    }
}

UInt32 CompressionCodecIntegerLightweight::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if unlikely (source_size % bytes_size != 0)
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with lightweight codec, data size {} is not aligned to {}",
            source_size,
            bytes_size);

    dest[0] = bytes_size;
    dest += 1;
    switch (bytes_size)
    {
    case 1:
        return 1 + compressDataForType<UInt8>(source, source_size, dest);
    case 2:
        return 1 + compressDataForType<UInt16>(source, source_size, dest);
    case 4:
        return 1 + compressDataForType<UInt32>(source, source_size, dest);
    case 8:
        return 1 + compressDataForType<UInt64>(source, source_size, dest);
    default:
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with lightweight codec, unknown bytes size {}",
            bytes_size);
    }
}

void CompressionCodecIntegerLightweight::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if unlikely (source_size < 2)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress lightweight-encoded data. File has wrong header");

    if (uncompressed_size == 0)
        return;

    UInt8 bytes_size = source[0];

    if unlikely (bytes_size != 1 && bytes_size != 2 && bytes_size != 4 && bytes_size != 8)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress lightweight-encoded data. File has wrong header");

    if unlikely (uncompressed_size % bytes_size != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress lightweight-encoded data. Uncompressed size {} is not aligned to {}",
            uncompressed_size,
            bytes_size);

    UInt32 source_size_no_header = source_size - 1;
    switch (bytes_size)
    {
    case 1:
        decompressDataForType<UInt8>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case 2:
        decompressDataForType<UInt16>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case 4:
        decompressDataForType<UInt32>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case 8:
        decompressDataForType<UInt64>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    default:
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot compress with lightweight codec, unknown bytes size {}",
            bytes_size);
    }
}

} // namespace DB
