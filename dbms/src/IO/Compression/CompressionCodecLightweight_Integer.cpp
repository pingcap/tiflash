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
#include <Common/TiFlashMetrics.h>
#include <IO/Compression/CompressionCodecLightweight.h>
#include <IO/Compression/CompressionSettings.h>
#include <IO/Compression/EncodingUtil.h>
#include <lz4.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

void CompressionCodecLightweight::IntegerCompressContext::update(size_t uncompressed_size, size_t compressed_size)
{
    if (mode == IntegerMode::LZ4)
    {
        GET_METRIC(tiflash_storage_pack_compression_bytes, type_lz4_uncompressed_bytes).Increment(uncompressed_size);
        GET_METRIC(tiflash_storage_pack_compression_bytes, type_lz4_compressed_bytes).Increment(compressed_size);
        GET_METRIC(tiflash_storage_pack_compression_algorithm_count, type_lz4).Increment();
        used_lz4 = true;
    }
    else
    {
        GET_METRIC(tiflash_storage_pack_compression_bytes, type_lightweight_uncompressed_bytes)
            .Increment(uncompressed_size);
        GET_METRIC(tiflash_storage_pack_compression_bytes, type_lightweight_compressed_bytes)
            .Increment(compressed_size);
    }
    switch (mode)
    {
    case IntegerMode::CONSTANT:
        GET_METRIC(tiflash_storage_pack_compression_algorithm_count, type_constant).Increment();
        break;
    case IntegerMode::CONSTANT_DELTA:
        GET_METRIC(tiflash_storage_pack_compression_algorithm_count, type_constant_delta).Increment();
        used_constant_delta = true;
        break;
    case IntegerMode::RunLength:
        GET_METRIC(tiflash_storage_pack_compression_algorithm_count, type_runlength).Increment();
        used_rle = true;
        break;
    case IntegerMode::FOR:
        GET_METRIC(tiflash_storage_pack_compression_algorithm_count, type_for).Increment();
        break;
    case IntegerMode::DELTA_FOR:
        GET_METRIC(tiflash_storage_pack_compression_algorithm_count, type_delta_for).Increment();
        used_delta_for = true;
        break;
    default:
        break;
    }
    // Since analyze CONSTANT is extremely fast, so it will not be counted in the round.
    if (mode != IntegerMode::CONSTANT)
    {
        ++compress_count;
        resetIfNeed();
    }
}

// Every ROUND_COUNT times as a round.
// At the beginning of each round, analyze once.
// During the round, if once used lz4, do not analyze anymore, and use lz4 directly.
bool CompressionCodecLightweight::IntegerCompressContext::needAnalyze() const
{
    return compress_count == 0 || !used_lz4;
}

void CompressionCodecLightweight::IntegerCompressContext::resetIfNeed()
{
    if (compress_count >= ROUND_COUNT)
    {
        compress_count = 0;
        used_lz4 = false;
        used_constant_delta = false;
        used_delta_for = false;
        used_rle = false;
    }
}

template <std::integral T>
bool CompressionCodecLightweight::IntegerCompressContext::needAnalyzeDelta() const
{
    return !std::is_same_v<T, UInt8> && (compress_count == 0 || used_constant_delta || used_delta_for);
}

template <std::integral T>
constexpr bool CompressionCodecLightweight::IntegerCompressContext::needAnalyzeFOR()
{
    // The performance of FOR is not good for UInt16, so we do not use FOR for UInt16.
    return !std::is_same_v<T, UInt16>;
}

bool CompressionCodecLightweight::IntegerCompressContext::needAnalyzeRunLength() const
{
    return compress_count == 0 || used_rle;
}

template <std::integral T>
void CompressionCodecLightweight::IntegerCompressContext::analyze(std::span<const T> & values, IntegerState<T> & state)
{
    if (values.empty())
    {
        mode = IntegerMode::Invalid;
        return;
    }

    if (!needAnalyze())
    {
        mode = IntegerMode::LZ4;
        return;
    }

    // Check CONSTANT
    auto minmax_value = std::minmax_element(values.begin(), values.end());
    T min_value = *minmax_value.first;
    T max_value = *minmax_value.second;
    if (min_value == max_value)
    {
        state = min_value;
        mode = IntegerMode::CONSTANT;
        return;
    }

    using TS = std::make_signed_t<T>;
    std::vector<T> deltas;
    UInt8 delta_for_width = sizeof(T) * 8;
    TS min_delta = std::numeric_limits<TS>::max();
    if (needAnalyzeDelta<T>())
    {
        // Check CONSTANT_DELTA

        // If values.size() == 1, mode will be CONSTANT
        // so values.size() must be greater than 1 here and deltas must be non empty.
        deltas.reserve(values.size() - 1);
        TS max_delta = std::numeric_limits<TS>::min();
        for (size_t i = 1; i < values.size(); ++i)
        {
            TS delta = static_cast<TS>(values[i]) - static_cast<TS>(values[i - 1]);
            min_delta = std::min(min_delta, delta);
            max_delta = std::max(max_delta, delta);
            deltas.push_back(delta);
        }
        if (min_delta == max_delta)
        {
            state = static_cast<T>(min_delta);
            mode = IntegerMode::CONSTANT_DELTA;
            return;
        }

        // DELTA_FOR
        if constexpr (needAnalyzeFOR<T>())
        {
            delta_for_width = BitpackingPrimitives::minimumBitWidth<T, false>(static_cast<T>(max_delta - min_delta));
        }
    }

    // Check RunLength
    size_t estimate_rle_size = std::numeric_limits<size_t>::max();
    if (needAnalyzeRunLength())
    {
        estimate_rle_size = Compression::estimateRunLengthDecodedByteSize(values.data(), values.size());
    }

    size_t estimate_lz_size = values.size() * sizeof(T) / ESRTIMATE_LZ4_COMPRESSION_RATIO;

    UInt8 for_width = BitpackingPrimitives::minimumBitWidth<T>(max_value - min_value);
    // min_delta, and 1 byte for width, and the rest for compressed data
    static constexpr auto FOR_EXTRA_BYTES = sizeof(T) + sizeof(UInt8);
    size_t for_size = BitpackingPrimitives::getRequiredSize(values.size(), for_width) + FOR_EXTRA_BYTES;

    // values[0], min_delta, 1 byte for width, and the rest for compressed data
    static constexpr auto DFOR_EXTRA_BYTES = sizeof(T) + sizeof(UInt8) + sizeof(T);
    size_t delta_for_size = BitpackingPrimitives::getRequiredSize(deltas.size(), delta_for_width) + DFOR_EXTRA_BYTES;

    if (needAnalyzeRunLength() && estimate_rle_size < delta_for_size && estimate_rle_size < for_size
        && estimate_rle_size < estimate_lz_size)
    {
        mode = IntegerMode::RunLength;
    }
    else if (needAnalyzeFOR<T>() && for_size < delta_for_size && for_size < estimate_lz_size)
    {
        std::vector<T> values_copy(values.begin(), values.end());
        state = FORState<T>{std::move(values_copy), min_value, for_width};
        mode = IntegerMode::FOR;
    }
    else if (needAnalyzeDelta<T>() && delta_for_size < estimate_lz_size)
    {
        state = DeltaFORState<T>{std::move(deltas), min_delta, delta_for_width};
        mode = IntegerMode::DELTA_FOR;
    }
    else
    {
        mode = IntegerMode::LZ4;
    }
}

template <std::integral T>
size_t CompressionCodecLightweight::compressDataForInteger(const char * source, UInt32 source_size, char * dest) const
{
    const auto bytes_size = static_cast<UInt8>(data_type);
    assert(bytes_size == sizeof(T));
    if unlikely (source_size % bytes_size != 0)
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with lightweight-integer codec, data size {} is not aligned to {}",
            source_size,
            bytes_size);

    // Load values
    const size_t count = source_size / bytes_size;
    std::span<const T> values(reinterpret_cast<const T *>(source), count);

    // Analyze
    IntegerState<T> state;
    ctx.analyze<T>(values, state);

    // Compress
    unalignedStore<UInt8>(dest, static_cast<UInt8>(ctx.mode));
    dest += sizeof(UInt8);
    size_t compressed_size = 1;
    switch (ctx.mode)
    {
    case IntegerMode::CONSTANT:
    {
        compressed_size += Compression::constantEncoding(std::get<0>(state), dest);
        break;
    }
    case IntegerMode::CONSTANT_DELTA:
    {
        compressed_size += Compression::constantDeltaEncoding(values[0], std::get<0>(state), dest);
        break;
    }
    case IntegerMode::RunLength:
    {
        compressed_size += Compression::runLengthEncoding<T>(source, source_size, dest);
        break;
    }
    case IntegerMode::FOR:
    {
        FORState for_state = std::get<1>(state);
        compressed_size += Compression::FOREncoding(
            for_state.values.data(),
            for_state.values.size(),
            for_state.min_value,
            for_state.bit_width,
            dest);
        break;
    }
    case IntegerMode::DELTA_FOR:
    {
        DeltaFORState delta_for_state = std::get<2>(state);
        unalignedStore<T>(dest, values[0]);
        dest += sizeof(T);
        compressed_size += sizeof(T);
        compressed_size += Compression::FOREncoding(
            delta_for_state.deltas.data(),
            delta_for_state.deltas.size(),
            static_cast<T>(delta_for_state.min_delta_value),
            delta_for_state.bit_width,
            dest);
        break;
    }
    case IntegerMode::LZ4:
    {
        auto success = LZ4_compress_fast(
            source,
            dest,
            source_size,
            LZ4_COMPRESSBOUND(source_size),
            CompressionSetting::getDefaultLevel(CompressionMethod::LZ4));
        if (unlikely(!success))
            throw Exception("Cannot LZ4_compress_fast", ErrorCodes::CANNOT_COMPRESS);
        compressed_size += success;
        break;
    }
    default:
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with lightweight-integer codec, unknown mode {}",
            static_cast<int>(ctx.mode));
    }

    // Update statistics
    ctx.update(source_size, compressed_size);

    return compressed_size;
}

template <std::integral T>
void CompressionCodecLightweight::decompressDataForInteger(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size) const
{
    if unlikely (output_size % sizeof(T) != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress lightweight-integer codec data. Uncompressed size {} is not aligned to {}",
            output_size,
            sizeof(T));

    auto mode = static_cast<IntegerMode>(unalignedLoad<UInt8>(source));
    source += sizeof(UInt8);
    source_size -= sizeof(UInt8);
    switch (mode)
    {
    case IntegerMode::CONSTANT:
        Compression::constantDecoding<T>(source, source_size, dest, output_size);
        break;
    case IntegerMode::CONSTANT_DELTA:
        Compression::constantDeltaDecoding<T>(source, source_size, dest, output_size);
        break;
    case IntegerMode::RunLength:
        Compression::runLengthDecoding<T>(source, source_size, dest, output_size);
        break;
    case IntegerMode::FOR:
        Compression::FORDecoding<T>(source, source_size, dest, output_size);
        break;
    case IntegerMode::DELTA_FOR:
        Compression::deltaFORDecoding<T>(source, source_size, dest, output_size);
        break;
    case IntegerMode::LZ4:
        if (unlikely(LZ4_decompress_safe(source, dest, source_size, output_size) < 0))
            throw Exception("Cannot LZ4_decompress_safe", ErrorCodes::CANNOT_DECOMPRESS);
        break;
    default:
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress with lightweight-integer codec, unknown mode {}",
            static_cast<int>(mode));
    }
}

template size_t CompressionCodecLightweight::compressDataForInteger<UInt8>(
    const char * source,
    UInt32 source_size,
    char * dest) const;
template size_t CompressionCodecLightweight::compressDataForInteger<UInt16>(
    const char * source,
    UInt32 source_size,
    char * dest) const;
template size_t CompressionCodecLightweight::compressDataForInteger<UInt32>(
    const char * source,
    UInt32 source_size,
    char * dest) const;
template size_t CompressionCodecLightweight::compressDataForInteger<UInt64>(
    const char * source,
    UInt32 source_size,
    char * dest) const;
template void CompressionCodecLightweight::decompressDataForInteger<UInt8>(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size) const;
template void CompressionCodecLightweight::decompressDataForInteger<UInt16>(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size) const;
template void CompressionCodecLightweight::decompressDataForInteger<UInt32>(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size) const;
template void CompressionCodecLightweight::decompressDataForInteger<UInt64>(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size) const;

} // namespace DB
