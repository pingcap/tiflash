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
#include <IO/Compression/EncodingUtil.h>
#include <lz4.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

String CompressionCodecLightweight::IntegerCompressContext::toDebugString() const
{
    return fmt::format(
        "lz4: {}, lightweight: {}, constant_delta: {}, delta_for: {}, rle: {}, lz4 {} -> {}, lightweight {} -> {}",
        lz4_counter,
        lw_counter,
        constant_delta_counter,
        delta_for_counter,
        rle_counter,
        lz4_uncompressed_size,
        lz4_compressed_size,
        lw_uncompressed_size,
        lw_compressed_size);
}

void CompressionCodecLightweight::IntegerCompressContext::update(size_t uncompressed_size, size_t compressed_size)
{
    if (mode == IntegerMode::LZ4)
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
    if (mode == IntegerMode::CONSTANT_DELTA)
        ++constant_delta_counter;
    if (mode == IntegerMode::DELTA_FOR)
        ++delta_for_counter;
    if (mode == IntegerMode::RunLength)
        ++rle_counter;
}

bool CompressionCodecLightweight::IntegerCompressContext::needAnalyze() const
{
    // lightweight codec is never used, do not analyze anymore
    if (lz4_counter > COUNT_THRESHOLD && lw_counter == 0)
        return false;
    // if lz4 is used more than COUNT_THRESHOLD times and the compression ratio is better than lightweight codec, do not analyze anymore
    if (lz4_counter > COUNT_THRESHOLD
        && lz4_uncompressed_size / lz4_compressed_size > lw_uncompressed_size / lw_compressed_size)
        return false;
    return true;
}

bool CompressionCodecLightweight::IntegerCompressContext::needAnalyzeDelta() const
{
    return lw_counter <= COUNT_THRESHOLD || constant_delta_counter != 0 || delta_for_counter != 0;
}

bool CompressionCodecLightweight::IntegerCompressContext::needAnalyzeRunLength() const
{
    return lw_counter <= COUNT_THRESHOLD || rle_counter != 0;
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
        RUNTIME_CHECK(mode == IntegerMode::LZ4);
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
    std::vector<TS> deltas;
    UInt8 delta_for_width = sizeof(T) * 8;
    size_t delta_for_size = std::numeric_limits<size_t>::max();
    TS min_delta = std::numeric_limits<TS>::min();
    if (needAnalyzeDelta())
    {
        // Check CONSTANT_DELTA

        // If values.size() == 1, mode will be CONSTANT
        // so values.size() must be greater than 1 here and deltas must be non empty.
        assert(values.size() > 1);
        deltas.reserve(values.size() - 1);
        for (size_t i = 1; i < values.size(); ++i)
        {
            deltas.push_back(values[i] - values[i - 1]);
        }
        auto minmax_delta = std::minmax_element(deltas.cbegin(), deltas.cend());
        min_delta = *minmax_delta.first;
        if (min_delta == *minmax_delta.second)
        {
            state = static_cast<T>(min_delta);
            mode = IntegerMode::CONSTANT_DELTA;
            return;
        }

        // DELTA_FOR
        delta_for_width = Compression::FOREncodingWidth(deltas, min_delta);
        // values[0], min_delta, 1 byte for width, and the rest for compressed data
        static constexpr auto ADDTIONAL_BYTES = sizeof(T) + sizeof(UInt8) + sizeof(T);
        delta_for_size = BitpackingPrimitives::getRequiredSize(deltas.size(), delta_for_width) + ADDTIONAL_BYTES;
    }

    // Check RunLength
    size_t estimate_rle_size = std::numeric_limits<size_t>::max();
    if (needAnalyzeRunLength())
    {
        estimate_rle_size = Compression::estimateRunLengthDecodedByteSize(values.data(), values.size());
    }

    UInt8 for_width = BitpackingPrimitives::minimumBitWidth<T>(max_value - min_value);
    // additional T bytes for min_delta, and 1 byte for width
    static constexpr auto ADDTIONAL_BYTES = sizeof(T) + sizeof(UInt8);
    size_t for_size = BitpackingPrimitives::getRequiredSize(values.size(), for_width) + ADDTIONAL_BYTES;

    size_t estimate_lz_size = values.size() * sizeof(T) / ESRTIMATE_LZ4_COMPRESSION_RATIO;
    if (needAnalyzeRunLength() && estimate_rle_size < delta_for_size && estimate_rle_size < for_size
        && estimate_rle_size < estimate_lz_size)
    {
        mode = IntegerMode::RunLength;
    }
    else if (for_size < delta_for_size && for_size < estimate_lz_size)
    {
        std::vector<T> values_copy(values.begin(), values.end());
        state = FORState<T>{std::move(values_copy), min_value, for_width};
        mode = IntegerMode::FOR;
    }
    else if (needAnalyzeDelta() && delta_for_size < estimate_lz_size)
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
        compressed_size += Compression::FOREncoding(for_state.values, for_state.min_value, for_state.bit_width, dest);
        break;
    }
    case IntegerMode::DELTA_FOR:
    {
        DeltaFORState delta_for_state = std::get<2>(state);
        unalignedStore<T>(dest, values[0]);
        dest += sizeof(T);
        compressed_size += sizeof(T);
        compressed_size += Compression::FOREncoding<typename std::make_signed_t<T>, true>(
            delta_for_state.deltas,
            delta_for_state.min_delta_value,
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
