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
#include <DataTypes/IDataType.h>
#include <IO/Compression/CompressionCodecLightweight.h>
#include <IO/Compression/CompressionInfo.h>
#include <IO/Compression/CompressionSettings.h>
#include <common/likely.h>
#include <common/unaligned.h>
#include <lz4.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

CompressionCodecLightweight::CompressionCodecLightweight(UInt8 bytes_size_)
    : bytes_size(bytes_size_)
{}

UInt8 CompressionCodecLightweight::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::Lightweight);
}

UInt32 CompressionCodecLightweight::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // 1 byte for bytes_size, 1 byte for mode, and the rest for compressed data
    return uncompressed_size + 1 + 1;
}

namespace
{

template <typename T>
size_t ConstantEncoding(T constant, char * dest)
{
    unalignedStore<T>(dest, constant);
    return sizeof(T);
}

template <typename T>
void ConstantDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    if (source_size < sizeof(T))
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress with bitpacking codec, data size {} is too small",
            source_size);

    T constant = unalignedLoad<T>(src);
    for (size_t i = 0; i < dest_size / sizeof(T); ++i)
    {
        unalignedStore<T>(dest, constant);
        dest += sizeof(T);
    }
}

template <typename T, typename TS = typename std::make_signed<T>::type>
size_t ConstantDeltaEncoding(T first_value, TS constant_delta, char * dest)
{
    unalignedStore<T>(dest, first_value);
    dest += sizeof(T);
    unalignedStore<TS>(dest, constant_delta);
    return sizeof(T) + sizeof(TS);
}

template <typename T, typename TS = typename std::make_signed<T>::type>
void ConstantDeltaDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    if (source_size < sizeof(T) + sizeof(TS))
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress with bitpacking codec, data size {} is too small",
            source_size);

    T first_value = unalignedLoad<T>(src);
    TS constant_delta = unalignedLoad<TS>(src + sizeof(T));
    for (size_t i = 0; i < dest_size / sizeof(T); ++i)
    {
        unalignedStore<T>(dest, first_value);
        first_value += constant_delta;
        dest += sizeof(T);
    }
}

template <typename T>
size_t RLEEncoding(const std::vector<std::pair<T, UInt16>> & rle, char * dest)
{
    for (const auto & [value, count] : rle)
    {
        unalignedStore<T>(dest, value);
        dest += sizeof(T);
        unalignedStore<UInt16>(dest, count);
        dest += sizeof(UInt16);
    }
    return rle.size() * (sizeof(T) + sizeof(UInt16));
}

template <typename T>
void RLEDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    if (source_size % (sizeof(T) + sizeof(UInt16)) != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress with bitpacking codec, data size {} is not aligned to {}",
            source_size,
            sizeof(T) + sizeof(UInt16));

    const char * dest_end = dest + dest_size;

    for (size_t i = 0; i < source_size / (sizeof(T) + sizeof(UInt16)); ++i)
    {
        T value = unalignedLoad<T>(src);
        src += sizeof(T);
        auto count = unalignedLoad<UInt16>(src);
        src += sizeof(UInt16);
        if (dest + count * sizeof(T) > dest_end)
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress with bitpacking codec, data is too large");
        for (size_t j = 0; j < count; ++j)
        {
            unalignedStore<T>(dest, value);
            dest += sizeof(T);
        }
    }
}

template <typename T>
size_t ForEncoding(std::vector<T> & values, T min_value, UInt8 width, char * dest)
{
    for (auto & value : values)
    {
        value -= min_value;
    }
    unalignedStore<T>(dest, min_value);
    dest += sizeof(T);
    unalignedStore<UInt8>(dest, width);
    dest += sizeof(UInt8);
    BitpackingPrimitives::packBuffer<T, false>(
        reinterpret_cast<unsigned char *>(dest),
        values.data(),
        values.size(),
        width);
    return BitpackingPrimitives::getRequiredSize(values.size(), width) + sizeof(T) + sizeof(UInt8);
}

template <typename T>
void ForDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    T min_value = unalignedLoad<T>(src);
    src += sizeof(T);
    auto width = unalignedLoad<UInt8>(src);
    src += sizeof(UInt8);

    size_t size = BitpackingPrimitives::getRequiredSize(dest_size / sizeof(T), width);
    if (source_size < sizeof(T) + sizeof(UInt8) + size)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress with bitpacking codec, data size {} is too small",
            source_size);
    BitpackingPrimitives::unPackBuffer<T>(
        reinterpret_cast<unsigned char *>(dest),
        reinterpret_cast<const unsigned char *>(src),
        dest_size / sizeof(T),
        width);

    for (size_t i = 0; i < dest_size / sizeof(T); ++i)
    {
        unalignedStore<T>(dest, unalignedLoad<T>(dest) + min_value);
        dest += sizeof(T);
    }
}

template <typename T, typename TS = typename std::make_signed<T>::type>
size_t DeltaForEncoding(std::vector<T> & values, TS min_delta_value, UInt8 width, char * dest)
{
    std::vector<TS> deltas(values.size() - 1);
    for (size_t i = 0; i < values.size(); ++i)
    {
        deltas[i] = static_cast<TS>(values[i + 1]) - static_cast<TS>(values[i]) - min_delta_value;
    }
    unalignedStore<T>(dest, values[0]);
    dest += sizeof(T);
    unalignedStore<TS>(dest, min_delta_value);
    dest += sizeof(TS);
    unalignedStore<UInt8>(dest, width);
    dest += sizeof(UInt8);
    BitpackingPrimitives::packBuffer<TS, true>(
        reinterpret_cast<unsigned char *>(dest),
        deltas.data(),
        deltas.size(),
        width);
    return BitpackingPrimitives::getRequiredSize(deltas.size(), width) + sizeof(T) + sizeof(TS) + sizeof(UInt8);
}

template <typename T, typename TS = typename std::make_signed<T>::type>
void DeltaForDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    T first_value = unalignedLoad<T>(src);
    src += sizeof(T);
    TS min_delta_value = unalignedLoad<TS>(src);
    src += sizeof(TS);
    auto width = unalignedLoad<UInt8>(src);
    src += sizeof(UInt8);

    size_t size = BitpackingPrimitives::getRequiredSize(dest_size / sizeof(T), width);
    if (source_size < sizeof(T) + sizeof(TS) + sizeof(UInt8) + size)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress with bitpacking codec, data size {} is too small",
            source_size);
    unalignedStore<T>(dest, first_value);
    dest += sizeof(T);
    BitpackingPrimitives::unPackBuffer<TS>(
        reinterpret_cast<unsigned char *>(dest),
        reinterpret_cast<const unsigned char *>(src),
        dest_size / sizeof(T) - 1,
        width);

    TS base = static_cast<TS>(first_value);
    for (size_t i = 1; i < dest_size / sizeof(T); ++i)
    {
        base += (unalignedLoad<TS>(dest) + min_delta_value);
        unalignedStore<T>(dest, base);
        dest += sizeof(T);
    }
}

} // namespace

template <typename T>
size_t CompressionCodecLightweight::compressDataForType(const char * source, UInt32 source_size, char * dest) const
{
    if (source_size % sizeof(T) != 0)
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with bitpacking codec, data size {} is not aligned to {}",
            source_size,
            sizeof(T));

    const size_t count = source_size / sizeof(T);
    std::vector<T> values(count);
    for (size_t i = 0; i < count; ++i)
    {
        values[i] = unalignedLoad<T>(source + i * sizeof(T));
    }

    State<T> state;
    auto mode = analyze(values, state);
    if (mode == Mode::Invalid)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with bitpacking codec, no data");

    unalignedStore<UInt8>(dest, static_cast<UInt8>(mode));
    dest += sizeof(UInt8);
    switch (mode)
    {
    case Mode::CONSTANT:
        return 1 + ConstantEncoding(std::get<0>(state), dest);
    case Mode::CONSTANT_DELTA:
        return 1 + ConstantDeltaEncoding(values[0], std::get<1>(state), dest);
    case Mode::RLE:
        return 1 + RLEEncoding(std::get<2>(state), dest);
    case Mode::FOR:
    {
        FORState for_state = std::get<3>(state);
        return 1 + ForEncoding(values, for_state.min_value, for_state.bit_width, dest);
    }
    case Mode::DELTA_FOR:
    {
        DeltaFORState delta_for_state = std::get<4>(state);
        return 1 + DeltaForEncoding(values, delta_for_state.min_delta_value, delta_for_state.bit_width, dest);
    }
    case Mode::LZ4:
    {
        return LZ4_compress_fast(
            source,
            dest,
            source_size,
            LZ4_COMPRESSBOUND(source_size),
            CompressionSettings::getDefaultLevel(CompressionMethod::LZ4));
    }
    default:
        __builtin_unreachable();
    }
}

template <typename T>
void CompressionCodecLightweight::decompressDataForType(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size) const
{
    auto mode = static_cast<Mode>(unalignedLoad<UInt8>(source));
    source += sizeof(UInt8);
    switch (mode)
    {
    case Mode::CONSTANT:
        ConstantDecoding<T>(source, source_size, dest, output_size);
        break;
    case Mode::CONSTANT_DELTA:
        ConstantDeltaDecoding<T>(source, source_size, dest, output_size);
        break;
    case Mode::RLE:
        RLEDecoding<T>(source, source_size, dest, output_size);
        break;
    case Mode::FOR:
        ForDecoding<T>(source, source_size, dest, output_size);
        break;
    case Mode::DELTA_FOR:
        DeltaForDecoding<T>(source, source_size, dest, output_size);
        break;
    case Mode::LZ4:
        if (unlikely(LZ4_decompress_safe(source, dest, source_size, output_size) < 0))
            throw Exception("Cannot LZ4_decompress_safe", ErrorCodes::CANNOT_DECOMPRESS);
        break;
    default:
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress with bitpacking codec, unknown mode {}",
            static_cast<int>(mode));
    }
}

UInt32 CompressionCodecLightweight::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % bytes_size;
    dest[0] = bytes_size;
    memcpy(&dest[1], source, bytes_to_skip);
    size_t start_pos = 1 + bytes_to_skip;
    switch (bytes_size)
    {
    case 1:
        return 1 + compressDataForType<UInt8>(source + bytes_to_skip, source_size - bytes_to_skip, &dest[start_pos]);
    case 2:
        return 1 + compressDataForType<UInt16>(source + bytes_to_skip, source_size - bytes_to_skip, &dest[start_pos]);
    case 4:
        return 1 + compressDataForType<UInt32>(source + bytes_to_skip, source_size - bytes_to_skip, &dest[start_pos]);
    case 8:
        return 1 + compressDataForType<UInt64>(source + bytes_to_skip, source_size - bytes_to_skip, &dest[start_pos]);
    default:
        __builtin_unreachable();
    }
}

template <typename T>
CompressionCodecLightweight::Mode CompressionCodecLightweight::analyze(std::vector<T> & values, State<T> & state) const
{
    // If mode is not AUTO, return it
    if (mode != Mode::AUTO)
        return mode;
    if (values.empty())
        return Mode::Invalid;

    using TS = typename std::make_signed<T>::type;

    // Check CONSTANT
    std::vector<std::pair<T, UInt16>> rle;
    rle.reserve(values.size());
    rle.emplace_back(values[0], 1);
    T min_value = values[0];
    T max_value = values[0];
    for (size_t i = 1; i < values.size(); ++i)
    {
        if (values[i] != values[i - 1])
            rle.emplace_back(values[i], 1);
        else
            ++rle.back().second;
        min_value = std::min(min_value, values[i]);
        max_value = std::max(max_value, values[i]);
    }
    if (rle.size() == 1)
    {
        state = rle[0].first;
        return Mode::CONSTANT;
    }

    // Check CONSTANT_DELTA
    TS min_delta = values[1] - values[0];
    TS max_delta = values[1] - values[0];
    for (size_t i = 2; i < values.size(); ++i)
    {
        TS delta = static_cast<TS>(values[i]) - static_cast<TS>(values[i - 1]);
        min_delta = std::min(min_delta, delta);
        max_delta = std::max(max_delta, delta);
    }
    if (min_delta == max_delta)
    {
        state = min_delta;
        return Mode::CONSTANT_DELTA;
    }

    UInt8 delta_for_width = BitpackingPrimitives::minimumBitWidth<TS, true>(max_delta - min_delta);
    // additional T for min_delta
    size_t delta_for_size = BitpackingPrimitives::getRequiredSize(values.size(), delta_for_width) + sizeof(T);
    UInt8 for_width = BitpackingPrimitives::minimumBitWidth<T, false>(max_value - min_value);
    // additional T for min_value
    size_t for_size = BitpackingPrimitives::getRequiredSize(values.size(), for_width) + sizeof(T);
    size_t origin_size = values.size() * sizeof(T);
    size_t rle_size = rle.size() * (sizeof(T) + sizeof(UInt16));
    if (rle_size < delta_for_size && rle_size < for_size && rle_size < origin_size)
    {
        state = std::move(rle);
        return Mode::RLE;
    }
    else if (for_size < delta_for_size && for_size < origin_size)
    {
        state = FORState<T>{min_value, for_width};
        return Mode::FOR;
    }
    else if (delta_for_size < origin_size)
    {
        state = DeltaFORState<T>{min_delta, delta_for_width};
        return Mode::DELTA_FOR;
    }
    return Mode::LZ4;
}

void CompressionCodecLightweight::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if unlikely (source_size < 2)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress bitpacking-encoded data. File has wrong header");

    if (uncompressed_size == 0)
        return;

    UInt8 bytes_size = source[0];

    if unlikely (bytes_size != 1 && bytes_size != 2 && bytes_size != 4 && bytes_size != 8)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress bitpacking-encoded data. File has wrong header");

    UInt8 bytes_to_skip = uncompressed_size % bytes_size;
    UInt32 output_size = uncompressed_size - bytes_to_skip;

    if unlikely (static_cast<UInt32>(1 + bytes_to_skip) > source_size)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress bitpacking-encoded data. File has wrong header");

    memcpy(dest, &source[1], bytes_to_skip);
    UInt32 source_size_no_header = source_size - bytes_to_skip - 1;
    switch (bytes_size)
    {
    case 1:
        decompressDataForType<UInt8>(
            &source[1 + bytes_to_skip],
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    case 2:
        decompressDataForType<UInt16>(
            &source[1 + bytes_to_skip],
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    case 4:
        decompressDataForType<UInt32>(
            &source[1 + bytes_to_skip],
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    case 8:
        decompressDataForType<UInt64>(
            &source[1 + bytes_to_skip],
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    default:
        __builtin_unreachable();
    }
}

} // namespace DB
