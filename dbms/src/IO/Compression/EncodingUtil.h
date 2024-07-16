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

#include <Common/BitpackingPrimitives.h>
#include <Common/Exception.h>
#include <common/types.h>
#include <common/unaligned.h>


namespace DB::ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace DB::ErrorCodes

namespace DB::Compression
{

/// Constant encoding

template <std::integral T>
size_t constantEncoding(T constant, char * dest)
{
    unalignedStore<T>(dest, constant);
    return sizeof(T);
}

template <std::integral T>
void constantDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    if (unlikely(source_size < sizeof(T)))
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot use Constant decoding, data size {} is too small",
            source_size);

    T constant = unalignedLoad<T>(src);
    for (size_t i = 0; i < dest_size / sizeof(T); ++i)
    {
        unalignedStore<T>(dest, constant);
        dest += sizeof(T);
    }
}

/// Constant delta encoding

template <std::integral T>
size_t constantDeltaEncoding(T first_value, T constant_delta, char * dest)
{
    unalignedStore<T>(dest, first_value);
    dest += sizeof(T);
    unalignedStore<T>(dest, constant_delta);
    return sizeof(T) + sizeof(T);
}

template <std::integral T>
void constantDeltaDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    if (unlikely(source_size < sizeof(T) + sizeof(T)))
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot use ConstantDelta decoding, data size {} is too small",
            source_size);

    T first_value = unalignedLoad<T>(src);
    T constant_delta = unalignedLoad<T>(src + sizeof(T));
    for (size_t i = 0; i < dest_size / sizeof(T); ++i)
    {
        unalignedStore<T>(dest, first_value);
        first_value += constant_delta;
        dest += sizeof(T);
    }
}

/// Run-length encoding

// <value, num_of_value>
template <std::integral T>
using RunLengthPair = std::pair<T, UInt8>;
template <std::integral T>
using RunLengthPairs = std::vector<RunLengthPair<T>>;
template <std::integral T>
static constexpr size_t RunLengthPairLength = sizeof(T) + sizeof(UInt8);

template <std::integral T>
size_t runLengthPairsByteSize(const RunLengthPairs<T> & rle)
{
    return rle.size() * RunLengthPairLength<T>;
}

template <std::integral T>
size_t runLengthEncoding(const RunLengthPairs<T> & rle, char * dest)
{
    for (const auto & [value, count] : rle)
    {
        unalignedStore<T>(dest, value);
        dest += sizeof(T);
        unalignedStore<UInt8>(dest, count);
        dest += sizeof(UInt8);
    }
    return rle.size() * RunLengthPairLength<T>;
}

template <std::integral T>
void runLengthDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    if (unlikely(source_size % RunLengthPairLength<T> != 0))
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot use RunLength decoding, data size {} is not aligned to {}",
            source_size,
            RunLengthPairLength<T>);

    const char * dest_end = dest + dest_size;
    for (UInt32 i = 0; i < source_size / RunLengthPairLength<T>; ++i)
    {
        T value = unalignedLoad<T>(src);
        src += sizeof(T);
        auto count = unalignedLoad<UInt8>(src);
        src += sizeof(UInt8);
        assert(dest + count * sizeof(T) <= dest_end);
        if constexpr (std::is_same_v<T, UInt8> || std::is_same_v<T, Int8>)
        {
            memset(dest, value, count);
            dest += count * sizeof(T);
        }
        else
        {
            for (UInt32 j = 0; j < count; ++j)
            {
                unalignedStore<T>(dest, value);
                dest += sizeof(T);
            }
        }
    }
}

/// Frame of Reference encoding

template <std::integral T>
void subtractFrameOfReference(T * dst, T frame_of_reference, UInt32 count);

template <std::integral T>
UInt8 FOREncodingWidth(std::vector<T> & values, T frame_of_reference);

template <std::integral T, bool skip_subtract_frame_of_reference = false>
size_t FOREncoding(std::vector<T> & values, T frame_of_reference, UInt8 width, char * dest)
{
    assert(!values.empty()); // caller must ensure input is not empty

    if constexpr (!skip_subtract_frame_of_reference)
        subtractFrameOfReference(values.data(), frame_of_reference, values.size());
    // store frame of reference
    unalignedStore<T>(dest, frame_of_reference);
    dest += sizeof(T);
    // store width
    unalignedStore<UInt8>(dest, width);
    dest += sizeof(UInt8);
    // if width == 0, skip bitpacking
    if (width == 0)
        return sizeof(T) + sizeof(UInt8);
    auto required_size = BitpackingPrimitives::getRequiredSize(values.size(), width);
    // after applying frame of reference, all values are bigger than 0.
    BitpackingPrimitives::packBuffer(reinterpret_cast<unsigned char *>(dest), values.data(), values.size(), width);
    return sizeof(T) + sizeof(UInt8) + required_size;
}

template <std::integral T>
void applyFrameOfReference(T * dst, T frame_of_reference, UInt32 count);

template <std::integral T>
void FORDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    static constexpr UInt8 BYTES_SIZE = sizeof(T);
    if unlikely (dest_size % BYTES_SIZE != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "uncompressed size {} is not aligned to {}",
            dest_size,
            BYTES_SIZE);

    const auto count = dest_size / BYTES_SIZE;
    T frame_of_reference = unalignedLoad<T>(src);
    src += BYTES_SIZE;
    auto width = unalignedLoad<UInt8>(src);
    src += sizeof(UInt8);
    const auto required_size = source_size - BYTES_SIZE - sizeof(UInt8);
    RUNTIME_CHECK(BitpackingPrimitives::getRequiredSize(count, width) == required_size);
    auto round_size = BitpackingPrimitives::roundUpToAlgorithmGroupSize(count);
    if (round_size != count)
    {
        // Reserve enough space for the temporary buffer.
        unsigned char tmp_buffer[round_size * BYTES_SIZE];
        BitpackingPrimitives::unPackBuffer<T>(tmp_buffer, reinterpret_cast<const unsigned char *>(src), count, width);
        applyFrameOfReference(reinterpret_cast<T *>(tmp_buffer), frame_of_reference, count);
        memcpy(dest, tmp_buffer, dest_size);
        return;
    }
    BitpackingPrimitives::unPackBuffer<T>(
        reinterpret_cast<unsigned char *>(dest),
        reinterpret_cast<const unsigned char *>(src),
        count,
        width);
    applyFrameOfReference(reinterpret_cast<T *>(dest), frame_of_reference, count);
}

/// Delta encoding

template <std::integral T>
void deltaEncoding(const T * source, UInt32 count, T * dest)
{
    T prev = 0;
    for (UInt32 i = 0; i < count; ++i)
    {
        T curr = source[i];
        dest[i] = curr - prev;
        prev = curr;
    }
}

template <std::integral T>
void ordinaryDeltaDecoding(const char * source, UInt32 source_size, char * dest)
{
    T accumulator{};
    const char * const source_end = source + source_size;
    while (source < source_end)
    {
        accumulator += unalignedLoad<T>(source);
        unalignedStore<T>(dest, accumulator);

        source += sizeof(T);
        dest += sizeof(T);
    }
}

template <std::integral T>
void deltaDecoding(const char * source, UInt32 source_size, char * dest);

/// Delta + Frame of Reference encoding

template <std::integral T>
void ordinaryDeltaFORDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size)
{
    // caller should ensure these size
    assert(source_size >= sizeof(T));
    assert(dest_size >= sizeof(T));

    using TS = typename std::make_signed_t<T>;
    // copy first value to dest
    memcpy(dest, src, sizeof(T));
    if (unlikely(source_size == sizeof(T)))
        return;
    // decode deltas
    FORDecoding<TS>(src + sizeof(T), source_size - sizeof(T), dest + sizeof(T), dest_size - sizeof(T));
    ordinaryDeltaDecoding<T>(dest, dest_size, dest);
}

template <std::integral T>
void deltaFORDecoding(const char * src, UInt32 source_size, char * dest, UInt32 dest_size);

} // namespace DB::Compression
