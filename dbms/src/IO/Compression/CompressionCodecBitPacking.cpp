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
#include <IO/Compression/CompressionCodecBitPacking.h>
#include <IO/Compression/CompressionInfo.h>
#include <common/likely.h>
#include <common/unaligned.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

CompressionCodecBitPacking::CompressionCodecBitPacking(UInt8 bytes_size_)
    : bytes_size(bytes_size_)
{}

UInt8 CompressionCodecBitPacking::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::BitPacking);
}

UInt32 CompressionCodecBitPacking::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // 1 byte for bytes_size, 1 byte for width, and the rest for compressed data
    return uncompressed_size + 1 + 1;
}

namespace
{

template <typename T>
void compressDataForType(const char * source, UInt32 source_size, char * dest)
{
    if (source_size % sizeof(T) != 0)
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with bitpacking codec, data size {} is not aligned to {}",
            source_size,
            sizeof(T));


    UInt8 width = BitpackingPrimitives::minimumBitWidth<T, false>(reinterpret_cast<const T *>(source), source_size);
    unalignedStore<UInt8>(dest, width);
    dest += sizeof(UInt8);
    size_t required_size = BitpackingPrimitives::getRequiredSize(static_cast<size_t>(source_size / sizeof(T)), width);
    if unlikely (required_size > source_size)
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with bitpacking codec, required size {} is greater than source size {}",
            required_size,
            source_size);

    BitpackingPrimitives::packBuffer<T, false>(
        reinterpret_cast<unsigned char *>(dest),
        reinterpret_cast<const T *>(source),
        source_size / sizeof(T),
        width);
}

template <typename T>
void decompressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    if ((source_size - 1) % sizeof(T) != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress bitpacking-encoded data, data size {} is not aligned to {}",
            source_size - 1,
            sizeof(T));

    auto width = unalignedLoad<UInt8>(source);
    BitpackingPrimitives::unPackBuffer<T>(
        reinterpret_cast<unsigned char *>(dest),
        reinterpret_cast<const unsigned char *>(source),
        output_size / sizeof(T),
        width);
}

} // namespace

UInt32 CompressionCodecBitPacking::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % bytes_size;
    dest[0] = bytes_size;
    memcpy(&dest[1], source, bytes_to_skip);
    size_t start_pos = 1 + bytes_to_skip;
    switch (bytes_size)
    {
    case 1:
        compressDataForType<UInt8>(source + bytes_to_skip, source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 2:
        compressDataForType<UInt16>(source + bytes_to_skip, source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 4:
        compressDataForType<UInt32>(source + bytes_to_skip, source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 8:
        compressDataForType<UInt64>(source + bytes_to_skip, source_size - bytes_to_skip, &dest[start_pos]);
        break;
    default:
        __builtin_unreachable();
    }
    return 1 + source_size;
}

void CompressionCodecBitPacking::doDecompressData(
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
