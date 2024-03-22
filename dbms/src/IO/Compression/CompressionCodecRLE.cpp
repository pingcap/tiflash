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

#include <Common/Exception.h>
#include <IO/Compression/CompressionCodecRLE.h>
#include <IO/Compression/CompressionInfo.h>
#include <IO/Compression/ICompressionCodec.h>
#include <common/unaligned.h>

#include "common/types.h"


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

CompressionCodecRLE::CompressionCodecRLE(UInt8 delta_bytes_size_)
    : delta_bytes_size(delta_bytes_size_)
{}

UInt8 CompressionCodecRLE::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::RLE);
}

UInt32 CompressionCodecRLE::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // 1 byte for delta_bytes_size, then (x, n) pairs, x is delta_bytes_size bytes, n is 2 byte.
    return 1 + uncompressed_size + (uncompressed_size / delta_bytes_size) * 2;
}

namespace
{
// TODO: better implementation
template <typename T>
UInt32 compressDataForType(const char * source, UInt32 source_size, char * dest)
{
    if (source_size % sizeof(T) != 0)
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with Delta codec, data size {} is not aligned to {}",
            source_size,
            sizeof(T));
    const char * source_end = source + source_size;
    UInt32 dest_size = 0;
    UInt16 count = 1;
    T prev = unalignedLoad<T>(source);
    source += sizeof(T);
    while (source < source_end)
    {
        T cur = unalignedLoad<T>(source);
        if (prev == cur)
        {
            ++count;
        }
        else
        {
            unalignedStore<T>(dest, prev);
            dest += sizeof(T);
            dest_size += sizeof(T);
            unalignedStore<UInt16>(dest, count);
            dest += sizeof(UInt16);
            dest_size += sizeof(UInt16);
            prev = cur;
            count = 1;
        }
        source += sizeof(T);
    }
    unalignedStore<T>(dest, prev);
    dest += sizeof(T);
    dest_size += sizeof(T);
    unalignedStore<UInt16>(dest, count);
    dest += sizeof(UInt16);
    dest_size += sizeof(UInt16);
    return dest_size;
}

template <typename T>
void decompressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    const char * output_end = dest + output_size;


    const char * source_end = source + source_size;
    while (source < source_end)
    {
        T data = unalignedLoad<T>(source);
        source += sizeof(T);
        auto count = unalignedLoad<UInt16>(source);
        source += sizeof(UInt16);
        for (UInt16 i = 0; i < count; ++i)
        {
            if unlikely (dest + sizeof(T) > output_end)
                throw Exception(
                    ErrorCodes::CANNOT_DECOMPRESS,
                    "Cannot decompress RLE-encoded data, output buffer is too small");
            unalignedStore<T>(dest, data);
            dest += sizeof(T);
        }
    }
}

} // namespace

UInt32 CompressionCodecRLE::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % delta_bytes_size;
    if unlikely ((source_size - bytes_to_skip) % delta_bytes_size != 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress RLE-encoded data. File has wrong size");

    dest[0] = delta_bytes_size;
    memcpy(&dest[1], source, bytes_to_skip);
    size_t start_pos = 1 + bytes_to_skip;
    switch (delta_bytes_size)
    {
    case 1:
        return 1 + bytes_to_skip
            + compressDataForType<UInt8>(
                   reinterpret_cast<const char *>(&source[bytes_to_skip]),
                   source_size - bytes_to_skip,
                   &dest[start_pos]);
    case 2:
        return 1 + bytes_to_skip
            + compressDataForType<UInt16>(
                   reinterpret_cast<const char *>(&source[bytes_to_skip]),
                   source_size - bytes_to_skip,
                   &dest[start_pos]);
    case 4:
        return 1 + bytes_to_skip
            + compressDataForType<UInt32>(
                   reinterpret_cast<const char *>(&source[bytes_to_skip]),
                   source_size - bytes_to_skip,
                   &dest[start_pos]);
    case 8:
        return 1 + bytes_to_skip
            + compressDataForType<UInt64>(
                   reinterpret_cast<const char *>(&source[bytes_to_skip]),
                   source_size - bytes_to_skip,
                   &dest[start_pos]);
    default:
        __builtin_unreachable();
    }
}

void CompressionCodecRLE::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if (source_size < 1)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress RLE-encoded data. File has wrong header");

    if (uncompressed_size == 0)
        return;

    UInt8 bytes_size = source[0];

    if (bytes_size != 1 && bytes_size != 2 && bytes_size != 4 && bytes_size != 8)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress RLE-encoded data. File has wrong header");

    UInt8 bytes_to_skip = uncompressed_size % bytes_size;
    UInt32 output_size = uncompressed_size - bytes_to_skip;

    if (static_cast<UInt32>(1 + bytes_to_skip) > source_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress RLE-encoded data. File has wrong header");

    UInt32 source_size_no_header = source_size - bytes_to_skip - 1;
    switch (bytes_size)
    {
    case 1:
        decompressDataForType<UInt8>(
            reinterpret_cast<const char *>(&source[1 + bytes_to_skip]),
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    case 2:
        decompressDataForType<UInt16>(
            reinterpret_cast<const char *>(&source[1 + bytes_to_skip]),
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    case 4:
        decompressDataForType<UInt32>(
            reinterpret_cast<const char *>(&source[1 + bytes_to_skip]),
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    case 8:
        decompressDataForType<UInt64>(
            reinterpret_cast<const char *>(&source[1 + bytes_to_skip]),
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    default:
        __builtin_unreachable();
    }
}

} // namespace DB
