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
#include <DataTypes/IDataType.h>
#include <IO/Compression/CompressionCodecZigZag.h>
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

CompressionCodecZigZag::CompressionCodecZigZag(UInt8 delta_bytes_size_)
    : delta_bytes_size(delta_bytes_size_)
{}

UInt8 CompressionCodecZigZag::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::Delta);
}

namespace
{

template <typename T>
void compressDataForType(const char * source, UInt32 source_size, char * dest)
{
    if (source_size % sizeof(T) != 0)
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with Delta codec, data size {} is not aligned to {}",
            source_size,
            sizeof(T));

    const char * const source_end = source + source_size;
    while (source < source_end)
    {
        T curr_src = unalignedLoad<T>(source);
        T v = (curr_src >> (sizeof(T) - 1)) ^ (curr_src << 1);
        unalignedStore<T>(dest, v);

        source += sizeof(T);
        dest += sizeof(T);
    }
}

template <typename T>
void decompressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    const char * const output_end = dest + output_size;

    if (source_size % sizeof(T) != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress Delta-encoded data, data size {} is not aligned to {}",
            source_size,
            sizeof(T));

    const char * const source_end = source + source_size;
    while (source < source_end)
    {
        T curr_src = unalignedLoad<T>(source);
        T v = (curr_src >> 1) ^ (-(curr_src & 1));
        if unlikely (dest + sizeof(v) > output_end)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data");
        unalignedStore<T>(dest, v);

        source += sizeof(T);
        dest += sizeof(T);
    }
}

} // namespace

UInt32 CompressionCodecZigZag::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % delta_bytes_size;
    dest[0] = delta_bytes_size;
    memcpy(&dest[1], source, bytes_to_skip);
    size_t start_pos = 1 + bytes_to_skip;
    switch (delta_bytes_size) // NOLINT(bugprone-switch-missing-default-case)
    {
    case 1:
        compressDataForType<UInt8>(
            reinterpret_cast<const char *>(source) + bytes_to_skip,
            source_size - bytes_to_skip,
            &dest[start_pos]);
        break;
    case 2:
        compressDataForType<UInt16>(
            reinterpret_cast<const char *>(source) + bytes_to_skip,
            source_size - bytes_to_skip,
            &dest[start_pos]);
        break;
    case 4:
        compressDataForType<UInt32>(
            reinterpret_cast<const char *>(source) + bytes_to_skip,
            source_size - bytes_to_skip,
            &dest[start_pos]);
        break;
    case 8:
        compressDataForType<UInt64>(
            reinterpret_cast<const char *>(source) + bytes_to_skip,
            source_size - bytes_to_skip,
            &dest[start_pos]);
        break;
    }
    return 1 + source_size;
}

void CompressionCodecZigZag::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if unlikely (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data. File has wrong header");

    if (uncompressed_size == 0)
        return;

    UInt8 bytes_size = source[0];

    if unlikely (bytes_size != 1 && bytes_size != 2 && bytes_size != 4 && bytes_size != 8)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data. File has wrong header");

    UInt8 bytes_to_skip = uncompressed_size % bytes_size;
    UInt32 output_size = uncompressed_size - bytes_to_skip;

    if unlikely (static_cast<UInt32>(1 + bytes_to_skip) > source_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data. File has wrong header");

    memcpy(dest, &source[1], bytes_to_skip);
    UInt32 source_size_no_header = source_size - bytes_to_skip - 1;
    switch (bytes_size) // NOLINT(bugprone-switch-missing-default-case)
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
    }
}

} // namespace DB
