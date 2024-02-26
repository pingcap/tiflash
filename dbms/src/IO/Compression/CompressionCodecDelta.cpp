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
#include <IO/Compression/CompressionCodecDelta.h>
#include <IO/Compression/CompressionInfo.h>
#include <common/unaligned.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
extern const int ILLEGAL_CODEC_PARAMETER;
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

CompressionCodecDelta::CompressionCodecDelta(UInt8 delta_bytes_size_)
    : delta_bytes_size(delta_bytes_size_)
{}

uint8_t CompressionCodecDelta::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Delta);
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

    T prev_src = 0;
    const char * const source_end = source + source_size;
    while (source < source_end)
    {
        T curr_src = unalignedLoad<T>(source);
        unalignedStore<T>(dest, curr_src - prev_src);
        prev_src = curr_src;

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

    T accumulator{};
    const char * const source_end = source + source_size;
    while (source < source_end)
    {
        accumulator += unalignedLoad<T>(source);
        if (dest + sizeof(accumulator) > output_end) [[unlikely]]
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data");
        unalignedStore<T>(dest, accumulator);

        source += sizeof(T);
        dest += sizeof(T);
    }
}

} // namespace

UInt32 CompressionCodecDelta::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % delta_bytes_size;
    dest[0] = delta_bytes_size;
    dest[1] = bytes_to_skip; /// unused (backward compatibility)
    memcpy(&dest[2], source, bytes_to_skip);
    size_t start_pos = 2 + bytes_to_skip;
    switch (delta_bytes_size) // NOLINT(bugprone-switch-missing-default-case)
    {
    case 1:
        compressDataForType<UInt8>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 2:
        compressDataForType<UInt16>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 4:
        compressDataForType<UInt32>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 8:
        compressDataForType<UInt64>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    }
    return 1 + 1 + source_size;
}

void CompressionCodecDelta::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data. File has wrong header");

    if (uncompressed_size == 0)
        return;

    UInt8 bytes_size = source[0];

    if (bytes_size != 1 && bytes_size != 2 && bytes_size != 4 && bytes_size != 8)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data. File has wrong header");

    UInt8 bytes_to_skip = uncompressed_size % bytes_size;
    UInt32 output_size = uncompressed_size - bytes_to_skip;

    if (static_cast<UInt32>(2 + bytes_to_skip) > source_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data. File has wrong header");

    memcpy(dest, &source[2], bytes_to_skip);
    UInt32 source_size_no_header = source_size - bytes_to_skip - 2;
    switch (bytes_size) // NOLINT(bugprone-switch-missing-default-case)
    {
    case 1:
        decompressDataForType<UInt8>(
            &source[2 + bytes_to_skip],
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    case 2:
        decompressDataForType<UInt16>(
            &source[2 + bytes_to_skip],
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    case 4:
        decompressDataForType<UInt32>(
            &source[2 + bytes_to_skip],
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    case 8:
        decompressDataForType<UInt64>(
            &source[2 + bytes_to_skip],
            source_size_no_header,
            &dest[bytes_to_skip],
            output_size);
        break;
    }
}

CompressionCodecPtr getCompressionCodecDelta(UInt8 delta_bytes_size)
{
    return std::make_shared<CompressionCodecDelta>(delta_bytes_size);
}

} // namespace DB
