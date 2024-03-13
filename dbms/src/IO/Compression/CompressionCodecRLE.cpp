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
#include <trle.h>

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

uint8_t CompressionCodecRLE::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::RLE);
}

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
            + srlec8(
                   reinterpret_cast<const unsigned char *>(&source[bytes_to_skip]),
                   source_size - bytes_to_skip,
                   reinterpret_cast<unsigned char *>(&dest[start_pos]),
                   0);
    case 2:
        return 1 + bytes_to_skip
            + srlec16(
                   reinterpret_cast<const unsigned char *>(&source[bytes_to_skip]),
                   source_size - bytes_to_skip,
                   reinterpret_cast<unsigned char *>(&dest[start_pos]),
                   0);
    case 4:
        return 1 + bytes_to_skip
            + srlec32(
                   reinterpret_cast<const unsigned char *>(&source[bytes_to_skip]),
                   source_size - bytes_to_skip,
                   reinterpret_cast<unsigned char *>(&dest[start_pos]),
                   0);
    case 8:
        return 1 + bytes_to_skip
            + srlec64(
                   reinterpret_cast<const unsigned char *>(&source[bytes_to_skip]),
                   source_size - bytes_to_skip,
                   reinterpret_cast<unsigned char *>(&dest[start_pos]),
                   0);
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
        srled8(
            reinterpret_cast<const unsigned char *>(&source[1 + bytes_to_skip]),
            source_size_no_header,
            reinterpret_cast<unsigned char *>(&dest[bytes_to_skip]),
            output_size,
            0);
        break;
    case 2:
        srled16(
            reinterpret_cast<const unsigned char *>(&source[1 + bytes_to_skip]),
            source_size_no_header,
            reinterpret_cast<unsigned char *>(&dest[bytes_to_skip]),
            output_size,
            0);
        break;
    case 4:
        srled32(
            reinterpret_cast<const unsigned char *>(&source[1 + bytes_to_skip]),
            source_size_no_header,
            reinterpret_cast<unsigned char *>(&dest[bytes_to_skip]),
            output_size,
            0);
        break;
    case 8:
        srled64(
            reinterpret_cast<const unsigned char *>(&source[1 + bytes_to_skip]),
            source_size_no_header,
            reinterpret_cast<unsigned char *>(&dest[bytes_to_skip]),
            output_size,
            0);
        break;
    default:
        __builtin_unreachable();
    }
}

} // namespace DB
