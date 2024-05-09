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
#include <IO/Compression/CompressionCodecDeltaFOR.h>
#include <IO/Compression/CompressionCodecFOR.h>
#include <IO/Compression/CompressionInfo.h>
#include <IO/Compression/EncodingUtil.h>
#include <common/likely.h>
#include <common/unaligned.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

CompressionCodecDeltaFOR::CompressionCodecDeltaFOR(UInt8 bytes_size_)
    : bytes_size(bytes_size_)
{}

UInt8 CompressionCodecDeltaFOR::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::DeltaFOR);
}

UInt32 CompressionCodecDeltaFOR::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    /**
     *|bytes_of_original_type|frame_of_reference|width(bits)  |bitpacked data|
     *|1 bytes               |bytes_size        |sizeof(UInt8)|required size |
     */
    const size_t count = uncompressed_size / bytes_size;
    return 1 + bytes_size + sizeof(UInt8) + BitpackingPrimitives::getRequiredSize(count, bytes_size * 8);
}

namespace
{

template <std::integral T>
UInt32 compressData(const char * source, UInt32 source_size, char * dest)
{
    const auto count = source_size / sizeof(T);
    DB::Compression::DeltaEncoding<T>(reinterpret_cast<const T *>(source), count, reinterpret_cast<T *>(dest));
    // Cast deltas to signed type to better compress negative values.
    // For example, if we have a sequence of UInt8 values [3, 2, 1, 0], the deltas will be [3, -1, -1, -1]
    // If we compress them as UInt8, we will get [3, 255, 255, 255], which is not optimal.
    using TS = typename std::make_signed<T>::type;
    return DB::CompressionCodecFOR::compressData<TS>(reinterpret_cast<TS *>(dest), count, dest);
}

} // namespace

UInt32 CompressionCodecDeltaFOR::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if unlikely (source_size % bytes_size != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "source size {} is not aligned to {}", source_size, bytes_size);
    dest[0] = bytes_size;
    size_t start_pos = 1;
    switch (bytes_size)
    {
    case 1:
        return 1 + compressData<UInt8>(source, source_size, &dest[start_pos]);
    case 2:
        return 1 + compressData<UInt16>(source, source_size, &dest[start_pos]);
    case 4:
        return 1 + compressData<UInt32>(source, source_size, &dest[start_pos]);
    case 8:
        return 1 + compressData<UInt64>(source, source_size, &dest[start_pos]);
    default:
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress DeltaFor-encoded data. Unsupported bytes size");
    }
}

void CompressionCodecDeltaFOR::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if unlikely (source_size < 2)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress DeltaFor-encoded data. File has wrong header");

    if (uncompressed_size == 0)
        return;

    UInt8 bytes_size = source[0];
    if unlikely (uncompressed_size % bytes_size != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "uncompressed size {} is not aligned to {}",
            uncompressed_size,
            bytes_size);

    UInt32 source_size_no_header = source_size - 1;
    switch (bytes_size)
    {
    case 1:
        DB::Compression::DeltaFORDecoding<UInt8>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case 2:
        DB::Compression::DeltaFORDecoding<UInt16>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case 4:
        DB::Compression::DeltaFORDecoding<UInt32>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case 8:
        DB::Compression::DeltaFORDecoding<UInt64>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    default:
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress DeltaFor-encoded data. Unsupported bytes size");
    }
}

void CompressionCodecDeltaFOR::ordinaryDecompress(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 dest_size)
{
    if unlikely (source_size < 2)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress DeltaFor-encoded data. File has wrong header");

    if (dest_size == 0)
        return;

    UInt8 bytes_size = source[0];
    if unlikely (dest_size % bytes_size != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "uncompressed size {} is not aligned to {}",
            dest_size,
            bytes_size);

    UInt32 source_size_no_header = source_size - 1;
    switch (bytes_size)
    {
    case 1:
        DB::Compression::OrdinaryDeltaFORDecoding<UInt8>(&source[1], source_size_no_header, dest, dest_size);
        break;
    case 2:
        DB::Compression::OrdinaryDeltaFORDecoding<UInt16>(&source[1], source_size_no_header, dest, dest_size);
        break;
    case 4:
        DB::Compression::OrdinaryDeltaFORDecoding<UInt32>(&source[1], source_size_no_header, dest, dest_size);
        break;
    case 8:
        DB::Compression::OrdinaryDeltaFORDecoding<UInt64>(&source[1], source_size_no_header, dest, dest_size);
        break;
    default:
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress DeltaFor-encoded data. Unsupported bytes size");
    }
}

} // namespace DB
