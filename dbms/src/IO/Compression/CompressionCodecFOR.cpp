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

CompressionCodecFOR::CompressionCodecFOR(UInt8 bytes_size_)
    : bytes_size(bytes_size_)
{}

UInt8 CompressionCodecFOR::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::FOR);
}

UInt32 CompressionCodecFOR::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    /**
     *|bytes_of_original_type|frame_of_reference|width(bits)  |bitpacked data|
     *|1 bytes               |bytes_size        |sizeof(UInt8)|required size |
     */
    const size_t count = uncompressed_size / bytes_size;
    return 1 + bytes_size + sizeof(UInt8) + BitpackingPrimitives::getRequiredSize(count, bytes_size * 8);
}

template <std::integral T>
UInt32 CompressionCodecFOR::compressData(const T * source, UInt32 count, char * dest)
{
    assert(count > 0); // doCompressData ensure it
    std::vector<T> values(count);
    values.assign(source, source + count);
    T frame_of_reference = *std::min_element(values.cbegin(), values.cend());
    UInt8 width = DB::Compression::ForEncodingWidth(values, frame_of_reference);
    return DB::Compression::ForEncoding<T, std::is_signed_v<T>>(values, frame_of_reference, width, dest);
}

UInt32 CompressionCodecFOR::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if unlikely (source_size % bytes_size != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "source size {} is not aligned to {}", source_size, bytes_size);
    dest[0] = bytes_size;
    auto count = source_size / bytes_size;
    switch (bytes_size)
    {
    case 1:
        return 1 + compressData<UInt8>(reinterpret_cast<const UInt8 *>(source), count, &dest[1]);
    case 2:
        return 1 + compressData<UInt16>(reinterpret_cast<const UInt16 *>(source), count, &dest[1]);
    case 4:
        return 1 + compressData<UInt32>(reinterpret_cast<const UInt32 *>(source), count, &dest[1]);
    case 8:
        return 1 + compressData<UInt64>(reinterpret_cast<const UInt64 *>(source), count, &dest[1]);
    default:
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress For-encoded data. Unsupported bytes size: {}",
            bytes_size);
    }
}

void CompressionCodecFOR::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if unlikely (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress For-encoded data. File has wrong header");

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
        DB::Compression::ForDecoding<UInt8>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case 2:
        DB::Compression::ForDecoding<UInt16>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case 4:
        DB::Compression::ForDecoding<UInt32>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case 8:
        DB::Compression::ForDecoding<UInt64>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    default:
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress For-encoded data. Unsupported bytes size: {}",
            bytes_size);
    }
}


// The following instantiations are used in CompressionCodecDeltaFor.cpp

template UInt32 CompressionCodecFOR::compressData<Int8>(const Int8 * source, UInt32 count, char * dest);
template UInt32 CompressionCodecFOR::compressData<Int16>(const Int16 * source, UInt32 count, char * dest);
template UInt32 CompressionCodecFOR::compressData<Int32>(const Int32 * source, UInt32 count, char * dest);
template UInt32 CompressionCodecFOR::compressData<Int64>(const Int64 * source, UInt32 count, char * dest);

} // namespace DB
