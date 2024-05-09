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
#include <IO/Compression/CompressionCodecRunLength.h>
#include <IO/Compression/CompressionInfo.h>
#include <IO/Compression/EncodingUtil.h>
#include <IO/Compression/ICompressionCodec.h>
#include <common/unaligned.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

CompressionCodecRunLength::CompressionCodecRunLength(UInt8 bytes_size_)
    : bytes_size(bytes_size_)
{}

UInt8 CompressionCodecRunLength::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::RunLength);
}

UInt32 CompressionCodecRunLength::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // If the encoded data is larger than the original data, we will store the original data
    // Additional byte is used to store the size of the data type
    return 1 + uncompressed_size;
}

namespace
{
constexpr UInt8 JUST_COPY_CODE = 0xFF;

template <typename T>
UInt32 compressDataForType(const char * source, UInt32 source_size, char * dest)
{
    const char * source_end = source + source_size;
    DB::Compression::RunLengthPairs<T> rle_vec;
    rle_vec.reserve(source_size / sizeof(T));
    for (const auto * src = source; src < source_end; src += sizeof(T))
    {
        T value = unalignedLoad<T>(src);
        if (rle_vec.empty() || rle_vec.back().first != value
            || rle_vec.back().second == std::numeric_limits<UInt8>::max())
            rle_vec.emplace_back(value, 1);
        else
            ++rle_vec.back().second;
    }

    if (DB::Compression::RunLengthPairsSize<T>(rle_vec) > source_size)
    {
        dest[0] = JUST_COPY_CODE;
        memcpy(&dest[1], source, source_size);
        return 1 + source_size;
    }

    dest[0] = sizeof(T);
    dest += 1;
    return 1 + DB::Compression::RunLengthEncoding<T>(rle_vec, dest);
}

} // namespace

UInt32 CompressionCodecRunLength::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if unlikely (source_size % bytes_size != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "source size {} is not aligned to {}", source_size, bytes_size);
    switch (bytes_size)
    {
    case 1:
        return compressDataForType<UInt8>(source, source_size, dest);
    case 2:
        return compressDataForType<UInt16>(source, source_size, dest);
    case 4:
        return compressDataForType<UInt32>(source, source_size, dest);
    case 8:
        return compressDataForType<UInt64>(source, source_size, dest);
    default:
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress RunLength-encoded data. Unsupported bytes size");
    }
}

void CompressionCodecRunLength::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if (source_size < 1)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress RunLength-encoded data. File has wrong header");

    if (uncompressed_size == 0)
        return;

    UInt8 bytes_size = source[0];
    if (bytes_size == JUST_COPY_CODE)
    {
        if (source_size - 1 < uncompressed_size)
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress RunLength-encoded data. File has wrong header");

        memcpy(dest, &source[1], uncompressed_size);
        return;
    }

    if unlikely (uncompressed_size % bytes_size != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "uncompressed size {} is not aligned to {}",
            uncompressed_size,
            bytes_size);

    switch (bytes_size)
    {
    case 1:
        DB::Compression::RunLengthDecoding<UInt8>(&source[1], source_size - 1, dest, uncompressed_size);
        break;
    case 2:
        DB::Compression::RunLengthDecoding<UInt16>(&source[1], source_size - 1, dest, uncompressed_size);
        break;
    case 4:
        DB::Compression::RunLengthDecoding<UInt32>(&source[1], source_size - 1, dest, uncompressed_size);
        break;
    case 8:
        DB::Compression::RunLengthDecoding<UInt64>(&source[1], source_size - 1, dest, uncompressed_size);
        break;
    default:
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress RunLength-encoded data. Unsupported bytes size");
    }
}

} // namespace DB
