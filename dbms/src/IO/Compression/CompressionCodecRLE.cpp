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


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

CompressionCodecRLE::CompressionCodecRLE(UInt8 bytes_size_)
    : bytes_size(bytes_size_)
{}

UInt8 CompressionCodecRLE::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::RLE);
}

UInt32 CompressionCodecRLE::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // If the encoded data is larger than the original data, we will store the original data
    // Additional byte is used to store the size of the data type
    return 1 + uncompressed_size;
}

namespace
{
constexpr UInt8 JUST_COPY_CODE = 0xFF;

// TODO: better implementation
template <typename T>
UInt32 compressDataForType(const char * source, UInt32 source_size, char * dest)
{
    const char * source_end = source + source_size;
    std::vector<std::pair<T, UInt16>> rle_vec;
    rle_vec.reserve(source_size / sizeof(T));
    static constexpr size_t RLE_PAIR_LENGTH = sizeof(T) + sizeof(UInt16);
    for (const auto * src = source; src < source_end; src += sizeof(T))
    {
        T value = unalignedLoad<T>(src);
        if (rle_vec.empty() || rle_vec.back().first != value)
            rle_vec.emplace_back(value, 1);
        else
            ++rle_vec.back().second;
    }

    if (rle_vec.size() * RLE_PAIR_LENGTH > source_size)
    {
        dest[0] = JUST_COPY_CODE;
        memcpy(&dest[1], source, source_size);
        return 1 + source_size;
    }

    dest[0] = sizeof(T);
    dest += 1;
    for (const auto & [value, count] : rle_vec)
    {
        unalignedStore<T>(dest, value);
        dest += sizeof(T);
        unalignedStore<UInt16>(dest, count);
        dest += sizeof(UInt16);
    }
    return 1 + rle_vec.size() * RLE_PAIR_LENGTH;
}

template <typename T>
void decompressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    const char * output_end = dest + output_size;
    const char * source_end = source + source_size;

    UInt8 bytes_size = source[0];
    RUNTIME_CHECK(bytes_size == sizeof(T), bytes_size, sizeof(T));
    source += 1;

    while (source < source_end)
    {
        T data = unalignedLoad<T>(source);
        source += sizeof(T);
        auto count = unalignedLoad<UInt16>(source);
        source += sizeof(UInt16);
        if unlikely (dest + count * sizeof(T) > output_end)
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress RLE-encoded data, output buffer is too small");
        for (UInt16 i = 0; i < count; ++i)
        {
            unalignedStore<T>(dest, data);
            dest += sizeof(T);
        }
    }
}

} // namespace

UInt32 CompressionCodecRLE::doCompressData(const char * source, UInt32 source_size, char * dest) const
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
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress RLE-encoded data. Unsupported bytes size");
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
    if (bytes_size == JUST_COPY_CODE)
    {
        if (source_size - 1 < uncompressed_size)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress RLE-encoded data. File has wrong header");

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
        decompressDataForType<UInt8>(source, source_size, dest, uncompressed_size);
        break;
    case 2:
        decompressDataForType<UInt16>(source, source_size, dest, uncompressed_size);
        break;
    case 4:
        decompressDataForType<UInt32>(source, source_size, dest, uncompressed_size);
        break;
    case 8:
        decompressDataForType<UInt64>(source, source_size, dest, uncompressed_size);
        break;
    default:
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress RLE-encoded data. Unsupported bytes size");
    }
}

} // namespace DB
