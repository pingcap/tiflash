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
#include <IO/Compression/CompressionSettings.h>
#include <IO/Compression/EncodingUtil.h>
#include <common/likely.h>
#include <common/unaligned.h>
#include <lz4.h>

#include <magic_enum.hpp>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

CompressionCodecFOR::CompressionCodecFOR(CompressionDataType data_type_)
    : data_type(data_type_)
{}

UInt8 CompressionCodecFOR::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::FOR);
}

UInt32 CompressionCodecFOR::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    switch (data_type)
    {
    case CompressionDataType::Int8:
    case CompressionDataType::Int16:
    case CompressionDataType::Int32:
    case CompressionDataType::Int64:
    {
        // |bytes_of_original_type|frame_of_reference|width(bits)  |bitpacked data|
        // |1 bytes               |bytes_size        |sizeof(UInt8)|required size |
        auto bytes_size = magic_enum::enum_integer(data_type);
        const size_t count = uncompressed_size / bytes_size;
        return 1 + bytes_size + sizeof(UInt8) + BitpackingPrimitives::getRequiredSize(count, bytes_size * 8);
    }
    default:
        return 1 + LZ4_COMPRESSBOUND(uncompressed_size);
    }
}

template <std::integral T>
UInt32 CompressionCodecFOR::compressData(const T * source, UInt32 source_size, char * dest)
{
    constexpr size_t bytes_size = sizeof(T);
    if unlikely (source_size % bytes_size != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "source size {} is not aligned to {}", source_size, bytes_size);
    auto count = source_size / bytes_size;
    if unlikely (count == 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress empty data");
    std::vector<T> values(source, source + count);
    T frame_of_reference = *std::min_element(values.cbegin(), values.cend());
    UInt8 width = DB::Compression::FOREncodingWidth(values, frame_of_reference);
    return DB::Compression::FOREncoding<T, std::is_signed_v<T>>(values, frame_of_reference, width, dest);
}

UInt32 CompressionCodecFOR::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    dest[0] = magic_enum::enum_integer(data_type);
    dest += 1;
    switch (data_type)
    {
    case CompressionDataType::Int8:
        return 1 + compressData<UInt8>(reinterpret_cast<const UInt8 *>(source), source_size, dest);
    case CompressionDataType::Int16:
        return 1 + compressData<UInt16>(reinterpret_cast<const UInt16 *>(source), source_size, dest);
    case CompressionDataType::Int32:
        return 1 + compressData<UInt32>(reinterpret_cast<const UInt32 *>(source), source_size, dest);
    case CompressionDataType::Int64:
        return 1 + compressData<UInt64>(reinterpret_cast<const UInt64 *>(source), source_size, dest);
    default:
        auto success = LZ4_compress_fast(
            source,
            dest,
            source_size,
            LZ4_COMPRESSBOUND(source_size),
            CompressionSetting::getDefaultLevel(CompressionMethod::LZ4));
        if (!success)
            throw Exception("Cannot LZ4_compress_fast", ErrorCodes::CANNOT_COMPRESS);
        return 1 + success;
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
    auto data_type = magic_enum::enum_cast<CompressionDataType>(bytes_size);
    RUNTIME_CHECK(data_type.has_value());

    UInt32 source_size_no_header = source_size - 1;
    switch (data_type.value())
    {
    case CompressionDataType::Int8:
        DB::Compression::FORDecoding<UInt8>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case CompressionDataType::Int16:
        DB::Compression::FORDecoding<UInt16>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case CompressionDataType::Int32:
        DB::Compression::FORDecoding<UInt32>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case CompressionDataType::Int64:
        DB::Compression::FORDecoding<UInt64>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    default:
        if (unlikely(LZ4_decompress_safe(&source[1], dest, source_size_no_header, uncompressed_size) < 0))
            throw Exception("Cannot LZ4_decompress_safe", ErrorCodes::CANNOT_DECOMPRESS);
        break;
    }
}

// The following instantiations are used in CompressionCodecDeltaFor.cpp

template UInt32 CompressionCodecFOR::compressData<Int8>(const Int8 * source, UInt32 source_size, char * dest);
template UInt32 CompressionCodecFOR::compressData<Int16>(const Int16 * source, UInt32 source_size, char * dest);
template UInt32 CompressionCodecFOR::compressData<Int32>(const Int32 * source, UInt32 source_size, char * dest);
template UInt32 CompressionCodecFOR::compressData<Int64>(const Int64 * source, UInt32 source_size, char * dest);

} // namespace DB
