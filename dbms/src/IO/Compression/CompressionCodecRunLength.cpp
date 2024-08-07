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
#include <IO/Compression/CompressionCodecLZ4.h>
#include <IO/Compression/CompressionCodecRunLength.h>
#include <IO/Compression/CompressionInfo.h>
#include <IO/Compression/CompressionSettings.h>
#include <IO/Compression/EncodingUtil.h>
#include <IO/Compression/ICompressionCodec.h>
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

CompressionCodecRunLength::CompressionCodecRunLength(CompressionDataType data_type_)
    : data_type(data_type_)
{}

UInt8 CompressionCodecRunLength::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::RunLength);
}

UInt32 CompressionCodecRunLength::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // If the data is not compressible as run-length encoding, we will compress it as LZ4.
    // 1 byte for data type, and the rest for LZ4 compressed data.
    return 1 + LZ4_COMPRESSBOUND(uncompressed_size);
}

template <typename T>
UInt32 CompressionCodecRunLength::compressDataForInteger(const char * source, UInt32 source_size, char * dest) const
{
    constexpr auto bytes_size = sizeof(T);
    if unlikely (source_size % bytes_size != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "source size {} is not aligned to {}", source_size, bytes_size);

    auto * typed_source = reinterpret_cast<const T *>(source);
    UInt32 source_count = source_size / bytes_size;

    if (DB::Compression::runLengthEncodedApproximateSize<T>(typed_source, source_count)
        >= source_size / CompressionCodecLZ4::ESRTIMATE_INTEGER_COMPRESSION_RATIO)
    {
        // If run-length encoding is not efficient, use LZ4 instead.
        // treat as unknown data type
        dest[0] = magic_enum::enum_integer(CompressionDataType::Unknown);
        dest += 1;
        auto success = LZ4_compress_fast(
            source,
            dest,
            source_size,
            LZ4_COMPRESSBOUND(source_size),
            CompressionSetting::getDefaultLevel(CompressionMethod::LZ4));
        if (unlikely(!success))
            throw Exception("Cannot LZ4_compress_fast", ErrorCodes::CANNOT_COMPRESS);
        return 1 + success;
    }

    dest[0] = magic_enum::enum_integer(data_type);
    dest += 1;
    return 1 + DB::Compression::runLengthEncoding<T>(typed_source, source_count, dest);
}

UInt32 CompressionCodecRunLength::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    switch (data_type)
    {
    case CompressionDataType::Int8:
        return compressDataForInteger<UInt8>(source, source_size, dest);
    case CompressionDataType::Int16:
        return compressDataForInteger<UInt16>(source, source_size, dest);
    case CompressionDataType::Int32:
        return compressDataForInteger<UInt32>(source, source_size, dest);
    case CompressionDataType::Int64:
        return compressDataForInteger<UInt64>(source, source_size, dest);
    default:
        auto success = LZ4_compress_fast(
            source,
            dest,
            source_size,
            LZ4_COMPRESSBOUND(source_size),
            CompressionSetting::getDefaultLevel(CompressionMethod::LZ4));
        if (unlikely(!success))
            throw Exception("Cannot LZ4_compress_fast", ErrorCodes::CANNOT_COMPRESS);
        return 1 + success;
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

    if (unlikely(uncompressed_size == 0))
        return;

    UInt8 bytes_size = source[0];
    auto data_type = magic_enum::enum_cast<CompressionDataType>(bytes_size);
    RUNTIME_CHECK(data_type.has_value());

    switch (data_type.value())
    {
    case CompressionDataType::Int8:
        DB::Compression::runLengthDecoding<UInt8>(&source[1], source_size - 1, dest, uncompressed_size);
        break;
    case CompressionDataType::Int16:
        DB::Compression::runLengthDecoding<UInt16>(&source[1], source_size - 1, dest, uncompressed_size);
        break;
    case CompressionDataType::Int32:
        DB::Compression::runLengthDecoding<UInt32>(&source[1], source_size - 1, dest, uncompressed_size);
        break;
    case CompressionDataType::Int64:
        DB::Compression::runLengthDecoding<UInt64>(&source[1], source_size - 1, dest, uncompressed_size);
        break;
    default:
        if (unlikely(LZ4_decompress_safe(&source[1], dest, source_size - 1, uncompressed_size) < 0))
            throw Exception("Cannot LZ4_decompress_safe", ErrorCodes::CANNOT_DECOMPRESS);
        break;
    }
}

} // namespace DB
