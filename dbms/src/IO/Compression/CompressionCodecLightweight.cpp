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

#include <Common/Exception.h>
#include <IO/Compression/CompressionCodecLightweight.h>
#include <common/likely.h>
#include <lz4.h>

#include <magic_enum.hpp>


namespace DB
{

// TODO: metrics

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

CompressionCodecLightweight::CompressionCodecLightweight(CompressionDataType data_type_)
    : data_type(data_type_)
{}

UInt8 CompressionCodecLightweight::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::Lightweight);
}

UInt32 CompressionCodecLightweight::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // 1 byte for bytes_size, 1 byte for mode, and the rest for compressed data
    return 1 + 1 + LZ4_COMPRESSBOUND(uncompressed_size);
}

CompressionCodecLightweight::~CompressionCodecLightweight()
{
    if (ctx.isCompression())
        LOG_INFO(Logger::get(), "lightweight codec: {}", ctx.toDebugString());
}

UInt32 CompressionCodecLightweight::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    dest[0] = magic_enum::enum_integer(data_type);
    dest += 1;
    switch (data_type)
    {
    case CompressionDataType::Int8:
        return 1 + compressDataForInteger<UInt8>(source, source_size, dest);
    case CompressionDataType::Int16:
        return 1 + compressDataForInteger<UInt16>(source, source_size, dest);
    case CompressionDataType::Int32:
        return 1 + compressDataForInteger<UInt32>(source, source_size, dest);
    case CompressionDataType::Int64:
        return 1 + compressDataForInteger<UInt64>(source, source_size, dest);
    case CompressionDataType::Float32:
    case CompressionDataType::Float64:
    case CompressionDataType::String:
        return 1 + compressDataForNonInteger(source, source_size, dest);
    default:
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress lightweight codec data. Invalid data type {}",
            magic_enum::enum_name(data_type));
    }
}

void CompressionCodecLightweight::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if unlikely (source_size < 2)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress lightweight codec data. File has wrong header");

    if (unlikely(uncompressed_size == 0))
        return;

    UInt8 bytes_size = source[0];
    auto data_type = magic_enum::enum_cast<CompressionDataType>(bytes_size);
    if unlikely (!data_type.has_value())
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress lightweight codec data. File has wrong header, unknown data type {}",
            bytes_size);

    UInt32 source_size_no_header = source_size - 1;
    switch (data_type.value())
    {
    case CompressionDataType::Int8:
        decompressDataForInteger<UInt8>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case CompressionDataType::Int16:
        decompressDataForInteger<UInt16>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case CompressionDataType::Int32:
        decompressDataForInteger<UInt32>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case CompressionDataType::Int64:
        decompressDataForInteger<UInt64>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case CompressionDataType::Float32:
    case CompressionDataType::Float64:
    case CompressionDataType::String:
        decompressDataForNonInteger(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    default:
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress lightweight codec data. Invalid data type {}",
            static_cast<int>(data_type.value()));
    }
}

} // namespace DB
