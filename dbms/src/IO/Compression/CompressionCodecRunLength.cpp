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
#include <rle.h>

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
    // 1 byte for data type, and the rest for the compressed data
    return 1 + rle_compress_bounds(uncompressed_size);
}

UInt32 CompressionCodecRunLength::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    dest[0] = magic_enum::enum_integer(data_type);
    dest += 1;

    auto dest_size = rle_compress_bounds(source_size);

    switch (data_type)
    {
    case CompressionDataType::Int8:
        return 1 + Compression::runLengthEncoding<UInt8>(source, source_size, dest, dest_size);
    case CompressionDataType::Int16:
        return 1 + Compression::runLengthEncoding<UInt16>(source, source_size, dest, dest_size);
    case CompressionDataType::Int32:
        return 1 + Compression::runLengthEncoding<UInt32>(source, source_size, dest, dest_size);
    case CompressionDataType::Int64:
        return 1 + Compression::runLengthEncoding<UInt64>(source, source_size, dest, dest_size);
    default:
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Unsupported data type: {}", magic_enum::enum_name(data_type));
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

    source += 1;
    source_size -= 1;
    switch (data_type.value())
    {
    case CompressionDataType::Int8:
        Compression::runLengthDecoding<UInt8>(source, source_size, dest, uncompressed_size);
        break;
    case CompressionDataType::Int16:
        Compression::runLengthDecoding<UInt16>(source, source_size, dest, uncompressed_size);
        break;
    case CompressionDataType::Int32:
        Compression::runLengthDecoding<UInt32>(source, source_size, dest, uncompressed_size);
        break;
    case CompressionDataType::Int64:
        Compression::runLengthDecoding<UInt64>(source, source_size, dest, uncompressed_size);
        break;
    default:
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress RunLength-encoded data. Unknown data type {}",
            bytes_size);
    }
}

} // namespace DB
