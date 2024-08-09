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
#include <IO/Compression/CompressionSettings.h>
#include <IO/Compression/EncodingUtil.h>
#include <common/likely.h>

#include <magic_enum.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

CompressionCodecDeltaFOR::CompressionCodecDeltaFOR(CompressionDataType data_type_)
    : data_type(data_type_)
{}

UInt8 CompressionCodecDeltaFOR::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::DeltaFOR);
}

UInt32 CompressionCodecDeltaFOR::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // |bytes_of_original_type|first_value|frame_of_reference|width(bits)  |bitpacked data|
    // |1 bytes               |bytes_size |bytes_size        |sizeof(UInt8)|required size |
    auto bytes_size = magic_enum::enum_integer(data_type);
    const size_t deltas_count = uncompressed_size / bytes_size - 1;
    return 1 + bytes_size * 2 + sizeof(UInt8) + BitpackingPrimitives::getRequiredSize(deltas_count, bytes_size * 8);
}

namespace
{

template <std::integral T>
UInt32 compressData(const char * source, UInt32 source_size, char * dest)
{
    constexpr auto bytes_size = sizeof(T);
    if unlikely (source_size % bytes_size != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "source size {} is not aligned to {}", source_size, bytes_size);
    const auto count = source_size / bytes_size;
    DB::Compression::deltaEncoding<T>(reinterpret_cast<const T *>(source), count, reinterpret_cast<T *>(dest));
    if (unlikely(count == 1))
        return bytes_size;
    // Cast deltas to signed type to better compress negative values.
    // For example, if we have a sequence of UInt8 values [3, 2, 1, 0], the deltas will be [3, -1, -1, -1]
    // If we compress them as UInt8, we will get [3, 255, 255, 255], which is not optimal.
    using TS = typename std::make_signed<T>::type;
    auto for_size = DB::CompressionCodecFOR::compressData<TS>(
        reinterpret_cast<TS *>(dest + bytes_size),
        source_size - bytes_size,
        dest + bytes_size);
    return bytes_size + for_size;
}

} // namespace

UInt32 CompressionCodecDeltaFOR::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    dest[0] = magic_enum::enum_integer(data_type);
    dest += 1;
    switch (data_type)
    {
    case CompressionDataType::Int8:
        return 1 + compressData<UInt8>(source, source_size, dest);
    case CompressionDataType::Int16:
        return 1 + compressData<UInt16>(source, source_size, dest);
    case CompressionDataType::Int32:
        return 1 + compressData<UInt32>(source, source_size, dest);
    case CompressionDataType::Int64:
        return 1 + compressData<UInt64>(source, source_size, dest);
    default:
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Unsupported data type: {}", magic_enum::enum_name(data_type));
    }
}

void CompressionCodecDeltaFOR::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if (unlikely(source_size < 2))
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress DeltaFor-encoded data. File has wrong header");

    if (unlikely(uncompressed_size == 0))
        return;

    UInt8 bytes_size = source[0];
    auto data_type = magic_enum::enum_cast<CompressionDataType>(bytes_size);
    RUNTIME_CHECK(data_type.has_value(), bytes_size);

    UInt32 source_size_no_header = source_size - 1;
    switch (data_type.value())
    {
    case CompressionDataType::Int8:
        DB::Compression::deltaFORDecoding<UInt8>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case CompressionDataType::Int16:
        DB::Compression::deltaFORDecoding<UInt16>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case CompressionDataType::Int32:
        DB::Compression::deltaFORDecoding<UInt32>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case CompressionDataType::Int64:
        DB::Compression::deltaFORDecoding<UInt64>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    default:
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Unsupported data type: {}",
            magic_enum::enum_name(data_type.value()));
    }
}

void CompressionCodecDeltaFOR::ordinaryDecompress(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size)
{
    if (unlikely(source_size < 2))
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress DeltaFor-encoded data. File has wrong header");

    if (unlikely(uncompressed_size == 0))
        return;

    UInt8 bytes_size = source[0];
    auto data_type = magic_enum::enum_cast<CompressionDataType>(bytes_size);
    RUNTIME_CHECK(data_type.has_value(), bytes_size);

    const UInt32 source_size_no_header = source_size - 1;
    switch (data_type.value())
    {
    case CompressionDataType::Int8:
        DB::Compression::ordinaryDeltaFORDecoding<UInt8>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case CompressionDataType::Int16:
        DB::Compression::ordinaryDeltaFORDecoding<UInt16>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case CompressionDataType::Int32:
        DB::Compression::ordinaryDeltaFORDecoding<UInt32>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    case CompressionDataType::Int64:
        DB::Compression::ordinaryDeltaFORDecoding<UInt64>(&source[1], source_size_no_header, dest, uncompressed_size);
        break;
    default:
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Unsupported data type: {}",
            magic_enum::enum_name(data_type.value()));
    }
}

} // namespace DB
