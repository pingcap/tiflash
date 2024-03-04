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
#include <IO/Compression/ICompressionCodec.h>
#include <common/unaligned.h>

#include <cassert>


namespace DB
{

UInt32 ICompressionCodec::compress(const char * source, UInt32 source_size, char * dest) const
{
    assert(source != nullptr && dest != nullptr);

    dest[0] = getMethodByte();
    UInt8 header_size = getHeaderSize();
    /// Write data from header_size
    UInt32 compressed_bytes_written = doCompressData(source, source_size, &dest[header_size]);
    unalignedStore<UInt32>(&dest[1], compressed_bytes_written + header_size);
    unalignedStore<UInt32>(&dest[5], source_size);
    return header_size + compressed_bytes_written;
}

void ICompressionCodec::decompress(const char * source, UInt32 source_size, char * dest, UInt32 dest_size) const
{
    assert(source != nullptr && dest != nullptr);

    UInt8 header_size = getHeaderSize();
    if (source_size < header_size)
        throw Exception(
            decompression_error_code,
            "Can't decompress data: the compressed data size ({}, this should include header size) "
            "is less than the header size ({})",
            source_size,
            static_cast<size_t>(header_size));

    UInt8 our_method = getMethodByte();
    UInt8 method = source[0];
    if (method != our_method)
        throw Exception(
            decompression_error_code,
            "Can't decompress data with codec byte {} using codec with byte {}",
            method,
            our_method);

    doDecompressData(&source[header_size], source_size - header_size, dest, dest_size);
}

UInt32 ICompressionCodec::readCompressedBlockSize(const char * source) const
{
    auto compressed_block_size = unalignedLoad<UInt32>(&source[1]);
    if (compressed_block_size == 0)
        throw Exception(
            decompression_error_code,
            "Can't decompress data: header is corrupt with compressed block size 0");
    return compressed_block_size;
}


UInt32 ICompressionCodec::readDecompressedBlockSize(const char * source) const
{
    auto decompressed_block_size = unalignedLoad<UInt32>(&source[5]);
    if (decompressed_block_size == 0)
        throw Exception(
            decompression_error_code,
            "Can't decompress data: header is corrupt with decompressed block size 0");
    return decompressed_block_size;
}


UInt8 ICompressionCodec::readMethod(const char * source)
{
    return static_cast<UInt8>(source[0]);
}

} // namespace DB
