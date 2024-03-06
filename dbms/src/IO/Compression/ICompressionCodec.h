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

#pragma once

#include <IO/Compression/CompressionInfo.h>
#include <common/types.h>

#include <boost/noncopyable.hpp>


namespace DB
{

/**
* Represents interface for compression codecs like LZ4, ZSTD, etc.
*/
class ICompressionCodec : private boost::noncopyable
{
public:
    virtual ~ICompressionCodec() = default;

    /// Byte which indicates codec in compressed file
    virtual UInt8 getMethodByte() const = 0;

    /// Compressed bytes from uncompressed source to dest. Dest should preallocate memory
    UInt32 compress(const char * source, UInt32 source_size, char * dest) const;

    /// Decompress bytes from compressed source to dest. Dest should preallocate memory;
    void decompress(const char * source, UInt32 source_size, char * dest, UInt32 dest_size) const;

    /// Number of bytes, that will be used to compress uncompressed_size bytes with current codec
    virtual UInt32 getCompressedReserveSize(UInt32 uncompressed_size) const
    {
        return getHeaderSize() + getMaxCompressedDataSize(uncompressed_size);
    }

    /// Size of header in compressed data on disk
    static constexpr UInt8 getHeaderSize() { return COMPRESSED_BLOCK_HEADER_SIZE; }

    /// Read size of compressed block from compressed source
    static UInt32 readCompressedBlockSize(const char * source);

    /// Read size of decompressed block from compressed source
    static UInt32 readDecompressedBlockSize(const char * source);

    /// Read method byte from compressed source
    static UInt8 readMethod(const char * source);

    /// Return true if this codec actually compressing something. Otherwise it can be just transformation that helps compression (e.g. Delta).
    virtual bool isCompression() const = 0;

    /// Is it a generic compression algorithm like lz4, zstd. Usually it does not make sense to apply generic compression more than single time.
    virtual bool isGenericCompression() const = 0;

    /// Is this the DEFLATE_QPL codec?
    virtual bool isDeflateQpl() const { return false; }

    /// If the codec's purpose is to calculate deltas between consecutive values.
    virtual bool isDeltaCompression() const { return false; }

    /// If it does nothing.
    virtual bool isNone() const { return false; }

protected:
    /// Return size of compressed data without header
    virtual UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const { return uncompressed_size; }

    /// Actually compress data without header
    virtual UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const = 0;

    /// Actually decompress data without header
    virtual void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
        = 0;
};

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

} // namespace DB
