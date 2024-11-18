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

#include <IO/Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecFactory;

class CompressionCodecLZ4 : public ICompressionCodec
{
public:
    // The official document says that the compression ratio of LZ4 is 2.1, https://github.com/lz4/lz4
    static constexpr size_t ESTIMATE_INTEGER_COMPRESSION_RATIO = 3;

    explicit CompressionCodecLZ4(int level_);

    UInt8 getMethodByte() const override;

    bool isCompression() const override { return true; }

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

private:
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
        const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

protected:
    const int level;
    friend class CompressionCodecFactory;
};


class CompressionCodecLZ4HC : public CompressionCodecLZ4
{
public:
    explicit CompressionCodecLZ4HC(int level_);

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
};

} // namespace DB
