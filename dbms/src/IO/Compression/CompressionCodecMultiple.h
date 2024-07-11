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


/** Compression codec for multiple codecs.
  * It compresses data with multiple codecs and stores the list of codecs in the header.
  * | <---------- Compression Header ---------> | <------------------------------------------- Additional Data -----------------------------------------------------------> | <- Compressed Data -> |
  * | 0x91 - 1 byte | compressed size - 4 bytes | uncompressed size - 4 bytes | number of codecs - 1 byte | code 1 method byte - 1 byte | ... | code N method byte - 1 byte |   compressed block    |
  * The compressed block is the result of compressing the data with all the codecs in the list.
  * Store each codec method byte in the Additional Data to get all the codecs from the compressed data without decompressing it.
  */
class CompressionCodecMultiple final : public ICompressionCodec
{
public:
    explicit CompressionCodecMultiple(Codecs && codecs_);

    UInt8 getMethodByte() const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    static std::vector<UInt8> getCodecsBytesFromData(const char * source);

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 decompressed_size)
        const override;

private:
    Codecs codecs;
};

} // namespace DB
