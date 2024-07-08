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

// The Frame of Reference (FOR) compression scheme for numeric values: Instead
// of compressing the actual value, use a value close to all others in the same
// range (for integers - often the minimum value, or the minimum without
// outliers/exceptionals) and encode all values using their difference from
// this reference. The differences typically need less bits to represent.
// One could think of this as an approximation of the data by a constant + residuals.
// Reference: https://dbms-arch.fandom.com/wiki/Frame_of_Reference_(Compression_Scheme)
class CompressionCodecRLE : public ICompressionCodec
{
public:
    explicit CompressionCodecRLE(UInt8 bytes_size_);

    UInt8 getMethodByte() const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
        const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return false; }
    bool isGenericCompression() const override { return false; }

private:
    const UInt8 bytes_size;
};

} // namespace DB
