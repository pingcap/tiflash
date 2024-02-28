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


#include <IO/Compression/CompressionCodecNone.h>
#include <IO/Compression/CompressionInfo.h>


namespace DB
{

CompressionCodecNone::CompressionCodecNone() = default;

uint8_t CompressionCodecNone::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::NONE);
}

UInt32 CompressionCodecNone::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    memcpy(dest, source, source_size);
    return source_size;
}

void CompressionCodecNone::doDecompressData(
    const char * source,
    UInt32 /*source_size*/,
    char * dest,
    UInt32 uncompressed_size) const
{
    memcpy(dest, source, uncompressed_size);
}

} // namespace DB
