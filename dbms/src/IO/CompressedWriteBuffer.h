// Copyright 2022 PingCAP, Ltd.
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

#include <Common/PODArray.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionSettings.h>
#include <IO/WriteBuffer.h>

#include <memory>


namespace DB
{
template <bool add_checksum = true>
class CompressedWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
private:
    WriteBuffer & out;
    CompressionSettings compression_settings;

    PODArray<char> compressed_buffer;

    void nextImpl() override;

public:
    CompressedWriteBuffer(
        WriteBuffer & out_,
        CompressionSettings compression_settings = CompressionSettings(),
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    /// The amount of compressed data
    size_t getCompressedBytes()
    {
        nextIfAtEnd();
        return out.count();
    }

    /// How many uncompressed bytes were written to the buffer
    size_t getUncompressedBytes()
    {
        return count();
    }

    /// How many bytes are in the buffer (not yet compressed)
    size_t getRemainingBytes()
    {
        nextIfAtEnd();
        return offset();
    }

    ~CompressedWriteBuffer() override;
};

} // namespace DB
