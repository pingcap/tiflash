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

#include <Common/PODArray.h>
#include <IO/Buffer/BufferWithOwnMemory.h>
#include <IO/Buffer/WriteBuffer.h>
#include <IO/Compression/CompressionSettings.h>
#include <IO/Compression/ICompressionCodec.h>

#include <memory>


namespace DB
{
/**
  * add_legacy_checksum:
  *   For the clickhouse implementation, there is a built-in checksum inside
  *   CompressedWriteBuffer.
  *   It is recommand to combine `FramedChecksumWriteBuffer` with
  *   `CompressedWriteBuffer<add_legacy_checksum=false>` instead.
  */
template <bool add_legacy_checksum = true>
class CompressedWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
private:
    WriteBuffer & out;
    CompressionSettings compression_settings;
    CompressionCodecPtr codec;
    PODArray<char> compressed_buffer;

    void nextImpl() override;

public:
    explicit CompressedWriteBuffer(
        WriteBuffer & out_,
        CompressionSettings compression_settings = CompressionSettings(),
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    static std::unique_ptr<WriteBuffer> build(
        WriteBuffer & plain_file,
        CompressionSettings compression_settings,
        bool enable_legacy_checksum)
    {
        if (!enable_legacy_checksum)
        {
            return std::make_unique<CompressedWriteBuffer<false>>(plain_file, std::move(compression_settings));
        }
        return std::make_unique<CompressedWriteBuffer<true>>(plain_file, std::move(compression_settings));
    }

    /// The amount of compressed data
    size_t getCompressedBytes()
    {
        nextIfAtEnd();
        return out.count();
    }

    /// How many uncompressed bytes were written to the buffer
    size_t getUncompressedBytes() { return count(); }

    /// How many bytes are in the buffer (not yet compressed)
    size_t getRemainingBytes()
    {
        nextIfAtEnd();
        return offset();
    }

    ~CompressedWriteBuffer() override;
};

} // namespace DB
