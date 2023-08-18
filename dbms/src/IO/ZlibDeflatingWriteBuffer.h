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

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/ZlibCompressionMethod.h>
#include <zlib.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ZLIB_DEFLATE_FAILED;
}

/// Performs compression using zlib library and writes compressed data to out_ WriteBuffer.
class ZlibDeflatingWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    ZlibDeflatingWriteBuffer(
        WriteBuffer & out_,
        ZlibCompressionMethod compression_method,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    /// Flush all pending data and write zlib footer to the underlying buffer.
    /// After the first call to this function, subsequent calls will have no effect and
    /// an attempt to write to this buffer will result in exception.
    void finish();

    ~ZlibDeflatingWriteBuffer() override;

private:
    void nextImpl() override;

    WriteBuffer & out;
    z_stream zstr;
    bool finished = false;
};

} // namespace DB
