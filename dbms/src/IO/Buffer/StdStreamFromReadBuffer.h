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

#include <IO/Buffer/StdStreamBufFromReadBuffer.h>

#include <iostream>
#include <memory>


namespace DB
{
class ReadBuffer;

/// `std::istream`-compatible wrapper around a ReadBuffer.
class StdIStreamFromReadBuffer : public std::istream
{
public:
    using Base = std::istream;
    StdIStreamFromReadBuffer(std::unique_ptr<ReadBuffer> buf, size_t size)
        : Base(&stream_buf)
        , stream_buf(std::move(buf), size)
    {}
    StdIStreamFromReadBuffer(ReadBuffer & buf, size_t size)
        : Base(&stream_buf)
        , stream_buf(buf, size)
    {}
    StdStreamBufFromReadBuffer * rdbuf() const { return const_cast<StdStreamBufFromReadBuffer *>(&stream_buf); }

private:
    StdStreamBufFromReadBuffer stream_buf;
};


/// `std::iostream`-compatible wrapper around a ReadBuffer.
class StdStreamFromReadBuffer : public std::iostream
{
public:
    using Base = std::iostream;
    StdStreamFromReadBuffer(std::unique_ptr<ReadBuffer> buf, size_t size)
        : Base(&stream_buf)
        , stream_buf(std::move(buf), size)
    {}
    StdStreamFromReadBuffer(ReadBuffer & buf, size_t size)
        : Base(&stream_buf)
        , stream_buf(buf, size)
    {}
    StdStreamBufFromReadBuffer * rdbuf() const { return const_cast<StdStreamBufFromReadBuffer *>(&stream_buf); }

private:
    StdStreamBufFromReadBuffer stream_buf;
};

} // namespace DB
