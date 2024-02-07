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

#include <memory>
#include <streambuf>


namespace DB
{
class ReadBuffer;
class ReadBufferFromFileBase;

/// `std::streambuf`-compatible wrapper around a ReadBuffer.
/// Warning:
/// The implementation this class is quite tricky and not complete.
/// We need to continuously keep the pointers of get area in sync by calling `setg()` and changing `position()`.
/// Please use it with caution and do enough tests.
class StdStreamBufFromReadBuffer : public std::streambuf
{
public:
    using Base = std::streambuf;

    explicit StdStreamBufFromReadBuffer(std::unique_ptr<ReadBuffer> read_buffer_, size_t size_);
    explicit StdStreamBufFromReadBuffer(ReadBuffer & read_buffer_, size_t size_);
    ~StdStreamBufFromReadBuffer() override;

private:
    // get area
    int underflow() override;
    std::streamsize showmanyc() override;
    std::streamsize xsgetn(char * s, std::streamsize count) override;

    // buffer and positioning
    std::streampos seekoff(std::streamoff off, std::ios_base::seekdir dir, std::ios_base::openmode which) override;
    std::streampos seekpos(std::streampos pos, std::ios_base::openmode which) override;

    // put area
    std::streamsize xsputn(const char * s, std::streamsize n) override;
    int overflow(int c) override;

    std::streampos getCurrentPosition() const;

    std::unique_ptr<ReadBuffer> read_buffer;
    ReadBufferFromFileBase * seekable_read_buffer = nullptr;
    size_t size;
};

} // namespace DB
