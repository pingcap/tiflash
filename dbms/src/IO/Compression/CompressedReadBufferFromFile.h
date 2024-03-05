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

#include <IO/Buffer/ReadBufferFromFileBase.h>
#include <IO/Compression/CompressedReadBufferBase.h>

namespace DB
{

/// CompressedSeekableReaderBuffer provides an extra abstraction layer to unify compressed buffers
/// This helps to unify CompressedReadBufferFromFileProviderImpl<false> and CompressedReadBufferFromFileProviderImpl<true>
struct CompressedSeekableReaderBuffer : public BufferWithOwnMemory<ReadBuffer>
{
    virtual void setProfileCallback(
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
        clockid_t clock_type_)
        = 0;

    virtual void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block) = 0;

    CompressedSeekableReaderBuffer()
        : BufferWithOwnMemory<ReadBuffer>(0)
    {}
};

/// Unlike CompressedReadBuffer, it can do seek.
template <bool has_legacy_checksum = true>
class CompressedReadBufferFromFileImpl
    : public CompressedReadBufferBase<has_legacy_checksum>
    , public CompressedSeekableReaderBuffer
{
public:
    explicit CompressedReadBufferFromFileImpl(std::unique_ptr<ReadBufferFromFileBase> && file_in_);

    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block) override;

    size_t readBig(char * to, size_t n) override;

    void setProfileCallback(const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_)
        override
    {
        file_in.setProfileCallback(profile_callback_, clock_type_);
    }

private:
    bool nextImpl() override;

    /** At any time, one of two things is true:
      * a) size_compressed = 0
      * b)
      *  - `working_buffer` contains the entire block.
      *  - `file_in` points to the end of this block.
      *  - `size_compressed` contains the compressed size of this block.
      */
    std::unique_ptr<ReadBufferFromFileBase> p_file_in;
    ReadBufferFromFileBase & file_in;
    size_t size_compressed = 0;
};

using LegacyCompressedReadBufferFromFile = CompressedReadBufferFromFileImpl</*has_legacy_checksum*/ true>;
using CompressedReadBufferFromFile = CompressedReadBufferFromFileImpl</*has_legacy_checksum*/ false>;

} // namespace DB
