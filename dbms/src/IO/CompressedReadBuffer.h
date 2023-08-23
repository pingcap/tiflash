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
#include <IO/CompressedReadBufferBase.h>
#include <IO/ReadBuffer.h>


namespace DB
{
template <bool has_checksum = true>
class CompressedReadBuffer
    : public CompressedReadBufferBase<has_checksum>
    , public BufferWithOwnMemory<ReadBuffer>
{
private:
    size_t size_compressed = 0;

    bool nextImpl() override;

public:
    CompressedReadBuffer(ReadBuffer & in_)
        : CompressedReadBufferBase<has_checksum>(&in_)
        , BufferWithOwnMemory<ReadBuffer>(0)
    {}

    size_t readBig(char * to, size_t n) override;

    /// The compressed size of the current block.
    size_t getSizeCompressed() const { return size_compressed; }
};

} // namespace DB
