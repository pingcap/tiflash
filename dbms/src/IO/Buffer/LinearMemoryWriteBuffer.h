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
#include <Common/Allocator.h>
#include <Common/RecycledAllocator.h>
#include <Core/Defines.h>
#include <IO/Buffer/IReadableWriteBuffer.h>
#include <IO/Buffer/WriteBuffer.h>

#include <boost/noncopyable.hpp>
#include <forward_list>

namespace DB
{
/// Similar to MemoryWriteBuffer, but grow buffer linearly. And you can customize the memory allocator.
class LinearMemoryWriteBuffer
    : public WriteBuffer
    , public IReadableWriteBuffer
    , private boost::noncopyable
{
public:
    static constexpr size_t DEFAULT_CHUNK_SIZE = 65536; // Equal to max_fixed_block_size in ArenaWithFreeLists

    explicit LinearMemoryWriteBuffer(RecycledAllocator & allocator_, size_t chunk_size_ = DEFAULT_CHUNK_SIZE);

    void nextImpl() override;

    ~LinearMemoryWriteBuffer() override;

protected:
    std::shared_ptr<ReadBuffer> getReadBufferImpl() override;

    RecycledAllocator & allocator;
    const size_t chunk_size;

    using Container = std::forward_list<BufferBase::Buffer>;

    Container chunk_list;
    Container::iterator chunk_tail;
    size_t total_chunks_size = 0;

    void addChunk();

    friend class ReadBufferFromLinearMemoryWriteBuffer;
};


} // namespace DB
