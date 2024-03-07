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
#include <Core/Defines.h>
#include <IO/Buffer/IReadableWriteBuffer.h>
#include <IO/Buffer/WriteBuffer.h>

#include <boost/noncopyable.hpp>
#include <forward_list>


namespace DB
{
/// Stores data in memory chunks, size of cunks are exponentially increasing during write
/// Written data could be reread after write
class MemoryWriteBuffer
    : public WriteBuffer
    , public IReadableWriteBuffer
    , boost::noncopyable
    , private Allocator<false>
{
public:
    /// Use max_total_size_ = 0 for unlimited storage
    explicit MemoryWriteBuffer(
        size_t max_total_size_ = 0,
        size_t initial_chunk_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        double growth_rate_ = 2.0,
        size_t max_chunk_size_ = 128 * DBMS_DEFAULT_BUFFER_SIZE);

    void nextImpl() override;

    ~MemoryWriteBuffer() override;

protected:
    std::shared_ptr<ReadBuffer> getReadBufferImpl() override;

    const size_t max_total_size;
    const size_t initial_chunk_size;
    const size_t max_chunk_size;
    const double growth_rate;

    using Container = std::forward_list<BufferBase::Buffer>;

    Container chunk_list;
    Container::iterator chunk_tail;
    size_t total_chunks_size = 0;

    void addChunk();

    friend class ReadBufferFromMemoryWriteBuffer;
};


} // namespace DB
