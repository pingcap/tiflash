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

#include <IO/Buffer/LinearMemoryWriteBuffer.h>
#include <common/likely.h>

#include <boost/noncopyable.hpp>

namespace DB
{
class ReadBufferFromLinearMemoryWriteBuffer
    : public ReadBuffer
    , private boost::noncopyable
{
public:
    explicit ReadBufferFromLinearMemoryWriteBuffer(LinearMemoryWriteBuffer && origin)
        : ReadBuffer(nullptr, 0)
        , allocator(origin.allocator)
        , chunk_list(std::move(origin.chunk_list))
        , end_pos(origin.position())
    {
        chunk_head = chunk_list.begin();
        setChunk();
    }

    bool nextImpl() override
    {
        if (chunk_head == chunk_list.end())
            return false;

        ++chunk_head;
        return setChunk();
    }

    ~ReadBufferFromLinearMemoryWriteBuffer() override
    {
        for (const auto & range : chunk_list)
            allocator.free(range.begin(), range.size());
    }

private:
    /// update buffers and position according to chunk_head pointer
    bool setChunk()
    {
        if (chunk_head != chunk_list.end())
        {
            internalBuffer() = *chunk_head;

            /// It is last chunk, it should be truncated
            if (std::next(chunk_head) != chunk_list.end())
                buffer() = internalBuffer();
            else
                buffer() = Buffer(internalBuffer().begin(), end_pos);

            position() = buffer().begin();
        }
        else
        {
            buffer() = internalBuffer() = Buffer(nullptr, nullptr);
            position() = nullptr;
        }

        return buffer().size() != 0;
    }

    using Container = std::forward_list<BufferBase::Buffer>;

    RecycledAllocator & allocator;

    Container chunk_list;
    Container::iterator chunk_head;
    Position end_pos;
};


LinearMemoryWriteBuffer::LinearMemoryWriteBuffer(RecycledAllocator & allocator_, size_t chunk_size_)
    : WriteBuffer(nullptr, 0)
    , allocator(allocator_)
    , chunk_size(chunk_size_)
{
    addChunk();
}


void LinearMemoryWriteBuffer::nextImpl()
{
    if (unlikely(hasPendingData()))
    {
        /// ignore flush
        buffer() = Buffer(pos, buffer().end());
        return;
    }

    addChunk();
}


void LinearMemoryWriteBuffer::addChunk()
{
    if (chunk_list.empty())
    {
        chunk_tail = chunk_list.before_begin();
    }

    auto * begin = reinterpret_cast<Position>(allocator.alloc(chunk_size));
    chunk_tail = chunk_list.emplace_after(chunk_tail, begin, begin + chunk_size);
    total_chunks_size += chunk_size;

    set(chunk_tail->begin(), chunk_tail->size());
}


std::shared_ptr<ReadBuffer> LinearMemoryWriteBuffer::getReadBufferImpl()
{
    auto res = std::make_shared<ReadBufferFromLinearMemoryWriteBuffer>(std::move(*this));

    /// invalidate members
    chunk_list.clear();
    chunk_tail = chunk_list.begin();

    return res;
}


LinearMemoryWriteBuffer::~LinearMemoryWriteBuffer()
{
    for (const auto & range : chunk_list)
        allocator.free(range.begin(), range.size());
}

} // namespace DB
