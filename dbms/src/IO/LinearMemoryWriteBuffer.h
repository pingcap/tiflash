#pragma once
#include <forward_list>

#include <Common/Allocator.h>
#include <Core/Defines.h>
#include <IO/IReadableWriteBuffer.h>
#include <IO/WriteBuffer.h>
#include <boost/noncopyable.hpp>

#include <Common/RecycledAllocator.h>

namespace DB
{

/// Similar to MemoryWriteBuffer, but grow buffer linearly. And you can customize the memory allocator.
class LinearMemoryWriteBuffer : public WriteBuffer, public IReadableWriteBuffer, boost::noncopyable
{
public:
    static constexpr size_t DEFAULT_CHUNK_SIZE = 65536; // Equal to max_fixed_block_size in ArenaWithFreeLists

    LinearMemoryWriteBuffer(RecycledAllocator & allocator_, size_t chunk_size_ = DEFAULT_CHUNK_SIZE);

    void nextImpl() override;

    ~LinearMemoryWriteBuffer() override;

protected:
    std::shared_ptr<ReadBuffer> getReadBufferImpl() override;

    RecycledAllocator & allocator;
    const size_t        chunk_size;

    using Container = std::forward_list<BufferBase::Buffer>;

    Container           chunk_list;
    Container::iterator chunk_tail;
    size_t              total_chunks_size = 0;

    void addChunk();

    friend class ReadBufferFromLinearMemoryWriteBuffer;
};


} // namespace DB
