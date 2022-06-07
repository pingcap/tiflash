// Copyright 2022 PingCAP, Ltd.
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

/// This file describes the multi-level memory hierarchy of delta merge trees. The general structure
/// is shown the diagram:
///
/// \code{.txt}
///                              ---------------------
///                              |  System Allocator |
///                              ---------------------
///                                        |
///                              ---------------------
///                              | Synchronized Pool |
///                              ---------------------
///                             /                     \
///                            /         .......       \
///             ==============/============= ===========\=================
///             | ThD 1      /             | | ThD N     \               |
///             |  ----------------------- | | ------------------------- |
///             |  | Unsynchronized Pool | | | | Unsynchronized Pool   | |
///             |  ----------------------- | | ------------------------| |
///             ====/======================= ========================\====
///                /                                                  \
///  =============/=================                  =================\=============
///  | Tree 1    /                 |                  | Tree N          \           |
///  |   -----------------------   |                  |   -----------------------   |
///  |   |  Monotonic Buffer   |   |       ....       |   |  Monotonic Buffer   |   |
///  |   -----------------------   |                  |   -----------------------   |
///  ===============================                  ===============================
/// \endcode
///
/// As it is shown in the diagram, all delta tree node memory originates from a global system allocator
/// and then pooled by size in a synchronized pool. Then, each worker threads hold a thread-local cache
/// which acquires memory from the global pool. To do real allocations, each tree hold a buffer as a
/// proxy to the thread local cache.
/// This architecture utilizes the following facts:
/// 1. Node deletions are limited and it is much fewer compared with node creation; therefore, monotonic
///    buffer can be a good choice in this case.
/// 2. Delta Tree are updated in a CoW manner and all modifications happens in a local thread. Hence,
///    thread-local pools and monotonic buffers does not need thread-safety. This enables fast allocation
///    routine.
///
/// However, a particular problem happens when a tree is placed back as an updated copy. How to handle
/// the memory ownership afterwards?
///
/// \code{.txt}
///   ----------------------  shared pointer  -----------------------
///   |  Monotonic Buffer  |  --------------> | Unsynchronized Pool |
///   ----------------------                  -----------------------
/// \endcode
///
/// To address the issue, each `Monotonic Buffer` holds a shared pointer to the `Unsynchronized Pool`.
/// Therefore, it is guaranteed that the memory can be returned back to the upstream each after the
/// thread exits unexpectedly.
///
/// \code{.txt}
///   =============================            =============================
///   | Delta Tree                |            | Thread Local Cache        |
///   | ----------                |            |        -------------      |
///   | | Buffer | -----------------------------------> | MPMCStack |      |
///   | ----------                |            |        -------------      |
///   =============================            =============================
/// \endcode
///
/// Another problem is that later, when the placed copy destructs, the memory deallocation may not
/// happen in the same thread where the upstream unsynchronized pool locates in. The solution is to do
/// a message passing and hands in the buffer to a MPMC stack at upstream. Later, when creating a new
/// buffer from the thread-local pool, one can check if there are pending buffer on the stack. If so, one
/// can destruct the buffer before returning the new buffer.
#pragma once
#include <Common/AllocatorMemoryResource.h>
#include <common/mpmcstack.h>

#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include <boost/container/pmr/pool_options.hpp>
#include <boost/container/pmr/synchronized_pool_resource.hpp>
#include <boost/container/pmr/unsynchronized_pool_resource.hpp>

namespace DB::DM::Memory
{
AllocatorMemoryResource<Allocator<false>> & system_memory_source();
MemoryResource::synchronized_pool_resource & global_memory_pool();
void replaceGlobalMemoryPool(const MemoryResource::pool_options & options);
void setPerThreadPoolOptions(const MemoryResource::pool_options & options);
void setLocalBufferInitialSize(size_t size);
std::shared_ptr<class ThreadMemoryPool> per_thread_memory_pool();

class ThreadMemoryPool : public MemoryResource::synchronized_pool_resource
{
public:
    using DownStream = MemoryResource::monotonic_buffer_resource;
    struct Cell
    {
        DownStream buffer;
        std::atomic<Cell *> next;

        explicit Cell(size_t initial_size, MemoryResource::memory_resource * upstream)
            : buffer(initial_size, upstream)
            , next(nullptr)
        {
        }
    };

public:
    ThreadMemoryPool(
        MemoryResource::pool_options options,
        MemoryResource::memory_resource * upstream);

    friend class LocalAllocatorBuffer;

    /// it should be okay not to clean up the MPMCStack, as the memory will be returned back to upstream anyway.
    /// ~ThreadMemoryPool();

private:
    common::MPMCStack<Cell> stack{};
    MemoryResource::synchronized_pool_resource pool;

    void recycle(Cell * cell)
    {
        stack.push(cell);
    }

    void garbageCollect()
    {
        for (size_t i = 0; i < 5; ++i)
        {
            if (auto * res = stack.pop())
            {
                std::destroy_at(res);
                deallocate(res, sizeof(Cell), alignof(Cell));
            }
        }
    }
};

class LocalAllocatorBuffer : public MemoryResource::memory_resource
{
    /// hold the upstream in case the thread has already exited
    std::shared_ptr<ThreadMemoryPool> upstream_holder = nullptr;
    ThreadMemoryPool::Cell * cell = nullptr;

public:
    LocalAllocatorBuffer() = default;
    LocalAllocatorBuffer(const LocalAllocatorBuffer &) = delete;
    LocalAllocatorBuffer operator=(const LocalAllocatorBuffer &) = delete;
    LocalAllocatorBuffer(LocalAllocatorBuffer && other)
        : upstream_holder(std::move(other.upstream_holder))
        , cell(other.cell)
    {
        other.upstream_holder = nullptr;
        other.cell = nullptr;
    }

    ~LocalAllocatorBuffer() override
    {
        if (cell && upstream_holder)
        {
            upstream_holder->recycle(cell);
        }
    }

    static LocalAllocatorBuffer create();
    void swap(LocalAllocatorBuffer & other)
    {
        std::swap(upstream_holder, other.upstream_holder);
        std::swap(cell, other.cell);
    }

protected:
    void * do_allocate(std::size_t bytes, std::size_t alignment) override
    {
        return this->cell->buffer.allocate(bytes, alignment);
    }
    void do_deallocate(void * p, std::size_t bytes, std::size_t alignment) override
    {
        return this->cell->buffer.deallocate(p, bytes, alignment);
    }
    bool do_is_equal(const MemoryResource::memory_resource & other) const noexcept override
    {
        return dynamic_cast<const LocalAllocatorBuffer *>(std::addressof(other)) != nullptr;
    }
};


} // namespace DB::DM::Memory
