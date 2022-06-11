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

#pragma once
#include <type_traits>
#include <boost/container/pmr/synchronized_pool_resource.hpp>
#include <boost/container/pmr/unsynchronized_pool_resource.hpp>
#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <common/mpmcstack.h>
#include <memory>
#include <mutex>
namespace DB::NumaAwareMemoryHierarchy
{
extern const size_t PAGE_SIZE;
static inline constexpr size_t SIZE_2MIB = 2 * 1024 * 1024;
static inline constexpr size_t SIZE_512KIB = 512 * 1024;

struct Node
{
    Node * next;
};

struct Chunk {
    Chunk * next;
    char * data;

    Chunk(Chunk * next, char * data) : next(next), data(data) {}
};

struct GlobalPagePool
{
    /// PerNumaFreeList is for slow path memory operations.
    /// Mutexes are good enough to protect free lists.
    struct PerNumaFreeList
    {
        const size_t numa{};
        std::mutex lock_2mb{};
        Node * freelist_2mb{};
        boost::container::pmr::synchronized_pool_resource internal_resource {};

        explicit PerNumaFreeList(size_t numa)
            : numa(numa)
        {}

        void * allocate();
        void recycle(void * p);

        ~PerNumaFreeList();
    };

    const size_t numa_count = 0;
    PerNumaFreeList * numa_freelists = nullptr;
    GlobalPagePool();
    ~GlobalPagePool();
    PerNumaFreeList & getFreeList() const;
};

namespace Impl
{

template <size_t chunk_size, class Upstream>
struct LocalFreeList {

    const size_t size;
    Upstream * source = nullptr;
    char * current = nullptr;
    Node * freelist = nullptr;
    Chunk * chunk_list = nullptr;

    boost::container::pmr::unsynchronized_pool_resource internal_resource;
    boost::container::pmr::polymorphic_allocator<Chunk> chunk_allocator;

    explicit LocalFreeList(Upstream * src, size_t size)
        : size(size)
        , source(src)
        , internal_resource(&src->internal_resource)
        , chunk_allocator(&internal_resource)
    {
    }

    void allocateNewChunk() {
        auto * chunk = chunk_allocator.allocate(1);
        chunk_allocator.template construct(chunk, chunk_list, static_cast<char *>(source->allocate()));
        chunk_list = chunk;
        current = chunk->data;
    };

    void * allocate()
    {
        // first allocate from freelist
        if (auto * blk = freelist)
        {
            freelist = freelist->next;
            return blk;
        }

        // then allocate from current chunk
        if (current != nullptr && current + size <= chunk_list->data + chunk_size)
        {
            auto * res = current;
            current += size;
            return res;
        }

        // then allocate from upstream
        allocateNewChunk();
        auto * res = current;
        current += size;
        return res;
    }

    void recycle(void * p)
    {
        freelist = ::new (p) Node{freelist};
    }

    ~LocalFreeList()
    {
        while (auto * chunk = chunk_list)
        {
            chunk_list = chunk->next;
            source->recycle(chunk->data);
            chunk_allocator.template destroy(chunk);
            chunk_allocator.deallocate(chunk, 1);
        }
    }
};
} // namespace Impl

struct ThreadLocalMemPool
{
    using Upstream = GlobalPagePool::PerNumaFreeList;
    using ThreadLocalList = Impl::LocalFreeList<SIZE_2MIB, Upstream>;
    using ClientList = Impl::LocalFreeList<SIZE_512KIB, ThreadLocalList>;

    struct Cell {
        std::atomic<Cell *> next;
        ClientList freelist;

        Cell(ThreadLocalList * upstream, size_t client_size)
            : next(nullptr)
            , freelist(upstream, client_size) {}
    };

    ThreadLocalList thread_local_list;
    boost::container::pmr::polymorphic_allocator<Cell> cell_allocator;
    common::MPMCStack<Cell> cell_list {};

    explicit ThreadLocalMemPool(Upstream * upstream)
        : thread_local_list(upstream, SIZE_512KIB)
        , cell_allocator(&thread_local_list.internal_resource)
    {
    }

    void garbageCollect() {
        for (size_t i = 0; i < 5; ++i) {
            if (auto * cell = cell_list.pop()) {
                cell_allocator.destroy(cell);
                cell_allocator.deallocate(cell, 1);
            }
        }
    }

    Cell* createCell(size_t client_size) {
        garbageCollect();
        auto * res = cell_allocator.allocate(1);
        cell_allocator.construct(res, &thread_local_list, client_size);
        return res;
    }
};


struct Client {
    std::shared_ptr<ThreadLocalMemPool> upstream_holder;
    ThreadLocalMemPool::Cell * cell;
    void * allocate() const {
        return cell->freelist.allocate();
    }
    void deallocate(void * p) const {
        return cell->freelist.recycle(p);
    }

    static inline size_t alignedSize(size_t size, size_t alignment) {
        auto delta = size % alignment;
        auto offset = delta == 0 ? 0 : alignment - delta;
        return size + offset;
    }

    Client(std::shared_ptr<ThreadLocalMemPool> upstream, size_t client_size, size_t alignment = 8)
        : upstream_holder(std::move(upstream))
        , cell(upstream_holder->createCell(alignedSize(client_size, alignment)))
    {

    }
    ~Client() {
        upstream_holder->cell_list.push(cell);
    }
};


} // namespace DB::NumaAwareMemoryHierarchy