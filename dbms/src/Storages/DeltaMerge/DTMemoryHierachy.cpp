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
#include <Storages/DeltaMerge/DTMemoryHierachy.h>

#include <boost/fiber/detail/cpu_relax.hpp>
#include <optional>
#include <sstream>
namespace DB::DM::Memory
{
#pragma push_macro("thread_local")
#undef thread_local

static MemoryResource::pool_options defaultGlobalPoolOptions()
{
    MemoryResource::pool_options global_opts{};
    global_opts.max_blocks_per_chunk = 64u;
    global_opts.largest_required_pool_block = 64u * 1024u * 1024u;
    return global_opts;
}

static MemoryResource::pool_options defaultThreadPoolOptions()
{
    MemoryResource::pool_options thread_opts{};
    thread_opts.max_blocks_per_chunk = 64u;
    thread_opts.largest_required_pool_block = 4u * 1024u * 1024u;
    return thread_opts;
};

static AllocatorMemoryResource<Allocator<false>> SYSTEM_MEMORY_RESOURCE{};
static std::optional<boost::container::pmr::synchronized_pool_resource> GLOBAL_MEMORY_POOL = std::nullopt;
static thread_local std::shared_ptr<ThreadMemoryPool> PER_THREAD_MEMORY_POOL = nullptr;
static MemoryResource::pool_options PER_THREAD_POOL_OPTIONS = defaultThreadPoolOptions();
static size_t INITIAL_BUFFER_SIZE = 64;

struct DefaultMemoryInitHook
{
    DefaultMemoryInitHook()
    {
        GLOBAL_MEMORY_POOL.emplace(defaultGlobalPoolOptions(), &SYSTEM_MEMORY_RESOURCE);
    }
};

static DefaultMemoryInitHook hook{};

AllocatorMemoryResource<Allocator<false>> & system_memory_source()
{
    return SYSTEM_MEMORY_RESOURCE;
}

boost::container::pmr::synchronized_pool_resource & global_memory_pool()
{
    return *GLOBAL_MEMORY_POOL;
}

void replaceGlobalMemoryPool(const MemoryResource::pool_options & options)
{
    GLOBAL_MEMORY_POOL.emplace(options, &SYSTEM_MEMORY_RESOURCE);
}

void setPerThreadPoolOptions(const MemoryResource::pool_options & options)
{
    PER_THREAD_POOL_OPTIONS = options;
}

void setLocalBufferInitialSize(size_t size)
{
    INITIAL_BUFFER_SIZE = size;
}

std::shared_ptr<ThreadMemoryPool> per_thread_memory_pool()
{
    if (unlikely(!PER_THREAD_MEMORY_POOL))
    {
        PER_THREAD_MEMORY_POOL = std::make_shared<ThreadMemoryPool>(PER_THREAD_POOL_OPTIONS, &*GLOBAL_MEMORY_POOL);
    }
    return PER_THREAD_MEMORY_POOL;
}

ThreadMemoryPool::ThreadMemoryPool(MemoryResource::pool_options options, MemoryResource::memory_resource * upstream)
    : pool(options, upstream)
    , log(&Poco::Logger::get("DeltaTreeThreadMemoryPool"))
{
    LOG_FMT_TRACE(log, "thread local memory pool created");
}

LocalAllocatorBuffer LocalAllocatorBuffer::create()
{
    std::shared_ptr<ThreadMemoryPool> thread_pool = per_thread_memory_pool();

    /// trigger garbage collect to clean up MPMCStack.
    thread_pool->garbageCollect();

    /// allocate memory from local pool
    LocalAllocatorBuffer buffer;
    auto * cell = static_cast<ThreadMemoryPool::Cell *>(
        thread_pool->allocate(sizeof(ThreadMemoryPool::Cell), alignof(ThreadMemoryPool::Cell)));

    /// setup fields
    ::new (cell) ThreadMemoryPool::Cell(INITIAL_BUFFER_SIZE, thread_pool.get());
    buffer.cell = cell;
    buffer.upstream_holder = std::move(thread_pool);
    return buffer;
}

#pragma pop_macro("thread_local")
} // namespace DB::DM::Memory