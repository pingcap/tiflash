#include <Storages/DeltaMerge/DTMemoryHierachy.h>

#include <boost/fiber/detail/cpu_relax.hpp>
#include <optional>
namespace DB::DM::Memory
{
#pragma push_macro("thread_local")
#undef thread_local

static AllocatorMemoryResource<Allocator<false>> SYSTEM_MEMORY_RESOURCE{};
static std::optional<boost::container::pmr::synchronized_pool_resource> GLOBAL_MEMORY_POOL;
static thread_local std::shared_ptr<ThreadMemoryPool> PER_THREAD_MEMORY_POOL = nullptr;
static MemoryResource::pool_options PER_THREAD_POOL_OPTIONS{};
static size_t INITIAL_BUFFER_SIZE = 1024;

AllocatorMemoryResource<Allocator<false>> & system_memory_source()
{
    return SYSTEM_MEMORY_RESOURCE;
}

boost::container::pmr::synchronized_pool_resource & global_memory_pool()
{
    return *GLOBAL_MEMORY_POOL;
}

void initGlobalMemoryPool(const MemoryResource::pool_options & options)
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
{
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