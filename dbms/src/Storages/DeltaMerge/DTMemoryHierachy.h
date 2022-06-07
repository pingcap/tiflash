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
void initGlobalMemoryPool(const MemoryResource::pool_options & options);
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
    LocalAllocatorBuffer() = default;

public:
    ~LocalAllocatorBuffer() override
    {
        upstream_holder->recycle(cell);
    }

    static LocalAllocatorBuffer create();

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
