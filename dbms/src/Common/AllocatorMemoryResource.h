#pragma once
#include <Common/Allocator.h>
#include <common/defines.h>

#include <algorithm>
#include <boost/container/pmr/memory_resource.hpp>
namespace DB
{
namespace MemoryResource
{
using namespace boost::container::pmr;
}

/// @attention: do not use this with `AllocatorWithStackMemory`, it is not feasible
/// to handle the equality assumption.
template <typename BaseAllocator>
class AllocatorMemoryResource : public BaseAllocator
    , public MemoryResource::memory_resource
{
    void * do_allocate(std::size_t bytes, std::size_t alignment) override
    {
        return this->BaseAllocator::alloc(bytes, alignment);
    }
    void do_deallocate(void * p, std::size_t bytes, std::size_t alignment) override
    {
        UNUSED(alignment);
        this->BaseAllocator::free(p, bytes);
    }
    /// Compare *this with other for identity.
    /// STL states that: Memory allocated using a synchronized_pool_resource
    /// can only be deallocated using that same resource. Hence, we just need to
    /// check whether the base class are the same.
    bool do_is_equal(const MemoryResource::memory_resource & other) const noexcept override
    {
        return dynamic_cast<const BaseAllocator *>(std::addressof(other)) != nullptr;
    }
};
} // namespace DB