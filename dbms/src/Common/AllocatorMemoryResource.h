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
#include <Common/Allocator.h>
#include <common/defines.h>
#include <common/numa.h>

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

public:
    static AllocatorMemoryResource create()
    {
        return {};
    }

    void swap(AllocatorMemoryResource & other)
    {
        UNUSED(other);
    }
};

template <class BaseResource>
struct NumaAwareWrapper : BaseResource
{
    void * do_allocate(std::size_t bytes, std::size_t alignment) override
    {
        static auto page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
        auto memory = this->allocate(bytes, alignment);
        if (bytes >= page_size)
        {
            auto node = common::numa::getNumaNode();
            common::numa::bindMemoryToNuma(memory, bytes, node);
        }
        return memory;
    }
};

using DefaultNumaResource = NumaAwareWrapper<AllocatorMemoryResource<Allocator<false>>>;
} // namespace DB