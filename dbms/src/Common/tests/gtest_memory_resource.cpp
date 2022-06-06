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
#include <Common/AllocatorMemoryResource.h>
#include <gtest/gtest.h>

#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include <boost/container/pmr/synchronized_pool_resource.hpp>
#include <boost/container/pmr/unsynchronized_pool_resource.hpp>

namespace DB::tests
{
TEST(AllocatorMemoryResource, Basics)
{
    using ResourceAllocator = AllocatorMemoryResource<Allocator<false>>;
    ResourceAllocator a;
    ResourceAllocator b;
    EXPECT_EQ(a, b);
    auto * p = a.allocate(1024);
    b.deallocate(p, 1024);
}
TEST(AllocatorMemoryResource, MonotonicBuffer)
{
    using ResourceAllocator = AllocatorMemoryResource<Allocator<false>>;
    ResourceAllocator base;
    MemoryResource::monotonic_buffer_resource resource(&base);
    auto * p = resource.allocate(1024);
    EXPECT_NE(p, nullptr);
    resource.deallocate(p, 1024);
}
TEST(AllocatorMemoryResource, SynchronizedPool)
{
    using ResourceAllocator = AllocatorMemoryResource<Allocator<false>>;
    ResourceAllocator base;
    MemoryResource::synchronized_pool_resource resource(&base);
    auto * p = resource.allocate(1024);
    EXPECT_NE(p, nullptr);
    resource.deallocate(p, 1024);
}
TEST(AllocatorMemoryResource, UnsychronizedPool)
{
    using ResourceAllocator = AllocatorMemoryResource<Allocator<false>>;
    ResourceAllocator base;
    MemoryResource::unsynchronized_pool_resource resource(&base);
    auto * p = resource.allocate(1024);
    EXPECT_NE(p, nullptr);
    resource.deallocate(p, 1024);
}
} // namespace DB::tests