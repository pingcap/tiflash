#include <Common/AllocatorMemoryResource.h>
#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include <boost/container/pmr/synchronized_pool_resource.hpp>
#include <boost/container/pmr/unsynchronized_pool_resource.hpp>

#include <gtest/gtest.h>

namespace DB::tests
{
TEST(AllocatorMemoryResource, Basics) {
    using ResourceAllocator = AllocatorMemoryResource<Allocator<false>>;
    ResourceAllocator a;
    ResourceAllocator b;
    EXPECT_EQ(a, b);
    auto *p = a.allocate(1024);
    b.deallocate(p, 1024);
}
TEST(AllocatorMemoryResource, MonotonicBuffer) {
    using ResourceAllocator = AllocatorMemoryResource<Allocator<false>>;
    ResourceAllocator base;
    MemoryResource::monotonic_buffer_resource resource(&base);
    auto *p = resource.allocate(1024);
    EXPECT_NE(p, nullptr);
    resource.deallocate(p, 1024);
}
TEST(AllocatorMemoryResource, SynchronizedPool) {
    using ResourceAllocator = AllocatorMemoryResource<Allocator<false>>;
    ResourceAllocator base;
    MemoryResource::synchronized_pool_resource resource(&base);
    auto *p = resource.allocate(1024);
    EXPECT_NE(p, nullptr);
    resource.deallocate(p, 1024);
}
TEST(AllocatorMemoryResource, UnsychronizedPool) {
    using ResourceAllocator = AllocatorMemoryResource<Allocator<false>>;
    ResourceAllocator base;
    MemoryResource::unsynchronized_pool_resource resource(&base);
    auto *p = resource.allocate(1024);
    EXPECT_NE(p, nullptr);
    resource.deallocate(p, 1024);
}
}