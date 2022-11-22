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

#include <Common/LRUCache.h>
#include <Common/Logger.h>
#include <common/types.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{

TEST(LRUCacheTest, WeightAfterRemove)
{
    constexpr size_t cache_max_size = 10;
    LRUCache<Int32, Int32> cache(cache_max_size);
    for (Int32 i = 0; i < 1000; ++i)
        cache.getOrSet(i, [i]() { return std::make_shared<Int32>(i); });
    for (Int32 i = 0; i < 1000; ++i)
        cache.remove(i);
    ASSERT_EQ(cache.count(), 0);
    ASSERT_EQ(cache.weight(), 0);

    for (size_t i = 0; i < 3 * cache_max_size; ++i)
        cache.getOrSet(i, [i]() { return std::make_shared<Int32>(i); });
    ASSERT_EQ(cache.count(), cache_max_size);
    ASSERT_EQ(cache.weight(), cache_max_size);

    cache.reset();
    ASSERT_EQ(cache.count(), 0);
    ASSERT_EQ(cache.weight(), 0);
}

TEST(LRUCacheTest, Concurrent)
{

}

} // namespace tests

} // namespace DB
