// Copyright 2023 PingCAP, Inc.
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
TEST(LRUCacheTest, set)
{
    using SimpleLRUCache = DB::LRUCache<int, int>;
    auto lru_cache = SimpleLRUCache(10, 10);
    lru_cache.set(1, std::make_shared<int>(2));
    lru_cache.set(2, std::make_shared<int>(3));

    auto w = lru_cache.weight();
    auto n = lru_cache.count();
    ASSERT_EQ(w, 2);
    ASSERT_EQ(n, 2);
}

TEST(LRUCacheTest, update)
{
    using SimpleLRUCache = DB::LRUCache<int, int>;
    auto lru_cache = SimpleLRUCache(10, 10);
    lru_cache.set(1, std::make_shared<int>(2));
    lru_cache.set(1, std::make_shared<int>(3));
    auto val = lru_cache.get(1);
    ASSERT_TRUE(val != nullptr);
    ASSERT_TRUE(*val == 3);
}

TEST(LRUCacheTest, get)
{
    using SimpleLRUCache = DB::LRUCache<int, int>;
    auto lru_cache = SimpleLRUCache(10, 10);
    lru_cache.set(1, std::make_shared<int>(2));
    lru_cache.set(2, std::make_shared<int>(3));
    SimpleLRUCache::MappedPtr value = lru_cache.get(1);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 2);

    value = lru_cache.get(2);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 3);
}

struct ValueWeight
{
    size_t operator()(const int & /*key*/, const size_t & x) const { return x; }
};

TEST(LRUCacheTest, evictOnSize)
{
    using SimpleLRUCache = DB::LRUCache<int, size_t>;
    auto lru_cache = SimpleLRUCache(20, 3);
    lru_cache.set(1, std::make_shared<size_t>(2));
    lru_cache.set(2, std::make_shared<size_t>(3));
    lru_cache.set(3, std::make_shared<size_t>(4));
    lru_cache.set(4, std::make_shared<size_t>(5));

    auto n = lru_cache.count();
    ASSERT_EQ(n, 3);

    auto value = lru_cache.get(1);
    ASSERT_TRUE(value == nullptr);
}

TEST(LRUCacheTest, evictOnWeight)
{
    using SimpleLRUCache = DB::LRUCache<int, size_t, std::hash<int>, ValueWeight>;
    auto lru_cache = SimpleLRUCache(10, 10);
    lru_cache.set(1, std::make_shared<size_t>(2));
    lru_cache.set(2, std::make_shared<size_t>(3));
    lru_cache.set(3, std::make_shared<size_t>(4));
    lru_cache.set(4, std::make_shared<size_t>(5));

    auto n = lru_cache.count();
    ASSERT_EQ(n, 2);

    auto w = lru_cache.weight();
    ASSERT_EQ(w, 9);

    auto value = lru_cache.get(1);
    ASSERT_TRUE(value == nullptr);
    value = lru_cache.get(2);
    ASSERT_TRUE(value == nullptr);
}

TEST(LRUCacheTest, getOrSet)
{
    using SimpleLRUCache = DB::LRUCache<int, size_t, std::hash<int>, ValueWeight>;
    auto lru_cache = SimpleLRUCache(10, 10);
    size_t x = 10;
    auto load_func = [&] {
        return std::make_shared<size_t>(x);
    };
    auto [value, loaded] = lru_cache.getOrSet(1, load_func);
    ASSERT_TRUE(value != nullptr);
    ASSERT_TRUE(*value == 10);
}

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

} // namespace tests
} // namespace DB
