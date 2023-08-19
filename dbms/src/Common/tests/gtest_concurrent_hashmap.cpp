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

#include <Common/HashTable/HashMap.h>
#include <Common/UniThreadPool.h>
#include <gtest/gtest.h>

#include <ext/singleton.h>
#include <random>

namespace DB
{
namespace tests
{
class TestConcurrentHashMap : public ext::Singleton<TestConcurrentHashMap>
{
public:
    static size_t test_loop;
};

size_t TestConcurrentHashMap::test_loop = 1;

struct MapType
{
    std::atomic_int value;
    MapType() { value.store(0); }
};

TEST(TestConcurrentHashMap, ConcurrentInsert)
{
    for (size_t time = 0; time < TestConcurrentHashMap::test_loop; time++)
    {
        size_t test_concurrency = 8;
        using ConcurrentMap = ConcurrentHashMap<UInt64, MapType, HashCRC32<UInt64>>;
        ConcurrentMap map(test_concurrency);
        ThreadPool insert_pool(test_concurrency);
        for (size_t i = 0; i < test_concurrency; i++)
        {
            insert_pool.scheduleOrThrowOnError([&] {
                for (size_t insert_value = 0; insert_value < 10000; insert_value++)
                {
                    typename ConcurrentMap::SegmentType::IteratorWithLock it;
                    bool inserted;
                    map.emplace(insert_value, it, inserted);
                    it.first->getMapped().value++;
                }
            });
        }
        insert_pool.wait();
        for (size_t insert_value = 0; insert_value < 10000; insert_value++)
        {
            ASSERT_EQ(map.has(insert_value), true);
            typename ConcurrentMap::SegmentType::IteratorWithLock it = map.find(insert_value);
            ASSERT_EQ(it.first->getMapped().value.load(), (int)test_concurrency);
        }
    }
}

TEST(TestConcurrentHashMap, ConcurrentInsertWithExplicitLock)
{
    for (size_t time = 0; time < TestConcurrentHashMap::test_loop; time++)
    {
        size_t test_concurrency = 8;
        using ConcurrentMap = ConcurrentHashMap<UInt64, MapType, HashCRC32<UInt64>>;
        ConcurrentMap map(test_concurrency);
        ThreadPool insert_pool(test_concurrency);
        for (size_t i = 0; i < test_concurrency; i++)
        {
            insert_pool.scheduleOrThrowOnError([&] {
                for (size_t insert_value = 0; insert_value < 10000; insert_value++)
                {
                    size_t segment_index = 0;
                    if (!map.isZero(insert_value))
                    {
                        size_t hash_value = map.hash(insert_value);
                        segment_index = hash_value % test_concurrency;
                    }
                    bool inserted;
                    std::lock_guard lk(map.getSegmentMutex(segment_index));
                    typename ConcurrentMap::SegmentType::HashTable::LookupResult it;
                    map.getSegmentTable(segment_index).emplace(insert_value, it, inserted);
                    it->getMapped().value++;
                }
            });
        }
        insert_pool.wait();
        for (size_t insert_value = 0; insert_value < 10000; insert_value++)
        {
            size_t segment_index = 0;
            if (!map.isZero(insert_value))
            {
                size_t hash_value = map.hash(insert_value);
                segment_index = hash_value % test_concurrency;
            }
            auto & sub_map = map.getSegmentTable(segment_index);
            ASSERT_EQ(sub_map.has(insert_value), true);
            typename ConcurrentMap::SegmentType::HashTable::LookupResult it = sub_map.find(insert_value);
            ASSERT_EQ(it->getMapped().value.load(), (int)test_concurrency);
        }
    }
}

TEST(TestConcurrentHashMap, ConcurrentRandomInsert)
{
    for (size_t time = 0; time < TestConcurrentHashMap::test_loop; time++)
    {
        size_t test_concurrency = 8;
        using ConcurrentMap = ConcurrentHashMap<UInt64, MapType, HashCRC32<UInt64>>;
        using Map = std::unordered_map<UInt64, Int64>;
        ConcurrentMap concurrent_map(test_concurrency);
        std::vector<Map> maps;
        maps.resize(test_concurrency);
        ThreadPool insert_pool(test_concurrency);
        for (size_t i = 0; i < test_concurrency; i++)
        {
            insert_pool.scheduleOrThrowOnError([&, i] {
                std::default_random_engine e;
                e.seed(std::chrono::system_clock::now().time_since_epoch().count());
                std::uniform_int_distribution<unsigned> u(0, 100);
                for (size_t insert_time = 0; insert_time < 10000; insert_time++)
                {
                    typename ConcurrentMap::SegmentType::IteratorWithLock it;
                    bool inserted;
                    UInt64 insert_value = u(e);
                    concurrent_map.emplace(insert_value, it, inserted);
                    it.first->getMapped().value++;
                    if (maps[i].count(insert_value) > 0)
                    {
                        maps[i][insert_value] = maps[i][insert_value] + 1;
                    }
                    else
                    {
                        maps[i].insert({insert_value, 1});
                    }
                }
            });
        }
        insert_pool.wait();
        Map final_map = maps[0];
        /// merge all the maps
        for (size_t i = 1; i < test_concurrency; i++)
        {
            Map current_map = maps[i];
            for (auto & it : current_map)
            {
                if (final_map.count(it.first))
                {
                    final_map[it.first] = final_map[it.first] + it.second;
                }
                else
                {
                    final_map.insert({it.first, it.second});
                }
            }
        }
        ASSERT_EQ(final_map.size(), concurrent_map.rowCount());
        for (auto it = final_map.begin(); it != final_map.end(); it++)
        {
            ASSERT_EQ(concurrent_map.has(it->first), true);
            typename ConcurrentMap::SegmentType::IteratorWithLock concurrent_it = concurrent_map.find(it->first);
            ASSERT_EQ(concurrent_it.first->getMapped().value.load(), it->second);
        }
    }
}

TEST(TestConcurrentHashMap, ConcurrentRandomInsertWithExplicitLock)
{
    for (size_t time = 0; time < TestConcurrentHashMap::test_loop; time++)
    {
        size_t test_concurrency = 8;
        using ConcurrentMap = ConcurrentHashMap<UInt64, MapType, HashCRC32<UInt64>>;
        using Map = std::unordered_map<UInt64, Int64>;
        ConcurrentMap concurrent_map(test_concurrency);
        std::vector<Map> maps;
        maps.resize(test_concurrency);
        ThreadPool insert_pool(test_concurrency);
        for (size_t i = 0; i < test_concurrency; i++)
        {
            insert_pool.scheduleOrThrowOnError([&, i] {
                std::default_random_engine e;
                e.seed(std::chrono::system_clock::now().time_since_epoch().count());
                std::uniform_int_distribution<unsigned> u(0, 100);
                for (size_t insert_time = 0; insert_time < 10000; insert_time++)
                {
                    UInt64 insert_value = u(e);
                    size_t segment_index = 0;
                    if (!concurrent_map.isZero(insert_value))
                    {
                        size_t hash_value = concurrent_map.hash(insert_value);
                        segment_index = hash_value % test_concurrency;
                    }
                    bool inserted;
                    {
                        std::lock_guard lk(concurrent_map.getSegmentMutex(segment_index));
                        typename ConcurrentMap::SegmentType::HashTable::LookupResult it;
                        concurrent_map.getSegmentTable(segment_index).emplace(insert_value, it, inserted);
                        it->getMapped().value++;
                    }
                    if (maps[i].count(insert_value) > 0)
                    {
                        maps[i][insert_value] = maps[i][insert_value] + 1;
                    }
                    else
                    {
                        maps[i].insert({insert_value, 1});
                    }
                }
            });
        }
        insert_pool.wait();
        Map final_map = maps[0];
        /// merge all the maps
        for (size_t i = 1; i < test_concurrency; i++)
        {
            Map current_map = maps[i];
            for (auto & it : current_map)
            {
                if (final_map.count(it.first))
                {
                    final_map[it.first] += it.second;
                }
                else
                {
                    final_map.insert({it.first, it.second});
                }
            }
        }
        ASSERT_EQ(final_map.size(), concurrent_map.rowCount());
        for (auto it = final_map.begin(); it != final_map.end(); it++)
        {
            auto insert_value = it->first;
            size_t segment_index = 0;
            if (!concurrent_map.isZero(insert_value))
            {
                size_t hash_value = concurrent_map.hash(insert_value);
                segment_index = hash_value % test_concurrency;
            }
            auto & sub_map = concurrent_map.getSegmentTable(segment_index);
            ASSERT_EQ(sub_map.has(insert_value), true);
            typename ConcurrentMap::SegmentType::HashTable::LookupResult concurrent_it = sub_map.find(insert_value);
            ASSERT_EQ(concurrent_it->getMapped().value.load(), it->second);
        }
    }
}
} // namespace tests
} // namespace DB
