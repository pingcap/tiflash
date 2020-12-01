#include <Common/HashTable/HashMap.h>
#include <common/ThreadPool.h>
#include <gtest/gtest.h>

#include <ext/singleton.h>

namespace DB
{
namespace tests
{

class TestConcurrentHashMap : public ext::singleton<TestConcurrentHashMap>
{
};

TEST(TestConcurrentHashMap, ConcurrentInsert)
{
    struct MapType
    {
        std::atomic_int value;
        MapType() { value.store(0); }
    };
    size_t test_concurrency = 8;
    using ConcurrentMap = ConcurrentHashMap<UInt64, MapType, HashCRC32<UInt64>>;
    ConcurrentMap map(test_concurrency);
    ThreadPool insert_pool(test_concurrency);
    for (size_t i = 0; i < test_concurrency; i++)
    {
        insert_pool.schedule([&] {
            for (size_t insert_value = 0; insert_value < 10000; insert_value++)
            {
                typename ConcurrentMap::SegmentType::HashTable::iterator it;
                bool inserted;
                map.emplace(insert_value, it, inserted);
                it->second.value++;
            }
        });
    }
    insert_pool.wait();
    for (size_t insert_value = 0; insert_value < 10000; insert_value++)
    {
        typename ConcurrentMap::SegmentType::HashTable::iterator it = map.find(insert_value);
        ASSERT_EQ(it->second.value.load(), (int)test_concurrency);
    }
}
} // namespace tests
} // namespace DB
