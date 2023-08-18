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

#include <Storages/DeltaMerge/Remote/RNLocalPageCache.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::DM::Remote::tests
{

class LocalPageCacheLRUTest : public ::testing::Test
{
};

TEST_F(LocalPageCacheLRUTest, Basic)
{
    RNLocalPageCacheLRU lru(5);
    lru.put("key_1", 3);
    lru.put("key_2", 10);
    ASSERT_EQ(13, lru.current_total_size);
    ASSERT_EQ(2, lru.index.size());
    ASSERT_EQ(lru.index.size(), lru.queue.size());
    {
        auto evicted = lru.evict();
        ASSERT_EQ(1, evicted.size());
        ASSERT_EQ("key_1", evicted[0]);
        ASSERT_EQ(10, lru.current_total_size);
        ASSERT_EQ(1, lru.index.size());
        ASSERT_EQ(lru.index.size(), lru.queue.size());
    }
    {
        auto evicted = lru.evict();
        ASSERT_EQ(0, evicted.size());
        ASSERT_EQ(10, lru.current_total_size);
        ASSERT_EQ(1, lru.index.size());
        ASSERT_EQ(lru.index.size(), lru.queue.size());
    }

    lru.put("key_3", 1);
    ASSERT_EQ(11, lru.current_total_size);
    ASSERT_EQ(2, lru.index.size());
    ASSERT_EQ(lru.index.size(), lru.queue.size());
    {
        auto evicted = lru.evict();
        ASSERT_EQ(1, evicted.size());
        ASSERT_EQ("key_2", evicted[0]);
        ASSERT_EQ(1, lru.current_total_size);
        ASSERT_EQ(1, lru.index.size());
        ASSERT_EQ(lru.index.size(), lru.queue.size());
    }

    lru.put("key_4", 2);
    lru.put("key_5", 1);
    ASSERT_EQ(4, lru.current_total_size);
    ASSERT_EQ(3, lru.index.size());
    ASSERT_EQ(lru.index.size(), lru.queue.size());
    {
        auto evicted = lru.evict();
        ASSERT_EQ(0, evicted.size());
        ASSERT_EQ(4, lru.current_total_size);
        ASSERT_EQ(3, lru.index.size());
        ASSERT_EQ(lru.index.size(), lru.queue.size());
    }

    lru.put("key_1", 10);
    ASSERT_EQ(14, lru.current_total_size);
    ASSERT_EQ(4, lru.index.size());
    ASSERT_EQ(lru.index.size(), lru.queue.size());
    {
        auto evicted = lru.evict();
        ASSERT_EQ(3, evicted.size());
        ASSERT_EQ("key_3", evicted[0]);
        ASSERT_EQ("key_4", evicted[1]);
        ASSERT_EQ("key_5", evicted[2]);
        ASSERT_EQ(10, lru.current_total_size);
        ASSERT_EQ(1, lru.index.size());
        ASSERT_EQ(lru.index.size(), lru.queue.size());
    }
}

TEST_F(LocalPageCacheLRUTest, PutAndRemove)
{
    RNLocalPageCacheLRU lru(5);
    lru.put("key_1", 1);
    lru.put("key_2", 4);
    lru.put("key_3", 3);
    ASSERT_EQ(8, lru.current_total_size);
    ASSERT_EQ(3, lru.index.size());
    ASSERT_EQ(lru.index.size(), lru.queue.size());

    lru.remove("key_2");
    ASSERT_EQ(4, lru.current_total_size);
    ASSERT_EQ(2, lru.index.size());
    ASSERT_EQ(lru.index.size(), lru.queue.size());

    {
        auto evicted = lru.evict();
        ASSERT_EQ(0, evicted.size());
        ASSERT_EQ(4, lru.current_total_size);
        ASSERT_EQ(2, lru.index.size());
        ASSERT_EQ(lru.index.size(), lru.queue.size());
    }

    lru.put("key_4", 3);
    lru.remove("key_not_exist");
    ASSERT_EQ(7, lru.current_total_size);
    ASSERT_EQ(3, lru.index.size());
    ASSERT_EQ(lru.index.size(), lru.queue.size());

    lru.put("key_3", 3); // refresh a key
    ASSERT_EQ(7, lru.current_total_size);
    ASSERT_EQ(3, lru.index.size());
    ASSERT_EQ(lru.index.size(), lru.queue.size());

    {
        auto evicted = lru.evict();
        ASSERT_EQ(2, evicted.size());
        ASSERT_EQ("key_1", evicted[0]);
        ASSERT_EQ("key_4", evicted[1]);
        ASSERT_EQ(3, lru.current_total_size);
        ASSERT_EQ(1, lru.index.size());
        ASSERT_EQ(lru.index.size(), lru.queue.size());
    }
}

TEST_F(LocalPageCacheLRUTest, PutDifferentSize)
{
    RNLocalPageCacheLRU lru(5);
    lru.put("key_1", 3);

    ASSERT_THROW({ lru.put("key_1", 1); }, DB::Exception);
}

} // namespace DB::DM::Remote::tests
