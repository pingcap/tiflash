// Copyright 2024 PingCAP, Inc.
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

#include <Storages/DeltaMerge/File/ColumnCacheLongTerm.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{


TEST(VectorIndexColumnCacheTest, Evict)
try
{
    size_t cache_hit = 0;
    size_t cache_miss = 0;

    auto cache = ColumnCacheLongTerm(150);
    cache.getStats(cache_hit, cache_miss);
    ASSERT_EQ(cache_hit, 0);
    ASSERT_EQ(cache_miss, 0);

    auto col = cache.get("/", 1, 2, [] {
        // key=40, value=40
        auto data = genSequence<Int64>("[0, 5)");
        auto col = ::DB::tests::createColumn<Int64>(data, "", 0).column;
        return col;
    });
    ASSERT_EQ(col->size(), 5);
    cache.getStats(cache_hit, cache_miss);
    ASSERT_EQ(cache_hit, 0);
    ASSERT_EQ(cache_miss, 1);

    col = cache.get("/", 1, 2, [] {
        // key=40, value=40
        auto data = genSequence<Int64>("[0, 5)");
        return ::DB::tests::createColumn<Int64>(data, "", 0).column;
    });
    ASSERT_EQ(col->size(), 5);
    cache.getStats(cache_hit, cache_miss);
    ASSERT_EQ(cache_hit, 1);
    ASSERT_EQ(cache_miss, 1);

    col = cache.get("/", 1, 3, [] {
        // key=40, value=400
        auto data = genSequence<Int64>("[0, 100)");
        return ::DB::tests::createColumn<Int64>(data, "", 0).column;
    });
    ASSERT_EQ(col->size(), 100);
    cache.getStats(cache_hit, cache_miss);
    ASSERT_EQ(cache_hit, 1);
    ASSERT_EQ(cache_miss, 2);

    col = cache.get("/", 1, 2, [] {
        // key=40, value=40
        auto data = genSequence<Int64>("[0, 5)");
        return ::DB::tests::createColumn<Int64>(data, "", 0).column;
    });
    ASSERT_EQ(col->size(), 5);
    cache.getStats(cache_hit, cache_miss);
    ASSERT_EQ(cache_hit, 1);
    ASSERT_EQ(cache_miss, 3);

    col = cache.get("/", 1, 4, [] {
        // key=40, value=8
        auto data = genSequence<Int64>("[0, 1)");
        return ::DB::tests::createColumn<Int64>(data, "", 0).column;
    });
    ASSERT_EQ(col->size(), 1);
    cache.getStats(cache_hit, cache_miss);
    ASSERT_EQ(cache_hit, 1);
    ASSERT_EQ(cache_miss, 4);

    col = cache.get("/", 1, 2, [] {
        // key=40, value=40
        auto data = genSequence<Int64>("[0, 5)");
        return ::DB::tests::createColumn<Int64>(data, "", 0).column;
    });
    ASSERT_EQ(col->size(), 5);
    cache.getStats(cache_hit, cache_miss);
    ASSERT_EQ(cache_hit, 2);
    ASSERT_EQ(cache_miss, 4);
}
CATCH


} // namespace DB::DM::tests
