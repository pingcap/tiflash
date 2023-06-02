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

#include <Flash/Mpp/MPPTaskManager.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>


namespace DB
{
namespace tests
{
class TestCancelledMPPGatherCache : public testing::Test
{
};

TEST_F(TestCancelledMPPGatherCache, TestEvict)
try
{
    size_t capacity = 5;
    CancelledMPPGatherCache cache(capacity);
    for (size_t i = 0; i < capacity; i++)
    {
        cache.put(MPPGatherId(i, MPPQueryId(1, 2, 3, 4)));
    }
    for (size_t i = 0; i < capacity; i++)
    {
        ASSERT_EQ(cache.exists(MPPGatherId(i, MPPQueryId(1, 2, 3, 4))), true);
    }
    for (size_t i = 0; i < capacity; i++)
    {
        cache.put(MPPGatherId(0, MPPQueryId(1, 2, 3, 4)));
    }
    for (size_t i = 0; i < capacity; i++)
    {
        ASSERT_EQ(cache.exists(MPPGatherId(i, MPPQueryId(1, 2, 3, 4))), true);
    }
    cache.put(MPPGatherId(capacity, MPPQueryId(1, 2, 3, 4)));
    ASSERT_EQ(cache.exists(MPPGatherId(0, MPPQueryId(1, 2, 3, 4))), false);
    for (size_t i = 0; i < capacity; i++)
    {
        ASSERT_EQ(cache.exists(MPPGatherId(i + 1, MPPQueryId(1, 2, 3, 4))), true);
    }
}
CATCH

} // namespace tests
} // namespace DB
