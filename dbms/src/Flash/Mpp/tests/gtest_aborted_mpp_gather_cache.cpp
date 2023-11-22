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

#include <Flash/Mpp/MPPTaskManager.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>


namespace DB
{
namespace tests
{
class TestAbortedMPPGatherCache : public testing::Test
{
};

TEST_F(TestAbortedMPPGatherCache, TestEvict)
try
{
    size_t capacity = 5;
    AbortedMPPGatherCache cache(capacity);
    for (size_t i = 0; i < capacity; i++)
    {
        cache.add(MPPGatherId(i, MPPQueryId(1, 2, 3, 4, /*resource_group_name=*/"", 5, "")), "");
    }
    for (size_t i = 0; i < capacity; i++)
    {
        ASSERT_EQ(
            !cache.check(MPPGatherId(i, MPPQueryId(1, 2, 3, 4, /*resource_group_name=*/"", 5, ""))).empty(),
            true);
    }
    for (size_t i = 0; i < capacity; i++)
    {
        cache.add(MPPGatherId(0, MPPQueryId(1, 2, 3, 4, /*resource_group_name=*/"", 5, "")), "");
    }
    for (size_t i = 0; i < capacity; i++)
    {
        ASSERT_EQ(
            !cache.check(MPPGatherId(i, MPPQueryId(1, 2, 3, 4, /*resource_group_name=*/"", 5, ""))).empty(),
            true);
    }
    cache.add(MPPGatherId(capacity, MPPQueryId(1, 2, 3, 4, /*resource_group_name=*/"", 5, "")), "");
    ASSERT_EQ(!cache.check(MPPGatherId(0, MPPQueryId(1, 2, 3, 4, /*resource_group_name=*/"", 5, ""))).empty(), false);
    for (size_t i = 0; i < capacity; i++)
    {
        ASSERT_EQ(
            !cache.check(MPPGatherId(i + 1, MPPQueryId(1, 2, 3, 4, /*resource_group_name=*/"", 5, ""))).empty(),
            true);
    }
}
CATCH

} // namespace tests
} // namespace DB
