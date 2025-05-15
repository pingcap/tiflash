// Copyright 2025 PingCAP, Inc.
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

#include <Interpreters/JoinV2/SemiJoinProbeList.h>
#include <TestUtils/FunctionTestUtils.h>
#include <cstddef>
#include <random>


namespace DB
{
namespace tests
{

class SemiJoinProbeListTest : public ::testing::Test
{
};

TEST_F(SemiJoinProbeListTest, TestRandom)
try
{
    // The KeyGetter type is unrelated to the functionality being tested.
    SemiJoinProbeList<HashJoinKeyGetterForType<HashJoinKeyMethod::OneKey32>> list;
    std::random_device rd;
    std::mt19937 g(rd());
    std::uniform_int_distribution dist;

    size_t n = dist(g) % 1000 + 1000;

    list.reset(n);
    EXPECT_EQ(list.slotCapacity(), n);
    EXPECT_EQ(list.activeSlots(), 0);
    std::unordered_set<size_t> s1, s2;
    for (size_t i = 0; i < n; ++i)
        s1.insert(i);
    while (!s1.empty() && !s2.empty())
    {
        EXPECT_EQ(list.activeSlots(), s1.size());
        bool is_append = !s1.empty() && dist(g) % 2 == 0; 
        if (is_append)
        {
            size_t append_idx = *s1.begin();
            EXPECT_TRUE(!list.contains(append_idx));
            s1.erase(append_idx);
            s2.insert(append_idx);
            list.append(append_idx);
            continue;
        }
        size_t remove_idx = *s2.begin();
        EXPECT_TRUE(list.contains(remove_idx));
        s2.erase(remove_idx);
        list.remove(remove_idx);
    }
    EXPECT_EQ(list.slotCapacity(), n);
    EXPECT_EQ(list.activeSlots(), 0);
}
CATCH

} // namespace tests
} // namespace DB