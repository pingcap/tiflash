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

#include <TestUtils/TiFlashTestBasic.h>

#include <TiDB/Decode/JsonPathExpr.cpp>

namespace DB
{
namespace tests
{
class TestJsonPathExpr : public ::testing::Test
{
};

TEST_F(TestJsonPathExpr, TestContainsAnyAsterisk)
try
{
    std::vector<String> input_paths{"$.a[1]", "$.a[*]", "$.*[1]", "$**.a[1]"};
    std::vector<bool> expect_result{false, true, true, true};
    for (size_t i = 0; i < input_paths.size(); ++i)
    {
        auto res = JsonPathExpr::parseJsonPathExpr(input_paths[i]);
        ASSERT_TRUE(res);
        ASSERT_TRUE(JsonPathExpr::containsAnyAsterisk(res->getFlag()) == expect_result[i]);
    }
}
CATCH

TEST_F(TestJsonPathExpr, TestValidatePathExpr)
try
{
    struct TestData
    {
        String expression;
        bool success;
        size_t legs;
    };
    std::vector<TestData> test_data_vec{
        {"   $  ", true, 0},
        {"   $ .   key1  [  3  ]\t[*].*.key3", true, 5},
        {"   $ .   key1  [  3  ]**[*].*.key3", true, 6},
        {"$.\"key1 string\"[  3  ][*].*.key3", true, 5},
        {"$.\"hello \\\"escaped quotes\\\" world\\n\"[3][*].*.key3", true, 5},
        {"$[1 to 5]", true, 1},
        {"$[2 to 1]", false, 1},
        {"$[last]", true, 1},
        {"$[1 to last]", true, 1},
        {"$[1to3]", false, 1},
        {"$[last - 5 to last - 10]", false, 1},
        {"$.\\\"escaped quotes\\\"[3][*].*.key3", false, 0},
        {"$.hello \\\"escaped quotes\\\" world[3][*].*.key3", false, 0},
        {"$NoValidLegsHere", false, 0},
        {"$        No Valid Legs Here .a.b.c", false, 0},
        {"$.a[b]", false, 0},
        {"$.*[b]", false, 0},
        {"$**.a[b]", false, 0},
        {"$.b[ 1 ].", false, 0},
        {"$.performance.txn-entry-size-limit", false, 0},
        {"$.\"performance\".txn-entry-size-limit", false, 0},
        {"$.\"performance.\"txn-entry-size-limit", false, 0},
        {"$.\"performance.\"txn-entry-size-limit\"", false, 0},
        {"$[", false, 0},
        {"$a.***[3]", false, 0},
        {"$1a", false, 0},
    };
    for (size_t i = 0; i < test_data_vec.size(); ++i)
    {
        auto res = JsonPathExpr::parseJsonPathExpr(test_data_vec[i].expression);
        ASSERT_TRUE(!res == !test_data_vec[i].success);
        if (res)
            ASSERT_TRUE(res->getLegs().size() == test_data_vec[i].legs);
    }
}
CATCH

} // namespace tests
} // namespace DB
