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

#include <TestUtils/MPPTaskTestUtils.h>

namespace DB
{
namespace tests
{
class CancelTaskRunner : public DB::tests::MPPTaskTestUtils
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        /// for agg
        context.addMockTable(
            {"test_db", "test_table_1"},
            {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", {1, {}, 10000000, 10000000}), toNullableVec<String>("s2", {"apple", {}, "banana", "test"}), toNullableVec<String>("s3", {"apple", {}, "banana", "test"})});

        /// for join
        context.addMockTable(
            {"test_db", "l_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", {}, "banana"}), toNullableVec<String>("join_c", {"apple", {}, "banana"})});
        context.addMockTable(
            {"test_db", "r_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", {}, "banana"}), toNullableVec<String>("join_c", {"apple", {}, "banana"})});
    }
};

TEST_F(CancelTaskRunner, runAggTasks)
try
{
    startServers(4);
    {
        injectCancel(context
                         .scan("test_db", "test_table_1")
                         .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                         .project({"max(s1)"}));
        MockComputeServerManager::instance().cancelTest();
    }
}
CATCH

} // namespace tests
} // namespace DB
