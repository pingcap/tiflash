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

#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class MockStorageTest : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "test_table_1"}, {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "r_table"}, {{"r_a", TiDB::TP::TypeLong}, {"r_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "l_table"}, {{"l_a", TiDB::TP::TypeLong}, {"l_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addExchangeRelationSchema("sender_1", {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
    }
};

TEST_F(MockStorageTest, MockTable)
try
{
    auto request = context.scan("test_db", "test_table").build(context);
    {
        String expected = "table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }


    request = context.scan("test_db", "test_table_1").build(context);
    {
        String expected = "table_scan_0 | {<0, Long>, <1, String>, <2, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
}
CATCH

} // namespace tests
} // namespace DB
