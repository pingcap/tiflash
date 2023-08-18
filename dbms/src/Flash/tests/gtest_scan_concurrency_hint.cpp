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

#include <TestUtils/ExecutorTestUtils.h>

#include <ext/enumerate.h>
#include <tuple>

namespace DB
{
namespace tests
{
class ScanConcurrencyHintTest : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        enablePipeline(false);
    }
};

TEST_F(ScanConcurrencyHintTest, InvalidHint)
try
{
    context.addMockTable(
        "simple_test",
        "t1",
        {{"a", TiDB::TP::TypeString}, {"b", TiDB::TP::TypeString}},
        {toNullableVec<String>("a", {"1", "2", {}, "1", {}}), toNullableVec<String>("b", {"3", "4", "3", {}, {}})},
        0);
    auto request = context.scan("simple_test", "t1").build(context);
    {
        /// the scan concurrency hint is invalid, the final stream concurrency is the original concurrency
        String expected = R"(
Union: <for test>
 Expression x 10: <final projection>
  MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }
}
CATCH

TEST_F(ScanConcurrencyHintTest, ValidHint)
try
{
    context.addMockTable(
        "simple_test",
        "t1",
        {{"a", TiDB::TP::TypeString}, {"b", TiDB::TP::TypeString}},
        {toNullableVec<String>("a", {"1", "2", {}, "1", {}}), toNullableVec<String>("b", {"3", "4", "3", {}, {}})},
        3);
    auto request = context.scan("simple_test", "t1").build(context);
    {
        /// case 1, concurrency < scan concurrency hint, the final stream concurrency is the original concurrency
        String expected = R"(
Expression: <final projection>
 MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 1);
    }
    {
        /// case 2, concurrency = scan concurrency hint, the final stream concurrency is the original concurrency
        String expected = R"(
Union: <for test>
 Expression x 3: <final projection>
  MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 3);
    }
    {
        /// case 3, concurrency > scan concurrency hint, the final stream concurrency is the scan concurrency hint
        String expected = R"(
Union: <for test>
 Expression x 3: <final projection>
  MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }
}
CATCH

} // namespace tests
} // namespace DB
