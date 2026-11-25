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

#include <Flash/Coprocessor/DAGContext.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
TEST(DAGContextTest, FlagsTest)
{
    DAGContext context(1024);

    ASSERT_EQ(context.getFlags(), static_cast<UInt64>(0));

    UInt64 f = TiDBSQLFlags::TRUNCATE_AS_WARNING | TiDBSQLFlags::IN_LOAD_DATA_STMT;
    context.setFlags(f);
    ASSERT_EQ(context.getFlags(), f);

    UInt64 f1 = f | TiDBSQLFlags::OVERFLOW_AS_WARNING;
    context.addFlag(TiDBSQLFlags::OVERFLOW_AS_WARNING);
    ASSERT_EQ(context.getFlags(), f1);

    context.delFlag(TiDBSQLFlags::OVERFLOW_AS_WARNING);
    ASSERT_EQ(context.getFlags(), f);
}

TEST(DAGContextTest, MergeWarningsPreservesTotalAcrossExchanges)
{
    DAGContext sender(2);
    sender.appendWarning("first", 1406);
    sender.appendWarning("second", 1406);
    sender.appendWarning("dropped by sender", 1406);
    tipb::SelectResponse first_hop;
    sender.fillWarnings(first_hop);
    ASSERT_EQ(first_hop.warnings_size(), 2);
    ASSERT_EQ(first_hop.warning_count(), 3);

    DAGContext receiver(2);
    receiver.appendWarning("local", 1406);
    receiver.mergeWarnings(first_hop);
    tipb::SelectResponse second_hop;
    receiver.fillWarnings(second_hop);
    ASSERT_EQ(second_hop.warnings_size(), 2);
    EXPECT_EQ(second_hop.warning_count(), 4);
    EXPECT_EQ(second_hop.warnings(0).msg(), "local");
    EXPECT_EQ(second_hop.warnings(1).msg(), "first");
}

} // namespace tests
} // namespace DB
