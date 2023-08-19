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

} // namespace tests
} // namespace DB
