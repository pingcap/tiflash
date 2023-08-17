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


#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class CoalesceTest : public DB::tests::FunctionTest
{
};

TEST_F(CoalesceTest, testOnlyNull)
try
{
    const String & func_name = "coalesce";

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"a"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"a"}),
            createConstColumn<Nullable<String>>(1, std::nullopt)));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"a"}),
        executeFunction(func_name, createColumn<Nullable<String>>({"a"}), createOnlyNullColumnConst(1)));
}
CATCH
} // namespace DB::tests
