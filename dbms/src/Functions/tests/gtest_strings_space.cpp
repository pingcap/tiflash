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

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>


namespace DB::tests
{
class StringSpace : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "space";

protected:
    static ColumnWithTypeAndName toVec(const std::vector<String> & v) { return createColumn<String>(v); }
    static ColumnWithTypeAndName toNullableVec(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }

    static ColumnWithTypeAndName toVecInt(const std::vector<std::optional<Int64>> & v)
    {
        return createColumn<Nullable<Int64>>(v);
    }

    static ColumnWithTypeAndName toConst(const Int64 & v) { return createConstColumn<Int64>(8, v); }
};

// test space
TEST_F(StringSpace, spaceTest)
try
{
    ASSERT_COLUMN_EQ(
        toNullableVec({"  ", "", "          ", "", "      "}),
        executeFunction(func_name, toVecInt({2, 0, 10, -1, 6})));
}
CATCH

// test space NULL
TEST_F(StringSpace, nullTest)
try
{
    ASSERT_COLUMN_EQ(toNullableVec({{}, "     "}), executeFunction(func_name, toVecInt({{}, 5})));

    ASSERT_COLUMN_EQ(toNullableVec({{}}), executeFunction(func_name, toVecInt({16777217})));
}
CATCH

} // namespace DB::tests
