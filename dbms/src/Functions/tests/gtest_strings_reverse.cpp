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

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <Poco/Types.h>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{
class StringReverse : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "reverse";

protected:
    ColumnWithTypeAndName toVec(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }
};

// test string and string
TEST_F(StringReverse, strAndStrTest)
try
{
    ASSERT_COLUMN_EQ(
        toVec({"olleh", "dlrow,olleh", "", "pacgnip.www", "。。。试.测.文中"}),
        executeFunction(
            func_name,
            toVec({"hello", "hello,world", "", "www.pingcap", "中文.测.试。。。"}), ));
}
CATCH

// test NULL
TEST_F(StringLength, nullTest)
{
    ASSERT_COLUMN_EQ(
        toVec({"", {}}),
        executeFunction(
            func_name,
            toVec({"", {}}), ));
}

} // namespace tests
} // namespace DB
