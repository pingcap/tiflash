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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsDuration.h>
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
// TODO: rewrite using executeFunction()
class TestDurationExtract : public DB::tests::FunctionTest
{
};

TEST_F(TestDurationExtract, ExtractFromMyDuration)
try
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> units{
        "hour",
        "minute",
        "second",
        "microsecond",
        "second_microsecond",
        "minute_microsecond",
        "minute_second",
        "hour_microsecond",
        "hour_second",
        "hour_minute",
    };
    MyDuration duration_value(1, 838, 34, 56, 123456, 6);
    std::vector<Int64> results{838, 34, 56, 123456, 56123456, 3456123456, 3456, 8383456123456, 8383456, 83834};

    for (size_t i = 0; i < units.size(); ++i)
    {
        const auto & unit = units[i];
        Block block;

        MutableColumnPtr col_units = ColumnString::create();
        col_units->insert(Field(unit.c_str(), unit.size()));
        col_units = ColumnConst::create(col_units->getPtr(), 1);

        auto col_duration = ColumnInt64::create();
        col_duration->insert(Field(duration_value.nanoSecond()));
        ColumnWithTypeAndName unit_ctn = ColumnWithTypeAndName(std::move(col_units), std::make_shared<DataTypeString>(), "unit");
        ColumnWithTypeAndName duration_ctn
            = ColumnWithTypeAndName(std::move(col_duration), std::make_shared<DataTypeMyDuration>(), "duration_value");

        block.insert(unit_ctn);
        block.insert(duration_ctn);
        // for result from extract
        block.insert({});

        // test extract
        auto func_builder_ptr = factory.tryGet("extractMyDuration", context);
        ASSERT_TRUE(func_builder_ptr != nullptr);

        ColumnNumbers arg_cols_idx{0, 1};
        size_t res_col_idx = 2;
        func_builder_ptr->build({unit_ctn, duration_ctn})->execute(block, arg_cols_idx, res_col_idx);
        const IColumn * ctn_res = block.getByPosition(res_col_idx).column.get();
        const auto * col_res = checkAndGetColumn<ColumnInt64>(ctn_res);

        Field res_field;
        col_res->get(0, res_field);
        Int64 s = res_field.get<Int64>();
        EXPECT_EQ(results[i], s);
    }
}
CATCH

} // namespace tests
} // namespace DB
