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
#include <Functions/FunctionsDateTime.h>
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
class TestDateTimeExtract : public DB::tests::FunctionTest
{
};

// Disabled for now, since we haven't supported ExtractFromString yet
TEST_F(TestDateTimeExtract, DISABLED_ExtractFromString)
try
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> units{
        "year",
        "quarter",
        "month",
        "week",
        "day",
        "day_microsecond",
        "day_second",
        "day_minute",
        "day_hour",
        "year_month",
    };
    String datetime_value{"2021/1/29 12:34:56.123456"};
    std::vector<Int64> results{2021, 1, 1, 4, 29, 29123456123456, 29123456, 291234, 2912, 202101};

    for (size_t i = 0; i < units.size(); ++i)
    {
        const auto & unit = units[i];
        Block block;

        MutableColumnPtr col_units = ColumnString::create();
        col_units->insert(Field(unit.c_str(), unit.size()));
        col_units = ColumnConst::create(col_units->getPtr(), 1);

        auto col_datetime = ColumnString::create();
        col_datetime->insert(Field(datetime_value.data(), datetime_value.size()));
        ColumnWithTypeAndName unit_ctn = ColumnWithTypeAndName(std::move(col_units), std::make_shared<DataTypeString>(), "unit");
        ColumnWithTypeAndName datetime_ctn
            = ColumnWithTypeAndName(std::move(col_datetime), std::make_shared<DataTypeString>(), "datetime_value");
        block.insert(unit_ctn);
        block.insert(datetime_ctn);
        // for result from extract
        block.insert({});

        // test extract
        auto func_builder_ptr = factory.tryGet("extractMyDateTime", context);
        ASSERT_TRUE(func_builder_ptr != nullptr);

        ColumnNumbers arg_cols_idx{0, 1};
        size_t res_col_idx = 2;
        func_builder_ptr->build({unit_ctn, datetime_ctn})->execute(block, arg_cols_idx, res_col_idx);
        const IColumn * ctn_res = block.getByPosition(res_col_idx).column.get();
        const ColumnInt64 * col_res = checkAndGetColumn<ColumnInt64>(ctn_res);

        Field res_field;
        col_res->get(0, res_field);
        Int64 s = res_field.get<Int64>();
        EXPECT_EQ(results[i], s);
    }
}
CATCH

TEST_F(TestDateTimeExtract, ExtractFromMyDateTime)
try
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> units{
        "year",
        "quarter",
        "month",
        "week",
        "day",
        "day_microsecond",
        "day_second",
        "day_minute",
        "day_hour",
        "year_month",
    };
    MyDateTime datetime_value(2021, 1, 29, 12, 34, 56, 123456);
    std::vector<Int64> results{2021, 1, 1, 4, 29, 29123456123456, 29123456, 291234, 2912, 202101};

    for (size_t i = 0; i < units.size(); ++i)
    {
        const auto & unit = units[i];
        Block block;

        MutableColumnPtr col_units = ColumnString::create();
        col_units->insert(Field(unit.c_str(), unit.size()));
        col_units = ColumnConst::create(col_units->getPtr(), 1);

        auto col_datetime = ColumnUInt64::create();
        col_datetime->insert(Field(datetime_value.toPackedUInt()));
        ColumnWithTypeAndName unit_ctn = ColumnWithTypeAndName(std::move(col_units), std::make_shared<DataTypeString>(), "unit");
        ColumnWithTypeAndName datetime_ctn
            = ColumnWithTypeAndName(std::move(col_datetime), std::make_shared<DataTypeMyDateTime>(), "datetime_value");

        block.insert(unit_ctn);
        block.insert(datetime_ctn);
        // for result from extract
        block.insert({});

        // test extract
        auto func_builder_ptr = factory.tryGet("extractMyDateTime", context);
        ASSERT_TRUE(func_builder_ptr != nullptr);

        ColumnNumbers arg_cols_idx{0, 1};
        size_t res_col_idx = 2;
        func_builder_ptr->build({unit_ctn, datetime_ctn})->execute(block, arg_cols_idx, res_col_idx);
        const IColumn * ctn_res = block.getByPosition(res_col_idx).column.get();
        const ColumnInt64 * col_res = checkAndGetColumn<ColumnInt64>(ctn_res);

        Field res_field;
        col_res->get(0, res_field);
        Int64 s = res_field.get<Int64>();
        EXPECT_EQ(results[i], s);
    }
}
CATCH

} // namespace tests
} // namespace DB
