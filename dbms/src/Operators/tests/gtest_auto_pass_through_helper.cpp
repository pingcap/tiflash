// Copyright 2024 PingCAP, Inc.
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

#include <Core/ColumnWithTypeAndName.h>
#include <Interpreters/Context.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Operators/AutoPassThroughHashAggHelper.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/ColumnGenerator.h>

#include <gtest/gtest.h>

namespace DB
{
namespace tests
{

class TestAutoPassThroughHelper : public ::testing::Test
{
public:
    void SetUp() override
    {
        ::DB::registerAggregateFunctions();
    }
};

TEST_F(TestAutoPassThroughHelper, basic)
try
{
    // select sum(col_uint8),              -> test sum func, upgrade uint8 to uint64
    //        sum(col_decimal64),          -> test sum func, upgrade decimal64 to decimal128
    //        sum(nullable_col_uint8),
    //        sum(nullable_col_decimal64),
    //        count(col_uint8),            -> test count func
    //        count(nullable_col_uint8),
    //        first_row(col_string),       -> test first_row
    //        first_row(nullable_col_string)
    //        from t group by col_test;
    auto data_type_uint8 = std::make_shared<DataTypeUInt8>();
    auto data_type_decimal64 = std::make_shared<DataTypeDecimal<Decimal64>>(15, 2);
    auto data_type_str = std::make_shared<DataTypeString>();
    auto nullable_data_type_uint8 = std::make_shared<DataTypeNullable>(data_type_uint8);
    auto nullable_data_type_decimal64 = std::make_shared<DataTypeNullable>(data_type_decimal64);
    auto nullable_data_type_str = std::make_shared<DataTypeNullable>(data_type_str);

    ColumnsWithTypeAndName child_header{
        {data_type_uint8, "col_uint8"},
        {data_type_decimal64, "col_decimal64"},

        {nullable_data_type_uint8, "nullable_col_uint8"},
        {nullable_data_type_decimal64, "nullable_col_decimal64"},

        {data_type_str, "col_str"},
        {nullable_data_type_str, "nullable_col_str"},
    };
    auto context = TiFlashTestEnv::getContext();
    AggregateDescriptions agg_descs{
        {
            .function = AggregateFunctionFactory::instance().get(*context, "sum", {data_type_uint8}, {}, 0, false),
            .parameters = {},
            .arguments = {0},
            .argument_names = {"col_uint8"},
            .column_name = "sum(col_uint8)"
        },
        {
            .function = AggregateFunctionFactory::instance().get(*context, "sum", {data_type_decimal64}, {}, 0, false),
            .parameters = {},
            .arguments = {1},
            .argument_names = {"col_decimal64"},
            .column_name = "sum(col_decimal64)"
        },
        {
            .function = AggregateFunctionFactory::instance().get(*context, "sum", {nullable_data_type_uint8}, {}, 0, false),
            .parameters = {},
            .arguments = {2},
            .argument_names = {"nullable_col_uint8"},
            .column_name = "sum(nullable_col_uint8)"
        },
        {
            .function = AggregateFunctionFactory::instance().get(*context, "sum", {nullable_data_type_decimal64}, {}, 0, false),
            .parameters = {},
            .arguments = {3},
            .argument_names = {"nullable_col_decimal64"},
            .column_name = "sum(nullable_col_decimal64)"
        },
        {
            .function = AggregateFunctionFactory::instance().get(*context, "count", {data_type_uint8}, {}, 0, false),
            .parameters = {},
            .arguments = {0},
            .argument_names = {"col_uint8"},
            .column_name = "count(col_uint8)"
        },
        {
            .function = AggregateFunctionFactory::instance().get(*context, "count", {data_type_decimal64}, {}, 0, false),
            .parameters = {},
            .arguments = {1},
            .argument_names = {"col_decimal64"},
            .column_name = "count(col_decimal64)"
        },
        {
            .function = AggregateFunctionFactory::instance().get(*context, "first_row", {data_type_str}, {}, 0, false),
            .parameters = {},
            .arguments = {4},
            .argument_names = {"col_str"},
            .column_name = "count(col_str)"
        },
        {
            .function = AggregateFunctionFactory::instance().get(*context, "first_row", {nullable_data_type_str}, {}, 0, false),
            .parameters = {},
            .arguments = {5},
            .argument_names = {"nullable_col_str"},
            .column_name = "count(nullable_col_str)"
        },
    };

    ColumnsWithTypeAndName header;
    header.reserve(agg_descs.size());
    for (const auto & desc : agg_descs)
    {
        header.push_back(ColumnWithTypeAndName(desc.function->getReturnType(), desc.column_name));
    }

    auto generators = setUpAutoPassThroughColumnGenerator(Block(header), Block(child_header), agg_descs);

    const size_t block_size = 8192;
    Block child_block;
    for (const auto & column : child_header)
    {
        auto col = ColumnGenerator::instance().generate({block_size, column.type->getName(), RANDOM, column.name});
        child_block.insert(col);
    }
    for (const auto & generator : generators)
    {
        ASSERT_TRUE(generator != nullptr);
        auto res_col = generator(child_block);
        ASSERT_TRUE(res_col != nullptr);
    }
}
CATCH

} // namespace tests
} // namespace DB
