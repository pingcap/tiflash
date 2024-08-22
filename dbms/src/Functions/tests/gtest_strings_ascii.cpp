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

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
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
class StringASCII : public DB::tests::FunctionTest
{
};

// test string and string
TEST_F(StringASCII, strAndStrTest)
{
    const auto context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"hello", "HELLO", "23333", "#%@#^", ""};

    for (int i = 0; i < 2; i++)
    {
        MutableColumnPtr csp;
        csp = ColumnString::create();

        for (const auto & str : strs)
        {
            csp->insert(Field(str.c_str(), str.size()));
        }

        Block test_block;
        ColumnWithTypeAndName ctn
            = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeString>(), "test_ascii");
        ColumnsWithTypeAndName ctns{ctn};
        test_block.insert(ctn);
        ColumnNumbers cns{0};

        // test ascii
        auto bp = factory.tryGet("ascii", *context);
        ASSERT_TRUE(bp != nullptr);
        ASSERT_FALSE(bp->isVariadic());

        auto func = bp->build(ctns);
        test_block.insert({nullptr, func->getReturnType(), "res"});
        func->execute(test_block, cns, 1);
        const IColumn * res = test_block.getByPosition(1).column.get();
        const auto * actual_res = checkAndGetColumn<ColumnInt64>(res);

        Field res_field;
        std::vector<Int64> expect_results{104, 72, 50, 35, 0};
        for (size_t t = 0; t < expect_results.size(); t++)
        {
            actual_res->get(t, res_field);
            Int64 res_val = res_field.get<Int64>();
            EXPECT_EQ(expect_results[t], res_val);
        }
    }
}

// test NULL
TEST_F(StringASCII, nullTest)
{
    const auto context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"a", "b", "c", "d", "e", "f"};
    std::vector<Int64> results{0, 98, 0, 100, 101, 0};
    std::vector<int> null_map{1, 0, 1, 0, 0, 1};
    auto input_str_col = ColumnString::create();
    for (const auto & str : strs)
    {
        Field field(str.c_str(), str.size());
        input_str_col->insert(field);
    }

    auto input_null_map = ColumnUInt8::create(strs.size(), 0);
    ColumnUInt8::Container & input_vec_null_map = input_null_map->getData();
    for (size_t i = 0; i < strs.size(); i++)
    {
        input_vec_null_map[i] = null_map[i];
    }

    auto input_null_col = ColumnNullable::create(std::move(input_str_col), std::move(input_null_map));
    DataTypePtr string_type = std::make_shared<DataTypeString>();
    DataTypePtr nullable_string_type = makeNullable(string_type);

    auto col1 = ColumnWithTypeAndName(std::move(input_null_col), nullable_string_type, "ascii");
    ColumnsWithTypeAndName ctns{col1};

    Block test_block;
    test_block.insert(col1);
    ColumnNumbers cns{0};

    auto bp = factory.tryGet("ascii", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_FALSE(bp->isVariadic());
    auto func = bp->build(ctns);
    test_block.insert({nullptr, func->getReturnType(), "res"});
    func->execute(test_block, cns, 1);
    auto res_col = test_block.getByPosition(1).column;

    ColumnPtr result_null_map_column = static_cast<const ColumnNullable &>(*res_col).getNullMapColumnPtr();
    MutableColumnPtr mutable_result_null_map_column = (*std::move(result_null_map_column)).mutate();
    NullMap & result_null_map = static_cast<ColumnUInt8 &>(*mutable_result_null_map_column).getData();
    const IColumn * res = test_block.getByPosition(1).column.get();
    const auto * res_nullable_string = checkAndGetColumn<ColumnNullable>(res);
    const IColumn & res_string = res_nullable_string->getNestedColumn();

    Field res_field;

    for (size_t i = 0; i < null_map.size(); i++)
    {
        EXPECT_EQ(result_null_map[i], null_map[i]);
        if (result_null_map[i] == 0)
        {
            res_string.get(i, res_field);
            Int64 res_val = res_field.get<Int64>();
            EXPECT_EQ(results[i], res_val);
        }
    }
}

} // namespace tests
} // namespace DB
