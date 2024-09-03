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
class StringLength : public DB::tests::FunctionTest
{
};

TEST_F(StringLength, length)
{
<<<<<<< HEAD
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"hi~", "23333", "pingcap", "你好", "233哈哈", ""};
    std::vector<Int64> results{3, 5, 7, 6, 9, 0};

    for (int i = 0; i < 2; i++)
=======
>>>>>>> b30c1f5090 (Improve the performance of `length` and `ascii` functions (#9345))
    {
        // test const
        ASSERT_COLUMN_EQ(createConstColumn<Int64>(0, 0), executeFunction("length", createConstColumn<String>(0, "")));
        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(1, 3),
            executeFunction("length", createConstColumn<String>(1, "aaa")));
        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(3, 3),
            executeFunction("length", createConstColumn<String>(3, "aaa")));
    }

    {
        // test vec
        ASSERT_COLUMN_EQ(createColumn<Int64>({}), executeFunction("length", createColumn<String>({})));

<<<<<<< HEAD
        Block test_block;
        ColumnWithTypeAndName ctn = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeString>(), "test_ascii");
        ColumnsWithTypeAndName ctns{ctn};
        test_block.insert(ctn);
        ColumnNumbers cns{0};

        // test length
        auto bp = factory.tryGet("length", context);
        ASSERT_TRUE(bp != nullptr);
        ASSERT_FALSE(bp->isVariadic());
=======
        ASSERT_COLUMN_EQ(
            createColumn<Int64>({0, 3, 5, 7, 6, 9, 0, 9, 16, 0}),
            executeFunction(
                "length",
                createColumn<String>(
                    {"", "hi~", "23333", "pingcap", "你好", "233哈哈", "", "asdの的", "ヽ(￣▽￣)و", ""})));
    }

    {
        // test nullable const
        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(0, {}),
            executeFunction("length", createConstColumn<Nullable<String>>(0, "aaa")));
        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(1, {3}),
            executeFunction("length", createConstColumn<Nullable<String>>(1, "aaa")));
        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(3, {3}),
            executeFunction("length", createConstColumn<Nullable<String>>(3, "aaa")));
    }
>>>>>>> b30c1f5090 (Improve the performance of `length` and `ascii` functions (#9345))

    {
        // test nullable vec
        std::vector<Int32> null_map{1, 0, 1, 0, 0, 1};
        ASSERT_COLUMN_EQ(
            createNullableColumn<Int64>({0, 4, 0, 6, 6, 0}, null_map),
            executeFunction(
                "length",
                createNullableColumn<String>({"a", "abcd", "嗯", "饼干", "馒头", "?？?"}, null_map)));
    }
}
<<<<<<< HEAD

// test NULL
TEST_F(StringLength, nullTest)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"a", "abcd", "嗯", "饼干", "馒头", "?？?"};
    std::vector<Int64> results{0, 4, 0, 6, 6, 0};
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

    auto col1 = ColumnWithTypeAndName(std::move(input_null_col), nullable_string_type, "length");
    ColumnsWithTypeAndName ctns{col1};

    Block test_block;
    test_block.insert(col1);
    ColumnNumbers cns{0};

    auto bp = factory.tryGet("length", context);
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
    const ColumnNullable * res_nullable_string = checkAndGetColumn<ColumnNullable>(res);
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

=======
>>>>>>> b30c1f5090 (Improve the performance of `length` and `ascii` functions (#9345))
} // namespace tests
} // namespace DB
