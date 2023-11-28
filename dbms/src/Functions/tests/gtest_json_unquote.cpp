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
#include <Functions/FunctionsJson.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/JsonScanner.h>

#include <string>
#include <vector>

namespace DB::tests
{
class TestJsonUnquote : public DB::tests::FunctionTest
{
public:
    ColumnWithTypeAndName executeJsonUnquoteFunction(
        Context & context,
        const ColumnWithTypeAndName & input_column,
        bool valid_check)
    {
        auto & factory = FunctionFactory::instance();
        ColumnsWithTypeAndName columns({input_column});
        ColumnNumbers argument_column_numbers;
        for (size_t i = 0; i < columns.size(); ++i)
            argument_column_numbers.push_back(i);

        ColumnsWithTypeAndName arguments;
        for (const auto argument_column_number : argument_column_numbers)
            arguments.push_back(columns.at(argument_column_number));

        const String func_name = "json_unquote";
        auto builder = factory.tryGet(func_name, context);
        if (!builder)
            throw TiFlashTestException(fmt::format("Function {} not found!", func_name));
        auto func = builder->build(arguments, nullptr);
        auto * function_build_ptr = builder.get();
        if (auto * default_function_builder = dynamic_cast<DefaultFunctionBuilder *>(function_build_ptr);
            default_function_builder)
        {
            auto * function_impl = default_function_builder->getFunctionImpl().get();
            if (auto * function_json_unquote = dynamic_cast<FunctionJsonUnquote *>(function_impl);
                function_json_unquote)
            {
                function_json_unquote->setNeedValidCheck(valid_check);
            }
            else
            {
                throw TiFlashTestException(fmt::format("Function {} not found!", func_name));
            }
        }

        Block block(columns);
        block.insert({nullptr, func->getReturnType(), "res"});
        func->execute(block, argument_column_numbers, columns.size());

        return block.getByPosition(columns.size());
    }
};

TEST_F(TestJsonUnquote, TestAll)
try
{
    /// Normal case: ColumnVector(nullable)
    const String func_name = "json_unquote";
    static auto const nullable_string_type_ptr = makeNullable(std::make_shared<DataTypeString>());
    static auto const string_type_ptr = std::make_shared<DataTypeString>();
    String bj2("\"hello, \\\"你好, \\u554A world, null, true]\"");
    String bj4("[[0, 1], [2, 3], [4, [5, 6]]]");
    auto input_col = createColumn<Nullable<String>>({bj2, {}, bj4});
    auto output_col
        = createColumn<Nullable<String>>({"hello, \"你好, 啊 world, null, true]", {}, "[[0, 1], [2, 3], [4, [5, 6]]]"});
    auto res = executeFunction(func_name, input_col);
    ASSERT_COLUMN_EQ(res, output_col);

    /// Normal case: ColumnVector(null)
    input_col = createColumn<Nullable<String>>({{}, {}, {}});
    output_col = createColumn<Nullable<String>>({{}, {}, {}});
    res = executeFunction(func_name, input_col);
    ASSERT_COLUMN_EQ(res, output_col);

    /// ColumnConst(null)
    auto null_input_col = createConstColumn<Nullable<String>>(3, {});
    output_col = createConstColumn<Nullable<String>>(3, {});
    res = executeFunction(func_name, null_input_col);
    ASSERT_COLUMN_EQ(res, output_col);

    /// ColumnVector(non-null)
    auto non_null_input_col = createColumn<String>({bj2, bj2, bj4});
    res = executeFunction(func_name, non_null_input_col);
    output_col = createColumn<Nullable<String>>(
        {"hello, \"你好, 啊 world, null, true]",
         "hello, \"你好, 啊 world, null, true]",
         "[[0, 1], [2, 3], [4, [5, 6]]]"});
    ASSERT_COLUMN_EQ(res, output_col);

    /// ColumnConst(non-null)
    auto const_non_null_input_col = createConstColumn<String>(3, bj2);
    res = executeFunction(func_name, const_non_null_input_col);
    output_col = createConstColumn<Nullable<String>>(3, {"hello, \"你好, 啊 world, null, true]"});
    ASSERT_COLUMN_EQ(res, output_col);

    /// ColumnConst(nullable)
    auto const_nullable_input_col = createConstColumn<Nullable<String>>(3, {bj2});
    res = executeFunction(func_name, const_nullable_input_col);
    output_col = createConstColumn<Nullable<String>>(3, {"hello, \"你好, 啊 world, null, true]"});
    ASSERT_COLUMN_EQ(res, output_col);
}
CATCH

TEST_F(TestJsonUnquote, TestCheckValid)
try
{
    /// Normal case: ColumnVector(nullable)
    const String func_name = "json_unquote";
    static auto const nullable_string_type_ptr = makeNullable(std::make_shared<DataTypeString>());
    static auto const string_type_ptr = std::make_shared<DataTypeString>();
    String bj2("\"hello, \\\"你好, \\u554A world, null, true]\"");
    String bj4("[[0, 1], [2, 3], [4, [5, 6]]]");
    auto input_col = createColumn<Nullable<String>>({bj2, {}, bj4});
    auto output_col
        = createColumn<Nullable<String>>({"hello, \"你好, 啊 world, null, true]", {}, "[[0, 1], [2, 3], [4, [5, 6]]]"});
    auto res = executeJsonUnquoteFunction(*context, input_col, false);
    ASSERT_COLUMN_EQ(res, output_col);

    res = executeJsonUnquoteFunction(*context, input_col, true);
    ASSERT_COLUMN_EQ(res, output_col);

    try
    {
        String bj3(R"("hello world \ ")");
        input_col = createColumn<Nullable<String>>({bj3});
        res = executeJsonUnquoteFunction(*context, input_col, true);
        GTEST_FAIL();
    }
    catch (Exception & e)
    {}
}
CATCH

TEST_F(TestJsonUnquote, TestCheckValidRaw)
try
{
    // Valid Literals
    std::vector<String> valid_literals = {R"(true)", R"(false)", R"(null)"};
    for (const auto & str : valid_literals)
    {
        ASSERT_TRUE(checkJsonValid(str.c_str(), str.size()));
    }

    // Invalid Literals
    std::vector<String> invalid_literals
        = {R"(tRue)", R"(tru)", R"(trued)", R"(False)", R"(fale)", R"(falses)", R"(nulL)", R"(nul)", R"(nulll)"};
    for (const auto & str : invalid_literals)
    {
        ASSERT_TRUE(!checkJsonValid(str.c_str(), str.size()));
    }

    // Valid Numbers
    std::vector<String> valid_numbers = {R"(3)", R"(-100)", R"(231.0123)", R"(3.14e0)", R"(-3.14e-1)", R"(3.14e100)"};
    for (const auto & str : valid_numbers)
    {
        ASSERT_TRUE(checkJsonValid(str.c_str(), str.size()));
    }

    // Invalid Numbers
    std::vector<String> invalid_numbers
        = {R"(3.3.3)", R"(3.3t)", R"(--100)", R"(e231.0123)", R"(+3.14e)", R"(-+3.23)", R"(-+341a)"};
    for (const auto & str : invalid_numbers)
    {
        ASSERT_TRUE(!checkJsonValid(str.c_str(), str.size()));
    }

    // Valid Strings
    std::vector<String> valid_strings
        = {R"("foo")",
           R"("hello world!\n")",
           R"("hello \"name\"")",
           R"("你好 朋友")",
           R"("\u554A world")",
           R"("{\"foo\":\"bar\",\"bar\":{\"baz\":[\"qux\"]}}")",
           R"("\"hello world\"")"};
    for (const auto & str : valid_strings)
    {
        ASSERT_TRUE(checkJsonValid(str.c_str(), str.size()));
    }

    // Invalid Strings
    std::vector<String> invalid_strings
        = {R"(""hello world"")", R"("hello world"ef)", R"("hello world)", R"("hello world \ ")"};
    for (const auto & str : invalid_strings)
    {
        ASSERT_TRUE(!checkJsonValid(str.c_str(), str.size()));
    }

    // Valid Objects
    std::vector<String> valid_objects
        = {R"({})",
           R"({"a":3.0, "b":-4, "c":"hello world", "d":true})",
           R"({"a":3.0, "b":{"c":{"d":"hello world"}}})",
           R"({"a":3.0, "b":{"name":"Tom", "experience":{"current":10, "last":30}}, "c":"hello world", "d":true})"};
    for (const auto & str : valid_objects)
    {
        ASSERT_TRUE(checkJsonValid(str.c_str(), str.size()));
    }

    // Invalid Objects
    std::vector<String> invalid_objects
        = {R"({"a")",
           R"({"a"})",
           R"({"a":})",
           R"({32:"a"})",
           R"({"a":32:})",
           R"({"a":32}})",
           R"({"a":32,:})",
           R"({"a":32,"dd":{"d","e"}})"};
    for (const auto & str : invalid_objects)
    {
        ASSERT_TRUE(!checkJsonValid(str.c_str(), str.size()));
    }

    // Valid Arrays
    std::vector<String> valid_arrays
        = {R"([])",
           R"([true, null, false,  "hello world", 3.0, -1e4])",
           R"([1,2,[ 3, "hello", ["world", 32]]])",
           R"([[],[]])"};
    for (const auto & str : valid_arrays)
    {
        ASSERT_TRUE(checkJsonValid(str.c_str(), str.size()));
    }

    // Invalid Arrays
    std::vector<String> invalid_arrays
        = {R"([32)", R"([32]])", R"([32,])", R"([[],23)", R"([32], 23)", R"(["hello", ["world"])"};
    for (const auto & str : invalid_arrays)
    {
        ASSERT_TRUE(!checkJsonValid(str.c_str(), str.size()));
    }

    // Valid Mixtures
    std::vector<String> valid_mixtures
        = {R"([{}])",
           R"([3, {"name":3}, {"experince":[3,6,9]}])",
           R"({"age":3, "value":[-32, true, {"exr":[23,-12,true]}]})"};
    for (const auto & str : valid_mixtures)
    {
        ASSERT_TRUE(checkJsonValid(str.c_str(), str.size()));
    }

    // Invalid Mixtures
    std::vector<String> invalid_mixtures
        = {R"({[]})",
           R"({"name":"tome", [3,2]})",
           R"([{"name":"tome"})",
           R"([{"name":"tome"])",
           R"([{"name":"tome", "age":[}])"};
    for (const auto & str : invalid_mixtures)
    {
        ASSERT_TRUE(!checkJsonValid(str.c_str(), str.size()));
    }
}
CATCH

} // namespace DB::tests