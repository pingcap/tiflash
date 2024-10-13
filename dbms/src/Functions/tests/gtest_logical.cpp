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

#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Types.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/types.h>

namespace DB::tests
{
class Logical : public DB::tests::FunctionTest
{
protected:
    void SetUp() override
    {
        FunctionTest::SetUp();
        ColumnGeneratorOpts not_null_opts{rows, "UInt8", DataDistribution::RANDOM};
        not_null_opts.gen_bool = true;
        ColumnGeneratorOpts nullable_opts{rows, "Nullable(UInt8)", DataDistribution::RANDOM};
        nullable_opts.gen_bool = true;
        for (size_t i = 0; i < uint8_column_num; ++i)
        {
            not_null_uint8_columns.push_back(ColumnGenerator::instance().generate(not_null_opts));
            nullable_uint8_columns.push_back(ColumnGenerator::instance().generate(nullable_opts));
        }
    }
    void testBinaryLogicalOP(
        const String & func_name,
        const ColumnWithTypeAndName & result,
        const ColumnWithTypeAndName & col1,
        const ColumnWithTypeAndName & col2)
    {
        ASSERT_COLUMN_EQ(result, executeFunction(func_name, col1, col2));
    }
    void testGenericLogicalOP(const String & func_name, const ColumnsWithTypeAndName & inputs, bool with_constants)
    {
        if (inputs.size() <= 1)
        {
            throw Exception("there must be at least 2 input columns");
        }
        if (with_constants)
        {
            auto new_inputs = inputs;
            // add constant_true
            new_inputs.push_back(not_null_true_const_long);
            testGenericLogicalOP(func_name, new_inputs, false);
            // add constant_false
            new_inputs = inputs;
            new_inputs.push_back(not_null_false_const_long);
            testGenericLogicalOP(func_name, new_inputs, false);
            // add constant_null
            new_inputs = inputs;
            new_inputs.push_back(nullable_null_const_long);
            testGenericLogicalOP(func_name, new_inputs, false);
            // multiple constant
            new_inputs = inputs;
            new_inputs.push_back(nullable_null_const_long);
            new_inputs.push_back(not_null_false_const_long);
            testGenericLogicalOP(func_name, new_inputs, false);
            new_inputs = inputs;
            new_inputs.push_back(nullable_null_const_long);
            new_inputs.push_back(not_null_true_const_long);
            testGenericLogicalOP(func_name, new_inputs, false);
            new_inputs = inputs;
            new_inputs.push_back(nullable_null_const_long);
            new_inputs.push_back(not_null_true_const_long);
            new_inputs.push_back(not_null_false_const_long);
            testGenericLogicalOP(func_name, new_inputs, false);
            new_inputs = inputs;
            new_inputs.push_back(not_null_true_const_long);
            new_inputs.push_back(not_null_false_const_long);
            testGenericLogicalOP(func_name, new_inputs, false);
            new_inputs = inputs;
            new_inputs.push_back(not_null_true_const_long);
            new_inputs.push_back(not_null_true_const_long);
            new_inputs.push_back(not_null_false_const_long);
            testGenericLogicalOP(func_name, new_inputs, false);
            new_inputs = inputs;
            new_inputs.push_back(not_null_false_const_long);
            new_inputs.push_back(not_null_true_const_long);
            new_inputs.push_back(not_null_false_const_long);
            testGenericLogicalOP(func_name, new_inputs, false);
        }
        else
        {
        auto res_1 = executeFunction(func_name, inputs);
        auto res_2 = executeFunction(func_name, inputs[0], inputs[1]);
        for (size_t i = 2; i < inputs.size(); ++i)
        {
            res_2 = executeFunction(func_name, res_2, inputs[i]);
        }
        ASSERT_COLUMN_EQ(res_1, res_2);
        }
    }

    ColumnWithTypeAndName not_null_false_column = createColumn<UInt8>({0, 0});
    ColumnWithTypeAndName not_null_true_column = createColumn<UInt8>({1, 1});
    ColumnWithTypeAndName nullable_false_column = createColumn<Nullable<UInt8>>({0, 0});
    ColumnWithTypeAndName nullable_true_column = createColumn<Nullable<UInt8>>({1, 1});
    ColumnWithTypeAndName nullable_null_column = createColumn<Nullable<UInt8>>({{}, {}});
    ColumnWithTypeAndName not_null_false_const = createConstColumn<UInt8>(2, 0);
    ColumnWithTypeAndName not_null_true_const = createConstColumn<UInt8>(2, 1);
    ColumnWithTypeAndName nullable_false_const = createConstColumn<Nullable<UInt8>>(2, 0);
    ColumnWithTypeAndName nullable_true_const = createConstColumn<Nullable<UInt8>>(2, 1);
    ColumnWithTypeAndName nullable_null_const = createConstColumn<Nullable<UInt8>>(2, {});
    static const size_t rows = 1024;
    static const size_t uint8_column_num = 8;
    std::mt19937_64 rand_gen;
    ColumnsWithTypeAndName not_null_uint8_columns;
    ColumnsWithTypeAndName nullable_uint8_columns;
    ColumnWithTypeAndName not_null_false_const_long = createConstColumn<UInt8>(rows, 0);
    ColumnWithTypeAndName not_null_true_const_long = createConstColumn<UInt8>(rows, 1);
    ColumnWithTypeAndName nullable_false_const_long = createConstColumn<Nullable<UInt8>>(rows, 0);
    ColumnWithTypeAndName nullable_true_const_long = createConstColumn<Nullable<UInt8>>(rows, 1);
    ColumnWithTypeAndName nullable_null_const_long = createConstColumn<Nullable<UInt8>>(rows, {});
};

TEST_F(Logical, binaryAndTest)
try
{
    const String & name = "and";
    // basic tests
    // false && false
    testBinaryLogicalOP(name, not_null_false_column, not_null_false_column, not_null_false_column);
    testBinaryLogicalOP(name, not_null_false_const, not_null_false_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, not_null_false_column);
    testBinaryLogicalOP(name, nullable_false_const, nullable_false_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_false_column, not_null_false_column, nullable_false_column);
    // nullable_false_constant will be converted to not_null_false_constant
    testBinaryLogicalOP(name, not_null_false_const, not_null_false_column, nullable_false_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, nullable_false_column);
    testBinaryLogicalOP(name, nullable_false_const, nullable_false_column, nullable_false_const);
    testBinaryLogicalOP(name, not_null_false_const, not_null_false_const, not_null_false_const);
    // false && true
    testBinaryLogicalOP(name, not_null_false_column, not_null_false_column, not_null_true_column);
    testBinaryLogicalOP(name, not_null_false_column, not_null_false_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, not_null_true_column);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_false_column, not_null_false_column, nullable_true_column);
    testBinaryLogicalOP(name, not_null_false_column, not_null_false_column, nullable_true_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, nullable_true_column);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, nullable_true_const);
    testBinaryLogicalOP(name, not_null_false_const, nullable_false_const, nullable_true_const);
    // false && null
    testBinaryLogicalOP(name, nullable_false_column, not_null_false_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_false_column, not_null_false_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_false_const, nullable_false_const, nullable_null_const);
    // true && false
    testBinaryLogicalOP(name, not_null_false_column, not_null_true_column, not_null_false_column);
    testBinaryLogicalOP(name, not_null_false_const, not_null_true_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_true_column, not_null_false_column);
    testBinaryLogicalOP(name, nullable_false_const, nullable_true_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_false_column, not_null_true_column, nullable_false_column);
    testBinaryLogicalOP(name, not_null_false_const, not_null_true_column, nullable_false_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_true_column, nullable_false_column);
    testBinaryLogicalOP(name, nullable_false_const, nullable_true_column, nullable_false_const);
    testBinaryLogicalOP(name, not_null_false_const, nullable_true_const, nullable_false_const);
    // true && true
    testBinaryLogicalOP(name, not_null_true_column, not_null_true_column, not_null_true_column);
    testBinaryLogicalOP(name, not_null_true_column, not_null_true_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, not_null_true_column);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_true_column, not_null_true_column, nullable_true_column);
    testBinaryLogicalOP(name, not_null_true_column, not_null_true_column, nullable_true_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, nullable_true_column);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, nullable_true_const);
    testBinaryLogicalOP(name, not_null_true_const, nullable_true_const, nullable_true_const);
    // true && null
    testBinaryLogicalOP(name, nullable_null_column, not_null_true_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_null_column, not_null_true_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_null_column, nullable_true_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_null_column, nullable_true_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_null_const, nullable_true_const, nullable_null_const);
    // null && true
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, not_null_true_column);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, nullable_true_column);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, nullable_true_const);
    testBinaryLogicalOP(name, nullable_null_const, nullable_null_const, nullable_true_const);
    // null && false
    testBinaryLogicalOP(name, nullable_false_column, nullable_null_column, not_null_false_column);
    testBinaryLogicalOP(name, nullable_false_const, nullable_null_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_null_column, nullable_false_column);
    testBinaryLogicalOP(name, nullable_false_const, nullable_null_column, nullable_false_const);
    testBinaryLogicalOP(name, nullable_false_const, nullable_null_const, nullable_false_const);
    // null && null
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_null_const, nullable_null_const, nullable_null_const);

    // column, column
    testBinaryLogicalOP(
        name,
        createColumn<Nullable<UInt8>>({0, 1, 0, 0, {}, 0}),
        createColumn<Nullable<UInt8>>({0, 1, 0, 1, {}, 0}),
        createColumn<Nullable<UInt8>>({0, 1, 1, 0, 1, {}}));
    // column, const
    testBinaryLogicalOP(
        name,
        createColumn<Nullable<UInt8>>({1, 0}),
        createConstColumn<Nullable<UInt8>>(2, 1),
        createColumn<Nullable<UInt8>>({1, 0}));
    testBinaryLogicalOP(
        name,
        createConstColumn<Nullable<UInt8>>(2, 0),
        createConstColumn<Nullable<UInt8>>(2, 0),
        createColumn<Nullable<UInt8>>({1, 0}));
    // const, const
    testBinaryLogicalOP(
        name,
        createConstColumn<UInt8>(1, 1),
        createConstColumn<Nullable<UInt8>>(1, 1),
        createConstColumn<Nullable<UInt8>>(1, 1));
    // only null
    testBinaryLogicalOP(
        name,
        createColumn<Nullable<UInt8>>({{}, 0}),
        createOnlyNullColumnConst(2),
        createColumn<Nullable<UInt8>>({1, 0}));
    // issue 6127
    testBinaryLogicalOP(
        name,
        createColumn<UInt8>({0, 1, 0, 0}),
        createColumn<Int64>({0, 123, 0, 41}),
        createColumn<UInt8>({0, 11, 221, 0}));
    // issue 6127, position of UInt8 column may affect the result
    testBinaryLogicalOP(
        name,
        createColumn<UInt8>({0, 1, 0, 0}),
        createColumn<UInt8>({0, 123, 0, 41}),
        createColumn<Int64>({0, 11, 221, 0}));
}
CATCH

TEST_F(Logical, AndTest)
try
{
    const String & name = "and";
    ColumnsWithTypeAndName input_columns;
    for (size_t i = 3; i < uint8_column_num; i++)
    {
        for (size_t j = 0; j < i; ++j)
        {
            if (rand_gen() % 2 == 0)
                input_columns.push_back(not_null_uint8_columns[j]);
            else
                input_columns.push_back(nullable_uint8_columns[j]);
        }
        testGenericLogicalOP(name, input_columns, false);
        testGenericLogicalOP(name, input_columns, true);
        input_columns.clear();
    }
    for (size_t i = 3; i < uint8_column_num; i++)
    {
        for (size_t j = 0; j < i; ++j)
        {
            input_columns.push_back(not_null_uint8_columns[i]);
        }
        testGenericLogicalOP(name, input_columns, false);
        testGenericLogicalOP(name, input_columns, true);
        input_columns.clear();
    }
    for (size_t i = 3; i < uint8_column_num; i++)
    {
        for (size_t j = 0; j < i; ++j)
        {
            input_columns.push_back(nullable_uint8_columns[j]);
        }
        testGenericLogicalOP(name, input_columns, false);
        testGenericLogicalOP(name, input_columns, true);
        input_columns.clear();
    }
}
CATCH

TEST_F(Logical, OrTest)
try
{
    const String & name = "or";
    ColumnsWithTypeAndName input_columns;
    for (size_t i = 3; i < uint8_column_num; i++)
    {
        for (size_t j = 0; j < i; ++j)
        {
            if (rand_gen() % 2 == 0)
                input_columns.push_back(not_null_uint8_columns[j]);
            else
                input_columns.push_back(nullable_uint8_columns[j]);
        }
        testGenericLogicalOP(name, input_columns, false);
        testGenericLogicalOP(name, input_columns, true);
        input_columns.clear();
    }
    for (size_t i = 3; i < uint8_column_num; i++)
    {
        for (size_t j = 0; j < i; ++j)
        {
            input_columns.push_back(not_null_uint8_columns[i]);
        }
        testGenericLogicalOP(name, input_columns, false);
        testGenericLogicalOP(name, input_columns, true);
        input_columns.clear();
    }
    for (size_t i = 3; i < uint8_column_num; i++)
    {
        for (size_t j = 0; j < i; ++j)
        {
            input_columns.push_back(nullable_uint8_columns[j]);
        }
        testGenericLogicalOP(name, input_columns, false);
        testGenericLogicalOP(name, input_columns, true);
        input_columns.clear();
    }
}
CATCH

TEST_F(Logical, XorTest)
try
{
    const String & name = "xor";
    ColumnsWithTypeAndName input_columns;
    for (size_t i = 3; i < uint8_column_num; i++)
    {
        for (size_t j = 0; j < i; ++j)
        {
            if (rand_gen() % 2 == 0)
                input_columns.push_back(not_null_uint8_columns[j]);
            else
                input_columns.push_back(nullable_uint8_columns[j]);
        }
        testGenericLogicalOP(name, input_columns, false);
        testGenericLogicalOP(name, input_columns, true);
        input_columns.clear();
    }
    for (size_t i = 3; i < uint8_column_num; i++)
    {
        for (size_t j = 0; j < i; ++j)
        {
            input_columns.push_back(not_null_uint8_columns[i]);
        }
        testGenericLogicalOP(name, input_columns, false);
        testGenericLogicalOP(name, input_columns, true);
        input_columns.clear();
    }
    for (size_t i = 3; i < uint8_column_num; i++)
    {
        for (size_t j = 0; j < i; ++j)
        {
            input_columns.push_back(nullable_uint8_columns[j]);
        }
        testGenericLogicalOP(name, input_columns, false);
        testGenericLogicalOP(name, input_columns, true);
        input_columns.clear();
    }
}
CATCH

TEST_F(Logical, binaryOrTest)
try
{
    const String & name = "or";
    // basic tests
    // false || false
    testBinaryLogicalOP(name, not_null_false_column, not_null_false_column, not_null_false_column);
    testBinaryLogicalOP(name, not_null_false_column, not_null_false_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, not_null_false_column);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_false_column, not_null_false_column, nullable_false_column);
    // nullable_false_constant will be converted to not_null_false_constant
    testBinaryLogicalOP(name, not_null_false_column, not_null_false_column, nullable_false_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, nullable_false_column);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, nullable_false_const);
    testBinaryLogicalOP(name, not_null_false_const, nullable_false_const, nullable_false_const);
    // false || true
    testBinaryLogicalOP(name, not_null_true_column, not_null_false_column, not_null_true_column);
    testBinaryLogicalOP(name, not_null_true_const, not_null_false_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_false_column, not_null_true_column);
    testBinaryLogicalOP(name, nullable_true_const, nullable_false_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_true_column, not_null_false_column, nullable_true_column);
    testBinaryLogicalOP(name, not_null_true_const, not_null_false_column, nullable_true_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_false_column, nullable_true_column);
    testBinaryLogicalOP(name, nullable_true_const, nullable_false_column, nullable_true_const);
    testBinaryLogicalOP(name, not_null_true_const, nullable_false_const, nullable_true_const);
    // false || null
    testBinaryLogicalOP(name, nullable_null_column, not_null_false_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_null_column, not_null_false_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_null_column, nullable_false_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_null_column, nullable_false_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_null_const, nullable_false_const, nullable_null_const);
    // true || false
    testBinaryLogicalOP(name, not_null_true_column, not_null_true_column, not_null_false_column);
    testBinaryLogicalOP(name, not_null_true_column, not_null_true_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, not_null_false_column);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_true_column, not_null_true_column, nullable_false_column);
    testBinaryLogicalOP(name, not_null_true_column, not_null_true_column, nullable_false_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, nullable_false_column);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, nullable_false_const);
    testBinaryLogicalOP(name, not_null_true_const, nullable_true_const, nullable_false_const);
    // true || true
    testBinaryLogicalOP(name, not_null_true_column, not_null_true_column, not_null_true_column);
    testBinaryLogicalOP(name, not_null_true_const, not_null_true_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, not_null_true_column);
    testBinaryLogicalOP(name, nullable_true_const, nullable_true_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_true_column, not_null_true_column, nullable_true_column);
    testBinaryLogicalOP(name, not_null_true_const, not_null_true_column, nullable_true_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, nullable_true_column);
    testBinaryLogicalOP(name, nullable_true_const, nullable_true_column, nullable_true_const);
    testBinaryLogicalOP(name, not_null_true_const, nullable_true_const, nullable_true_const);
    // true || null
    testBinaryLogicalOP(name, nullable_true_column, not_null_true_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_true_column, not_null_true_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_true_const, nullable_true_const, nullable_null_const);
    // null || true
    testBinaryLogicalOP(name, nullable_true_column, nullable_null_column, not_null_true_column);
    testBinaryLogicalOP(name, nullable_true_const, nullable_null_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_null_column, nullable_true_column);
    testBinaryLogicalOP(name, nullable_true_const, nullable_null_column, nullable_true_const);
    testBinaryLogicalOP(name, nullable_true_const, nullable_null_const, nullable_true_const);
    // null || false
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, not_null_false_column);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, nullable_false_column);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, nullable_false_const);
    testBinaryLogicalOP(name, nullable_null_const, nullable_null_const, nullable_false_const);
    // null || null
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_null_const, nullable_null_const, nullable_null_const);

    // column, column
    testBinaryLogicalOP(
        name,
        createColumn<Nullable<UInt8>>({0, 1, 1, 1, 1, {}}),
        createColumn<Nullable<UInt8>>({0, 1, 0, 1, {}, 0}),
        createColumn<Nullable<UInt8>>({0, 1, 1, 0, 1, {}}));
    // column, const
    testBinaryLogicalOP(
        name,
        createConstColumn<Nullable<UInt8>>(2, 1),
        createConstColumn<Nullable<UInt8>>(2, 1),
        createColumn<Nullable<UInt8>>({1, 0}));
    // const, const
    testBinaryLogicalOP(
        name,
        createConstColumn<UInt8>(1, 1),
        createConstColumn<Nullable<UInt8>>(1, 1),
        createConstColumn<Nullable<UInt8>>(1, 0));
    // only null
    testBinaryLogicalOP(
        name,
        createColumn<Nullable<UInt8>>({1, {}}),
        createOnlyNullColumnConst(2),
        createColumn<Nullable<UInt8>>({1, 0}));
    // issue 5849
    testBinaryLogicalOP(
        name,
        createColumn<UInt8>({0, 1, 1, 1}),
        createColumn<UInt8>({0, 123, 0, 41}),
        createColumn<Int64>({0, 11, 221, 0}));
}
CATCH

TEST_F(Logical, binaryXorTest)
try
{
    const String & name = "xor";
    // basic tests
    // false xor false
    testBinaryLogicalOP(name, not_null_false_column, not_null_false_column, not_null_false_column);
    testBinaryLogicalOP(name, not_null_false_column, not_null_false_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, not_null_false_column);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_false_column, not_null_false_column, nullable_false_column);
    // nullable_false_constant will be converted to not_null_false_constant
    testBinaryLogicalOP(name, not_null_false_column, not_null_false_column, nullable_false_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, nullable_false_column);
    testBinaryLogicalOP(name, nullable_false_column, nullable_false_column, nullable_false_const);
    testBinaryLogicalOP(name, not_null_false_const, nullable_false_const, nullable_false_const);
    // false xor true
    testBinaryLogicalOP(name, not_null_true_column, not_null_false_column, not_null_true_column);
    testBinaryLogicalOP(name, not_null_true_column, not_null_false_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_false_column, not_null_true_column);
    testBinaryLogicalOP(name, nullable_true_column, nullable_false_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_true_column, not_null_false_column, nullable_true_column);
    testBinaryLogicalOP(name, not_null_true_column, not_null_false_column, nullable_true_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_false_column, nullable_true_column);
    testBinaryLogicalOP(name, nullable_true_column, nullable_false_column, nullable_true_const);
    testBinaryLogicalOP(name, not_null_true_const, nullable_false_const, nullable_true_const);
    // false xor null
    testBinaryLogicalOP(name, nullable_null_column, not_null_false_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_null_const, not_null_false_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_null_column, nullable_false_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_null_const, nullable_false_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_null_const, nullable_false_const, nullable_null_const);
    // true xor false
    testBinaryLogicalOP(name, not_null_true_column, not_null_true_column, not_null_false_column);
    testBinaryLogicalOP(name, not_null_true_column, not_null_true_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, not_null_false_column);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_true_column, not_null_true_column, nullable_false_column);
    testBinaryLogicalOP(name, not_null_true_column, not_null_true_column, nullable_false_const);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, nullable_false_column);
    testBinaryLogicalOP(name, nullable_true_column, nullable_true_column, nullable_false_const);
    testBinaryLogicalOP(name, not_null_true_const, nullable_true_const, nullable_false_const);
    // true xor true
    testBinaryLogicalOP(name, not_null_false_column, not_null_true_column, not_null_true_column);
    testBinaryLogicalOP(name, not_null_false_column, not_null_true_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_true_column, not_null_true_column);
    testBinaryLogicalOP(name, nullable_false_column, nullable_true_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_false_column, not_null_true_column, nullable_true_column);
    testBinaryLogicalOP(name, not_null_false_column, not_null_true_column, nullable_true_const);
    testBinaryLogicalOP(name, nullable_false_column, nullable_true_column, nullable_true_column);
    testBinaryLogicalOP(name, nullable_false_column, nullable_true_column, nullable_true_const);
    testBinaryLogicalOP(name, not_null_false_const, nullable_true_const, nullable_true_const);
    // true xor null
    testBinaryLogicalOP(name, nullable_null_column, not_null_true_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_null_const, not_null_true_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_null_column, nullable_true_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_null_const, nullable_true_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_null_const, nullable_true_const, nullable_null_const);
    // null xor true
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, not_null_true_column);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, not_null_true_const);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, nullable_true_column);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, nullable_true_const);
    testBinaryLogicalOP(name, nullable_null_const, nullable_null_const, nullable_true_const);
    // null xor false
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, not_null_false_column);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, not_null_false_const);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, nullable_false_column);
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, nullable_false_const);
    testBinaryLogicalOP(name, nullable_null_const, nullable_null_const, nullable_false_const);
    // null xor null
    testBinaryLogicalOP(name, nullable_null_column, nullable_null_column, nullable_null_column);
    testBinaryLogicalOP(name, nullable_null_const, nullable_null_column, nullable_null_const);
    testBinaryLogicalOP(name, nullable_null_const, nullable_null_const, nullable_null_const);

    // column, column
    testBinaryLogicalOP(
        name,
        createColumn<Nullable<UInt8>>({0, 0, 1, 1, {}, {}}),
        createColumn<Nullable<UInt8>>({0, 1, 0, 1, {}, 0}),
        createColumn<Nullable<UInt8>>({0, 1, 1, 0, 1, {}}));
    // column, const
    testBinaryLogicalOP(
        name,
        createColumn<Nullable<UInt8>>({0, 1}),
        createConstColumn<Nullable<UInt8>>(2, 1),
        createColumn<Nullable<UInt8>>({1, 0}));
    // const, const
    testBinaryLogicalOP(
        name,
        createConstColumn<UInt8>(1, 0),
        createConstColumn<Nullable<UInt8>>(1, 1),
        createConstColumn<Nullable<UInt8>>(1, 1));
    // only null
    testBinaryLogicalOP(
        name,
        createOnlyNullColumnConst(2),
        createOnlyNullColumnConst(2),
        createColumn<Nullable<UInt8>>({1, 0}));
}
CATCH

TEST_F(Logical, notTest)
try
{
    const String & func_name = "not";

    // column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({1, 0, {}}),
        executeFunction(func_name, createColumn<Nullable<UInt8>>({0, 1, {}})));
    // const
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(1, 0),
        executeFunction(func_name, createConstColumn<Nullable<UInt8>>(1, 1)));
    // only null
    ASSERT_COLUMN_EQ(createOnlyNullColumnConst(1), executeFunction(func_name, createOnlyNullColumnConst(1)));
}
CATCH

} // namespace DB::tests
