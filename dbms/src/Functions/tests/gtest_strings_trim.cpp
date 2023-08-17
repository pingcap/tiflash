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
class StringTrim : public DB::tests::FunctionTest
{
};


TEST_F(StringTrim, stringTrimStringUnitTest)
{
    const auto context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"};

    MutableColumnPtr csp = ColumnString::create();
    for (const auto & str : strs)
    {
        csp->insert(Field(str.c_str(), str.size()));
    }

    Block test_block;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeString>(), "test_trim");
    ColumnsWithTypeAndName ctns{ctn};
    test_block.insert(ctn);
    // for result from trim, ltrim and rtrim
    test_block.insert({});
    test_block.insert({});
    test_block.insert({});
    ColumnNumbers cns{0};

    // test trim
    auto bp = factory.tryGet("tidbTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 1);
    const IColumn * res = test_block.getByPosition(1).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field res_field;

    std::vector<String> results{"hello", "h e llo", "hello", "", "hello, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("tidbLTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 2);
    res = test_block.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"hello   ", "h e llo", "hello    ", "", "hello, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("tidbRTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 3);
    res = test_block.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"  hello", "   h e llo", "hello", "", "hello, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }
}


TEST_F(StringTrim, stringTrimConstUnitTest)
{
    const auto context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();
    MutableColumnPtr cp = ColumnString::create();
    cp->insert(Field("  hello   ", 10));

    ColumnPtr csp = ColumnConst::create(cp->getPtr(), 5);
    Block test_block;
    auto type = std::make_shared<DataTypeString>();

    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(csp, type, "test_trim_const");

    ColumnsWithTypeAndName ctns{ctn};
    test_block.insert(ctn);
    // for result from trim, ltrim and rtrim
    test_block.insert({});
    test_block.insert({});
    test_block.insert({});
    ColumnNumbers cns{0};

    // test trim
    auto bp = factory.tryGet("tidbTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 1);

    const IColumn * res = test_block.getByPosition(1).column.get();
    const ColumnConst * c0_string = checkAndGetColumn<ColumnConst>(res);


    Field res_field;

    std::vector<String> results{"hello", "hello", "hello", "hello", "hello"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("tidbLTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 2);
    res = test_block.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnConst>(res);

    results = {"hello   ", "hello   ", "hello   ", "hello   ", "hello   "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("tidbRTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 3);
    res = test_block.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnConst>(res);

    results = {
        "  hello",
        "  hello",
        "  hello",
        "  hello",
        "  hello",
    };
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

TEST_F(StringTrim, stringTrimwsConstUnitTest)
{
    const auto context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();
    MutableColumnPtr cp = ColumnString::create();
    cp->insert(Field("  hello   ", 10));
    MutableColumnPtr excp = ColumnString::create();
    excp->insert(Field("  he", 4));

    ColumnPtr csp = ColumnConst::create(cp->getPtr(), 5);
    ColumnPtr excsp = ColumnConst::create(excp->getPtr(), 5);
    Block test_block;

    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(csp, std::make_shared<DataTypeString>(), "test_trim_const");
    ColumnWithTypeAndName exctn
        = ColumnWithTypeAndName(excsp, std::make_shared<DataTypeString>(), "test_ex_trim_const");

    ColumnsWithTypeAndName ctns{ctn, exctn};
    test_block.insert(ctn);
    test_block.insert(exctn);
    // for result from trim, ltrim and rtrim
    test_block.insert({});
    test_block.insert({});
    test_block.insert({});
    ColumnNumbers cns{0, 1};

    // test trim
    auto bp = factory.tryGet("tidbTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 2);

    const IColumn * res = test_block.getByPosition(2).column.get();
    const ColumnConst * c0_string = checkAndGetColumn<ColumnConst>(res);


    Field res_field;

    std::vector<String> results{"llo   ", "llo   ", "llo   ", "llo   ", "llo   "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("tidbLTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 2);
    res = test_block.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnConst>(res);

    results = {"llo   ", "llo   ", "llo   ", "llo   ", "llo   "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("tidbRTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 3);
    res = test_block.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnConst>(res);

    results = {
        "  hello   ",
        "  hello   ",
        "  hello   ",
        "  hello   ",
        "  hello   ",
    };
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

TEST_F(StringTrim, stringTrimwsUtf8UnitTest)
{
    const auto context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"  你好   ", "   上海", "北京晨凯", "     ", "  你好, world  "};
    String trim = " ";

    MutableColumnPtr csp = ColumnString::create();
    for (const auto & str : strs)
    {
        csp->insert(Field(str.c_str(), str.size()));
    }
    MutableColumnPtr cp2 = ColumnString::create();
    cp2->insert(Field(trim.c_str(), trim.size()));
    ColumnPtr excsp = ColumnConst::create(cp2->getPtr(), 5);

    Block test_block;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeString>(), "test_trim");
    ColumnWithTypeAndName exctn = ColumnWithTypeAndName(excsp, std::make_shared<DataTypeString>(), "test_ex_trim");
    ColumnsWithTypeAndName ctns{ctn, exctn};
    test_block.insert(ctn);
    // for result from trim, ltrim and rtrim
    test_block.insert(exctn);
    test_block.insert({});
    ColumnNumbers cns{0, 1};

    // test trim
    auto bp = factory.tryGet("tidbTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 2);
    const IColumn * res = test_block.getByPosition(2).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field res_field;
    std::vector<String> results{"你好", "上海", "北京晨凯", "", "你好, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("tidbLTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 2);
    res = test_block.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"你好   ", "上海", "北京晨凯", "", "你好, world  "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("tidbRTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 2);
    res = test_block.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"  你好", "   上海", "北京晨凯", "", "  你好, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

TEST_F(StringTrim, stringTrimwsConstUtf8UnitTest)
{
    const auto context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    String str = "  你好   ";
    String trim = "  你";

    MutableColumnPtr cp = ColumnString::create();
    cp->insert(Field(str.c_str(), str.size()));
    ColumnPtr csp = ColumnConst::create(cp->getPtr(), 5);
    MutableColumnPtr cp2 = ColumnString::create();
    cp2->insert(Field(trim.c_str(), trim.size()));
    ColumnPtr excsp = ColumnConst::create(cp2->getPtr(), 5);

    Block test_block;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(csp, std::make_shared<DataTypeString>(), "test_trim");
    ColumnWithTypeAndName exctn = ColumnWithTypeAndName(excsp, std::make_shared<DataTypeString>(), "test_ex_trim");
    ColumnsWithTypeAndName ctns{ctn, exctn};
    test_block.insert(ctn);
    // for result from trim, ltrim and rtrim
    test_block.insert(exctn);
    test_block.insert({});
    ColumnNumbers cns{0, 1};

    // test trim
    auto bp = factory.tryGet("tidbTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 2);
    const IColumn * res = test_block.getByPosition(2).column.get();
    const ColumnConst * c0_string = checkAndGetColumn<ColumnConst>(res);

    Field res_field;
    std::vector<String> results{"好   ", "好   ", "好   ", "好   ", "好   "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("tidbLTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 2);
    res = test_block.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnConst>(res);

    results = {"好   ", "好   ", "好   ", "好   ", "好   "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("tidbRTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 2);
    res = test_block.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnConst>(res);

    results = {"  你好   ", "  你好   ", "  你好   ", "  你好   ", "  你好   "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

TEST_F(StringTrim, stringTrimUtf8UnitTest)
{
    const auto context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"  你好   ", "   上海", "北京晨凯", "     ", "你好, world"};

    MutableColumnPtr csp = ColumnString::create();
    for (const auto & str : strs)
    {
        csp->insert(Field(str.c_str(), str.size()));
    }

    Block test_block;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeString>(), "test_trim");
    ColumnsWithTypeAndName ctns{ctn};
    test_block.insert(ctn);
    // for result from trim, ltrim and rtrim
    test_block.insert({});
    ColumnNumbers cns{0};

    // test trim
    auto bp = factory.tryGet("tidbTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 1);
    const IColumn * res = test_block.getByPosition(1).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field res_field;
    std::vector<String> results{"你好", "上海", "北京晨凯", "", "你好, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("tidbLTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 1);
    res = test_block.getByPosition(1).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"你好   ", "上海", "北京晨凯", "", "你好, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("tidbRTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 1);
    res = test_block.getByPosition(1).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"  你好", "   上海", "北京晨凯", "", "你好, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

TEST_F(StringTrim, stringTrimConstUtf8UnitTest)
{
    const auto context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    String str = "  你好   ";

    MutableColumnPtr cp = ColumnString::create();
    cp->insert(Field(str.c_str(), str.size()));

    ColumnPtr csp = ColumnConst::create(cp->getPtr(), 5);

    Block test_block;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(csp, std::make_shared<DataTypeString>(), "test_trim");
    ColumnsWithTypeAndName ctns{ctn};
    test_block.insert(ctn);
    // for result from trim, ltrim and rtrim
    test_block.insert({});
    ColumnNumbers cns{0};

    // test trim
    auto bp = factory.tryGet("tidbTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 1);
    const IColumn * res = test_block.getByPosition(1).column.get();
    const ColumnConst * c0_string = checkAndGetColumn<ColumnConst>(res);

    Field res_field;
    std::vector<String> results{"你好", "你好", "你好", "你好", "你好"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("tidbLTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 1);
    res = test_block.getByPosition(1).column.get();
    c0_string = checkAndGetColumn<ColumnConst>(res);

    results = {"你好   ", "你好   ", "你好   ", "你好   ", "你好   "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("tidbRTrim", *context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(test_block, cns, 1);
    res = test_block.getByPosition(1).column.get();
    c0_string = checkAndGetColumn<ColumnConst>(res);

    results = {"  你好", "  你好", "  你好", "  你好", "  你好"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, res_field);
        String s = res_field.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

TEST_F(StringTrim, strTrimTest)
try
{
    // trim(const)
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "x"),
        executeFunction("tidbTrim", createConstColumn<Nullable<String>>(5, " x ")));

    // trim(const from const)
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "a"),
        executeFunction(
            "tidbTrim",
            createConstColumn<Nullable<String>>(5, "xax"),
            createConstColumn<Nullable<String>>(5, "x")));

    // trim(leading|trailing|both const from const)
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "a"),
        executeFunction(
            "tidbTrim",
            createConstColumn<Nullable<String>>(5, "xax"),
            createConstColumn<Nullable<String>>(5, "x"),
            createConstColumn<Nullable<Int8>>(5, 0)));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "a"),
        executeFunction(
            "tidbTrim",
            createConstColumn<Nullable<String>>(5, "xax"),
            createConstColumn<Nullable<String>>(5, "x"),
            createConstColumn<Nullable<Int8>>(5, 1)));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "ax"),
        executeFunction(
            "tidbTrim",
            createConstColumn<Nullable<String>>(5, "xax"),
            createConstColumn<Nullable<String>>(5, "x"),
            createConstColumn<Nullable<Int8>>(5, 2)));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "xa"),
        executeFunction(
            "tidbTrim",
            createConstColumn<Nullable<String>>(5, "xax"),
            createConstColumn<Nullable<String>>(5, "x"),
            createConstColumn<Nullable<Int8>>(5, 3)));


    // trim(column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"xx aa", "xxaa xx", "\t aa \t", "", {}}),
        executeFunction("tidbTrim", createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}})));
    ASSERT_COLUMN_EQ(
        createColumn<String>({"xx aa", "xxaa xx", "\t aa \t", "", {}}),
        executeFunction("tidbTrim", createColumn<String>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}})));

    // trim(column from column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}}),
            createColumn<Nullable<String>>({"x", "x", "x", "x", "x"})));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa", "aa xx ", "axxa \t", "aa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({"x", "x", "x", "x", "x"})));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"x aa", "aa xx ", "axxa \tx", "aa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xxx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({"xx", "xx", "xx", "xx", "xx"})));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa ", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({" x x aa  x", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({" x", " x", " x", " x", " x"})));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa ", "aa xx ", "a \tx", "axx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({" x x aa  x", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({" x", "x", "xx", "xxa", " x"})));

    ASSERT_COLUMN_EQ(
        createColumn<String>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<String>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}}),
            createColumn<String>({"x", "x", "x", "x", "x"})));
    ASSERT_COLUMN_EQ(
        createColumn<String>({" aa", "aa xx ", "axxa \t", "aa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<String>({"xx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createColumn<String>({"x", "x", "x", "x", "x"})));
    ASSERT_COLUMN_EQ(
        createColumn<String>({"x aa", "aa xx ", "axxa \tx", "aa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<String>({"xxx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createColumn<String>({"xx", "xx", "xx", "xx", "xx"})));
    ASSERT_COLUMN_EQ(
        createColumn<String>({" aa ", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<String>({" x x aa  x", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
            createColumn<String>({" x", " x", " x", " x", " x"})));
    ASSERT_COLUMN_EQ(
        createColumn<String>({" aa ", "aa xx ", "a \tx", "axx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<String>({" x x aa  x", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
            createColumn<String>({" x", "x", "xx", "xxa", " x"})));

    // trim(both|leading|trailing column from column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}}),
            createColumn<Nullable<String>>({"x", "x", "x", "x", "x"}),
            createConstColumn<Nullable<Int8>>(5, 0)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}}),
            createColumn<Nullable<String>>({"x", "x", "x", "x", "x"}),
            createConstColumn<Nullable<Int8>>(5, 1)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}}),
            createColumn<Nullable<String>>({"x", "x", "x", "x", "x"}),
            createConstColumn<Nullable<Int8>>(5, 2)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}}),
            createColumn<Nullable<String>>({"x", "x", "x", "x", "x"}),
            createConstColumn<Nullable<Int8>>(5, 3)));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa", "aa xx ", "axxa \t", "aa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({"x", "x", "x", "x", "x"}),
            createConstColumn<Nullable<Int8>>(5, 0)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa", "aa xx ", "axxa \t", "aa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({"x", "x", "x", "x", "x"}),
            createConstColumn<Nullable<Int8>>(5, 1)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa", "aa xx ", "axxa \txxx", "aaxx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({"x", "x", "x", "x", "x"}),
            createConstColumn<Nullable<Int8>>(5, 2)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "axxa \t", "xxaa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({"x", "x", "x", "x", "x"}),
            createConstColumn<Nullable<Int8>>(5, 3)));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"x aa", "aa xx ", "axxa \tx", "aa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xxx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({"xx", "xx", "xx", "xx", "xx"}),
            createConstColumn<Nullable<Int8>>(5, 0)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"x aa", "aa xx ", "axxa \tx", "aa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xxx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({"xx", "xx", "xx", "xx", "xx"}),
            createConstColumn<Nullable<Int8>>(5, 1)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"x aa", "aa xx ", "axxa \txxx", "aaxx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xxx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({"xx", "xx", "xx", "xx", "xx"}),
            createConstColumn<Nullable<Int8>>(5, 2)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"xxx aa", "xxaa xx ", "axxa \tx", "xxaa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xxx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({"xx", "xx", "xx", "xx", "xx"}),
            createConstColumn<Nullable<Int8>>(5, 3)));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa ", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({" x x aa  x", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({" x", " x", " x", " x", " x"}),
            createConstColumn<Nullable<Int8>>(5, 0)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa ", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({" x x aa  x", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({" x", " x", " x", " x", " x"}),
            createConstColumn<Nullable<Int8>>(5, 1)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa  x", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({" x x aa  x", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({" x", " x", " x", " x", " x"}),
            createConstColumn<Nullable<Int8>>(5, 2)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" x x aa ", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({" x x aa  x", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({" x", " x", " x", " x", " x"}),
            createConstColumn<Nullable<Int8>>(5, 3)));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa ", "aa xx ", "a \tx", "axx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({" x x aa  x", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({" x", "x", "xx", "xxa", " x"}),
            createConstColumn<Nullable<Int8>>(5, 0)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa ", "aa xx ", "a \tx", "axx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({" x x aa  x", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({" x", "x", "xx", "xxa", " x"}),
            createConstColumn<Nullable<Int8>>(5, 1)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa  x", "aa xx ", "a \txxx", "axx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({" x x aa  x", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({" x", "x", "xx", "xxa", " x"}),
            createConstColumn<Nullable<Int8>>(5, 2)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" x x aa ", "xaa xx ", "xxa \tx", "xxaaxx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({" x x aa  x", "xaa xx ", "xxa \txxx", "xxaaxx", {}}),
            createColumn<Nullable<String>>({" x", "x", "xx", "xxa", " x"}),
            createConstColumn<Nullable<Int8>>(5, 3)));

    // trim(const from column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa", "aa xx ", "axxa \t", "aa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createConstColumn<Nullable<String>>(5, "x")));

    // trim(column from const)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"ax ", "ax x", "x x", "xxax", {}}),
        executeFunction(
            "tidbTrim",
            createConstColumn<Nullable<String>>(5, "xxax x"),
            createColumn<Nullable<String>>({"x", "xx", "xxa", " x", {}})));

    // trim(both|leading|trailing const from column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa", "aa xx ", "axxa \t", "aa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createConstColumn<Nullable<String>>(5, "x"),
            createConstColumn<Nullable<Int8>>(5, 0)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa", "aa xx ", "axxa \t", "aa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createConstColumn<Nullable<String>>(5, "x"),
            createConstColumn<Nullable<Int8>>(5, 1)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({" aa", "aa xx ", "axxa \txxx", "aaxx", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createConstColumn<Nullable<String>>(5, "x"),
            createConstColumn<Nullable<Int8>>(5, 2)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "axxa \t", "xxaa", {}}),
        executeFunction(
            "tidbTrim",
            createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "axxa \txxx", "xxaaxx", {}}),
            createConstColumn<Nullable<String>>(5, "x"),
            createConstColumn<Nullable<Int8>>(5, 3)));

    // trim(both|leading|trailing column from const)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"ax ", "ax x", "x x", "xxax", {}}),
        executeFunction(
            "tidbTrim",
            createConstColumn<Nullable<String>>(5, "xxax x"),
            createColumn<Nullable<String>>({"x", "xx", "xxa", " x", {}}),
            createConstColumn<Nullable<Int8>>(5, 0)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"ax ", "ax x", "x x", "xxax", {}}),
        executeFunction(
            "tidbTrim",
            createConstColumn<Nullable<String>>(5, "xxax x"),
            createColumn<Nullable<String>>({"x", "xx", "xxa", " x", {}}),
            createConstColumn<Nullable<Int8>>(5, 1)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"ax x", "ax x", "x x", "xxax x", {}}),
        executeFunction(
            "tidbTrim",
            createConstColumn<Nullable<String>>(5, "xxax x"),
            createColumn<Nullable<String>>({"x", "xx", "xxa", " x", {}}),
            createConstColumn<Nullable<Int8>>(5, 2)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"xxax ", "xxax x", "xxax x", "xxax", {}}),
        executeFunction(
            "tidbTrim",
            createConstColumn<Nullable<String>>(5, "xxax x"),
            createColumn<Nullable<String>>({"x", "xx", "xxa", " x", {}}),
            createConstColumn<Nullable<Int8>>(5, 3)));

    //different trim policy
    for (int i = 0; i < 3; i++)
    {
        //test NULL and "" case
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"", "", "", {}, "", "", ""}),
            executeFunction(
                "tidbTrim",
                createColumn<Nullable<String>>({"", "", "", "", "", "", ""}),
                createColumn<Nullable<String>>({"", "x", "xx", {}, "啊", "\t", " "}),
                createConstColumn<Nullable<Int8>>(7, i)));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({{}, {}, {}, {}, {}, {}, {}}),
            executeFunction(
                "tidbTrim",
                createColumn<Nullable<String>>({{}, {}, {}, {}, {}, {}, {}}),
                createColumn<Nullable<String>>({"", "x", "xx", {}, "啊", "\t", " "}),
                createConstColumn<Nullable<Int8>>(7, i)));

        //test repeated pattern: ASCII & non-ASCII
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"", "x", "", "x", "", "x", {}}),
            executeFunction(
                "tidbTrim",
                createColumn<Nullable<String>>({"", "x", "xx", "xxx", "xxxx", "xxxxx", {}}),
                createColumn<Nullable<String>>({"xx", "xx", "xx", "xx", "xx", "xx", "xx"}),
                createConstColumn<Nullable<Int8>>(7, i)));


        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"", "啊", "", "啊", "", "啊"}),
            executeFunction(
                "tidbTrim",
                createColumn<Nullable<String>>({"", "啊", "啊啊", "啊啊啊", "啊啊啊啊", "啊啊啊啊啊"}),
                createColumn<Nullable<String>>({"啊啊", "啊啊", "啊啊", "啊啊", "啊啊", "啊啊"}),
                createConstColumn<Nullable<Int8>>(6, i)));
    }

    //test non-ASCII cases
    InferredDataInitializerList<Nullable<String>> results_columns_ws_with_core_text[] = {
        {" 波 波 ", "啊 波 波 啊", " 波 波 ", "啊 波 波 啊", " 波 波 "}, //default(both)
        {" 波 波 ", "啊 波 波 啊", " 波 波 ", "啊 波 波 啊", " 波 波 "}, //both
        {" 波 波 ", "啊 波 波 啊", " 波 波 啊啊", "啊 波 波 啊啊啊", " 波 波 啊啊啊啊"}, //left
        {" 波 波 ", "啊 波 波 啊", "啊啊 波 波 ", "啊啊啊 波 波 啊", "啊啊啊啊 波 波 "} //right
    };

    InferredDataInitializerList<Nullable<String>> input_columns_ws_with_core_text
        = {" 波 波 ", "啊 波 波 啊", "啊啊 波 波 啊啊", "啊啊啊 波 波 啊啊啊", "啊啊啊啊 波 波 啊啊啊啊"};

    //different trim policy
    for (int i = 0; i < 3; i++)
    {
        //non-const
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>(results_columns_ws_with_core_text[i]),
            executeFunction(
                "tidbTrim",
                createColumn<Nullable<String>>(input_columns_ws_with_core_text),
                createColumn<Nullable<String>>({"啊啊", "啊啊", "啊啊", "啊啊", "啊啊", "啊啊"}),
                createConstColumn<Nullable<Int8>>(6, i)));

        //const
        for (size_t j = 0; j < results_columns_ws_with_core_text[i].size(); j++)
        {
            const auto * input_itr = input_columns_ws_with_core_text.begin();
            size_t cnt = 0;
            for (const auto * res_itr = results_columns_ws_with_core_text[i].begin();
                 res_itr != results_columns_ws_with_core_text[i].end()
                 && input_itr != input_columns_ws_with_core_text.end();
                 res_itr++, input_itr++)
            {
                ASSERT_COLUMN_EQ(
                    input_itr->has_value() ? createConstColumn<String>(5, res_itr->value())
                                           : createConstColumn<Nullable<String>>(5, *res_itr),
                    executeFunction(
                        "tidbTrim",
                        createConstColumn<Nullable<String>>(5, *input_itr),
                        createConstColumn<Nullable<String>>(5, "啊啊"),
                        createConstColumn<Nullable<Int8>>(5, i)));
                cnt++;
            }
            ASSERT_EQ(cnt, input_columns_ws_with_core_text.size());
            ASSERT_EQ(cnt, results_columns_ws_with_core_text[i].size());
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
