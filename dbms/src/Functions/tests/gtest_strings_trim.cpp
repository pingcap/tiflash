#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
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


class StringTrim : public ::testing::Test
{
protected:
    static void SetUpTestCase()
    {
        try
        {
            registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registed, ignore exception here.
        }
    }
};


TEST_F(StringTrim, string_trim_string_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"};

    MutableColumnPtr csp = ColumnString::create();
    for (const auto & str : strs)
    {
        csp->insert(Field(str.c_str(), str.size()));
    }

    Block testBlock;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeString>(), "test_trim");
    ColumnsWithTypeAndName ctns{ctn};
    testBlock.insert(ctn);
    // for result from trim, ltrim and rtrim
    testBlock.insert({});
    testBlock.insert({});
    testBlock.insert({});
    ColumnNumbers cns{0};

    // test trim
    auto bp = factory.tryGet("trim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 1);
    const IColumn * res = testBlock.getByPosition(1).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field resField;

    std::vector<String> results{"hello", "h e llo", "hello", "", "hello, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("ltrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 2);
    res = testBlock.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"hello   ", "h e llo", "hello    ", "", "hello, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("rtrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"  hello", "   h e llo", "hello", "", "hello, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}


TEST_F(StringTrim, string_trim_const_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();
    MutableColumnPtr cp = ColumnString::create();
    cp->insert(Field("  hello   ", 10));

    ColumnPtr csp = ColumnConst::create(cp->getPtr(), 5);
    Block testBlock;
    auto type = std::make_shared<DataTypeString>();

    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(csp, type, "test_trim_const");

    ColumnsWithTypeAndName ctns{ctn};
    testBlock.insert(ctn);
    // for result from trim, ltrim and rtrim
    testBlock.insert({});
    testBlock.insert({});
    testBlock.insert({});
    ColumnNumbers cns{0};

    // test trim
    auto bp = factory.tryGet("trim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 1);

    const IColumn * res = testBlock.getByPosition(1).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);


    Field resField;

    std::vector<String> results{"hello", "hello", "hello", "hello", "hello"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("ltrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 2);
    res = testBlock.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"hello   ", "hello   ", "hello   ", "hello   ", "hello   "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("rtrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {
        "  hello",
        "  hello",
        "  hello",
        "  hello",
        "  hello",
    };
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

TEST_F(StringTrim, string_trimws_const_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();
    MutableColumnPtr cp = ColumnString::create();
    cp->insert(Field("  hello   ", 10));
    MutableColumnPtr excp = ColumnString::create();
    excp->insert(Field(" hoe", 10));

    ColumnPtr csp = ColumnConst::create(cp->getPtr(), 5);
    ColumnPtr excsp = ColumnConst::create(excp->getPtr(), 5);
    Block testBlock;

    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(csp, std::make_shared<DataTypeString>(), "test_trim_const");
    ColumnWithTypeAndName exctn = ColumnWithTypeAndName(excsp, std::make_shared<DataTypeString>(), "test_ex_trim_const");

    ColumnsWithTypeAndName ctns{ctn, exctn};
    testBlock.insert(ctn);
    testBlock.insert(exctn);
    // for result from trim, ltrim and rtrim
    testBlock.insert({});
    testBlock.insert({});
    testBlock.insert({});
    ColumnNumbers cns{0, 1};

    // test trim
    auto bp = factory.tryGet("trim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 2);

    const IColumn * res = testBlock.getByPosition(2).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);


    Field resField;

    std::vector<String> results{"ll", "ll", "ll", "ll", "ll"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("ltrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 2);
    res = testBlock.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"llo   ", "llo   ", "llo   ", "llo   ", "llo   "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("rtrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {
        "  hell",
        "  hell",
        "  hell",
        "  hell",
        "  hell",
    };
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

TEST_F(StringTrim, string_trimws_utf8_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"  你好   ", "   上海", "北京晨凯", "     ", "你好, world"};
    String trim = "你好上 ";

    MutableColumnPtr csp = ColumnString::create();
    for (const auto & str : strs)
    {
        csp->insert(Field(str.c_str(), str.size()));
    }
    MutableColumnPtr cp2 = ColumnString::create();
    cp2->insert(Field(trim.c_str(), trim.size()));
    ColumnPtr excsp = ColumnConst::create(cp2->getPtr(), 5);

    Block testBlock;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeString>(), "test_trim");
    ColumnWithTypeAndName exctn = ColumnWithTypeAndName(excsp, std::make_shared<DataTypeString>(), "test_ex_trim");
    ColumnsWithTypeAndName ctns{ctn, exctn};
    testBlock.insert(ctn);
    // for result from trim, ltrim and rtrim
    testBlock.insert(exctn);
    testBlock.insert({});
    ColumnNumbers cns{0, 1};

    // test trim
    auto bp = factory.tryGet("trim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 2);
    const IColumn * res = testBlock.getByPosition(2).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field resField;
    std::vector<String> results{"", "海", "北京晨凯", "", ", world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("ltrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 2);
    res = testBlock.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"", "海", "北京晨凯", "", ", world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("rtrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 2);
    res = testBlock.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"", "   上海", "北京晨凯", "", "你好, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

TEST_F(StringTrim, string_trimws_const_utf8_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    String str = "  你好   ";
    String trim = " 你 ";

    MutableColumnPtr cp = ColumnString::create();
    cp->insert(Field(str.c_str(), str.size()));
    ColumnPtr csp = ColumnConst::create(cp->getPtr(), 5);
    MutableColumnPtr cp2 = ColumnString::create();
    cp2->insert(Field(trim.c_str(), trim.size()));
    ColumnPtr excsp = ColumnConst::create(cp2->getPtr(), 5);

    Block testBlock;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(csp, std::make_shared<DataTypeString>(), "test_trim");
    ColumnWithTypeAndName exctn = ColumnWithTypeAndName(excsp, std::make_shared<DataTypeString>(), "test_ex_trim");
    ColumnsWithTypeAndName ctns{ctn, exctn};
    testBlock.insert(ctn);
    // for result from trim, ltrim and rtrim
    testBlock.insert(exctn);
    testBlock.insert({});
    ColumnNumbers cns{0, 1};

    // test trim
    auto bp = factory.tryGet("trim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 2);
    const IColumn * res = testBlock.getByPosition(2).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field resField;
    std::vector<String> results{"好", "好", "好", "好", "好"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("ltrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 2);
    res = testBlock.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"好   ", "好   ", "好   ", "好   ", "好   "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("rtrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 2);
    res = testBlock.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"  你好", "  你好", "  你好", "  你好", "  你好"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

TEST_F(StringTrim, string_trim_utf8_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"  你好   ", "   上海", "北京晨凯", "     ", "你好, world"};

    MutableColumnPtr csp = ColumnString::create();
    for (const auto & str : strs)
    {
        csp->insert(Field(str.c_str(), str.size()));
    }

    Block testBlock;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeString>(), "test_trim");
    ColumnsWithTypeAndName ctns{ctn};
    testBlock.insert(ctn);
    // for result from trim, ltrim and rtrim
    testBlock.insert({});
    ColumnNumbers cns{0};

    // test trim
    auto bp = factory.tryGet("trim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 1);
    const IColumn * res = testBlock.getByPosition(1).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field resField;
    std::vector<String> results{"你好", "上海", "北京晨凯", "", "你好, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("ltrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 1);
    res = testBlock.getByPosition(1).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"你好   ", "上海", "北京晨凯", "", "你好, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("rtrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 1);
    res = testBlock.getByPosition(1).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"  你好", "   上海", "北京晨凯", "", "你好, world"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

TEST_F(StringTrim, string_trim_const_utf8_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    String str = "  你好   ";

    MutableColumnPtr cp = ColumnString::create();
    cp->insert(Field(str.c_str(), str.size()));

    ColumnPtr csp = ColumnConst::create(cp->getPtr(), 5);

    Block testBlock;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(csp, std::make_shared<DataTypeString>(), "test_trim");
    ColumnsWithTypeAndName ctns{ctn};
    testBlock.insert(ctn);
    // for result from trim, ltrim and rtrim
    testBlock.insert({});
    ColumnNumbers cns{0};

    // test trim
    auto bp = factory.tryGet("trim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 1);
    const IColumn * res = testBlock.getByPosition(1).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field resField;
    std::vector<String> results{"你好", "你好", "你好", "你好", "你好"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("ltrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 1);
    res = testBlock.getByPosition(1).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"你好   ", "你好   ", "你好   ", "你好   ", "你好   "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("rtrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 1);
    res = testBlock.getByPosition(1).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"  你好", "  你好", "  你好", "  你好", "  你好"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

} // namespace tests
} // namespace DB
