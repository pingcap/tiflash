#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
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


class StringPad : public ::testing::Test
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


TEST_F(StringPad, string_pad_string_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"hello", "hello, world", "It's TiFlash", "     ", ""};

    MutableColumnPtr csp = ColumnString::create();
    for (const auto & str : strs)
    {
        csp->insert(Field(str.c_str(), str.size()));
    }
    auto cp2 = ColumnInt64::create();
    ColumnInt64::Container & vec = cp2->getData();
    vec.resize(1);
    vec[0] = 12;
    ColumnPtr csp2 = ColumnConst::create(cp2->getPtr(), 5);

    MutableColumnPtr cp3 = ColumnString::create();
    cp3->insert(Field("abc", 3));

    ColumnPtr csp3 = ColumnConst::create(cp3->getPtr(), 5);

    Block testBlock;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeString>(), "test_pad_1");
    ColumnWithTypeAndName ctn2 = ColumnWithTypeAndName(csp2, std::make_shared<DataTypeInt64>(), "test_pad_2");
    ColumnWithTypeAndName ctn3 = ColumnWithTypeAndName(csp3, std::make_shared<DataTypeString>(), "test_pad_3");
    ColumnsWithTypeAndName ctns{ctn, ctn2, ctn3};
    testBlock.insert(ctn);

    testBlock.insert(ctn2);
    testBlock.insert(ctn3);
    testBlock.insert({});
    ColumnNumbers cns{0, 1, 2};

    // test lpad
    auto bp = factory.tryGet("lpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field resField;

    std::vector<String> results{"abcabcahello", "hello, world", "It's TiFlash", "abcabca     ", "abcabcabcabc"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rpad
    bp = factory.tryGet("rpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"helloabcabca", "hello, world", "It's TiFlash", "     abcabca", "abcabcabcabc"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}


TEST_F(StringPad, string_pad_const_string_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    MutableColumnPtr cp = ColumnString::create();
    cp->insert(Field("hello", 5));
    ColumnPtr csp = ColumnConst::create(cp->getPtr(), 5);

    auto cp2 = ColumnInt64::create();
    ColumnInt64::Container & vec = cp2->getData();
    vec.resize(1);
    vec[0] = 10;
    ColumnPtr csp2 = ColumnConst::create(cp2->getPtr(), 5);

    MutableColumnPtr cp3 = ColumnString::create();
    cp3->insert(Field("abc", 3));

    ColumnPtr csp3 = ColumnConst::create(cp3->getPtr(), 5);

    Block testBlock;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(csp, std::make_shared<DataTypeString>(), "test_pad_1");
    ColumnWithTypeAndName ctn2 = ColumnWithTypeAndName(csp2, std::make_shared<DataTypeInt64>(), "test_pad_2");
    ColumnWithTypeAndName ctn3 = ColumnWithTypeAndName(csp3, std::make_shared<DataTypeString>(), "test_pad_3");
    ColumnsWithTypeAndName ctns{ctn, ctn2, ctn3};
    testBlock.insert(ctn);

    testBlock.insert(ctn2);
    testBlock.insert(ctn3);
    testBlock.insert({});
    ColumnNumbers cns{0, 1, 2};

    // test lpad
    auto bp = factory.tryGet("lpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field resField;

    std::vector<String> results{"abcabhello", "abcabhello", "abcabhello", "abcabhello", "abcabhello"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_STREQ(results[t].c_str(), s.c_str());
    }

    // test rpad
    bp = factory.tryGet("rpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"helloabcab", "helloabcab", "helloabcab", "helloabcab", "helloabcab"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_STREQ(results[t].c_str(), s.c_str());
    }
}


TEST_F(StringPad, string_pad_empty_padding_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"hello", "hello, world", "It's TiFlash", "     ", ""};

    MutableColumnPtr csp = ColumnString::create();
    for (const auto & str : strs)
    {
        csp->insert(Field(str.c_str(), str.size()));
    }
    auto cp2 = ColumnInt64::create();
    ColumnInt64::Container & vec = cp2->getData();
    vec.resize(1);
    vec[0] = 12;
    ColumnPtr csp2 = ColumnConst::create(cp2->getPtr(), 5);

    MutableColumnPtr cp3 = ColumnString::create();
    cp3->insert(Field("", 0));

    ColumnPtr csp3 = ColumnConst::create(cp3->getPtr(), 5);

    Block testBlock;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeString>(), "test_pad_1");
    ColumnWithTypeAndName ctn2 = ColumnWithTypeAndName(csp2, std::make_shared<DataTypeInt64>(), "test_pad_2");
    ColumnWithTypeAndName ctn3 = ColumnWithTypeAndName(csp3, std::make_shared<DataTypeString>(), "test_pad_3");
    ColumnsWithTypeAndName ctns{ctn, ctn2, ctn3};
    testBlock.insert(ctn);

    testBlock.insert(ctn2);
    testBlock.insert(ctn3);
    testBlock.insert({});
    ColumnNumbers cns{0, 1, 2};

    // test lpad
    auto bp = factory.tryGet("lpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field resField;

    std::vector<String> results{"hello", "hello, world", "It's TiFlash", "     ", ""};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rpad
    bp = factory.tryGet("rpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"hello", "hello, world", "It's TiFlash", "     ", ""};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}


TEST_F(StringPad, string_pad_utf8_padding_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"你好,", "你好， 世界", "这是 TiFlash", "     ", ""};

    MutableColumnPtr csp = ColumnString::create();
    for (const auto & str : strs)
    {
        csp->insert(Field(str.c_str(), str.size()));
    }
    auto cp2 = ColumnInt64::create();
    ColumnInt64::Container & vec = cp2->getData();
    vec.resize(1);
    vec[0] = 4;
    ColumnPtr csp2 = ColumnConst::create(cp2->getPtr(), 5);

    MutableColumnPtr cp3 = ColumnString::create();
    String padding = "上海";
    cp3->insert(Field(padding.c_str(), padding.size()));

    ColumnPtr csp3 = ColumnConst::create(cp3->getPtr(), 5);

    Block testBlock;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeString>(), "test_pad_1");
    ColumnWithTypeAndName ctn2 = ColumnWithTypeAndName(csp2, std::make_shared<DataTypeInt64>(), "test_pad_2");
    ColumnWithTypeAndName ctn3 = ColumnWithTypeAndName(csp3, std::make_shared<DataTypeString>(), "test_pad_3");
    ColumnsWithTypeAndName ctns{ctn, ctn2, ctn3};
    testBlock.insert(ctn);

    testBlock.insert(ctn2);
    testBlock.insert(ctn3);
    testBlock.insert({});
    ColumnNumbers cns{0, 1, 2};

    // test lpad
    auto bp = factory.tryGet("lpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field resField;

    std::vector<String> results{"上你好,", "你好， ", "这是 T", "    ", "上海上海"};
    for (size_t t = 0; t < results.size(); t++)
    {
        String s;
        c0_string->get(t, resField);
        s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rpad
    bp = factory.tryGet("rpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"你好,上", "你好， ", "这是 T", "    ", "上海上海"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}


TEST_F(StringPad, string_pad_fixed_utf8_padding_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"你好,", "你好， 世界", "这是 TiFlash", "     ", ""};

    MutableColumnPtr csp = ColumnFixedString::create(30);
    for (const auto & str : strs)
    {
        csp->insert(Field(str.c_str(), 30));
    }
    auto cp2 = ColumnInt64::create();
    ColumnInt64::Container & vec = cp2->getData();
    vec.resize(1);
    vec[0] = 4;
    ColumnPtr csp2 = ColumnConst::create(cp2->getPtr(), 5);

    MutableColumnPtr cp3 = ColumnString::create();
    String padding = "上海";
    cp3->insert(Field(padding.c_str(), padding.size()));

    ColumnPtr csp3 = ColumnConst::create(cp3->getPtr(), 5);

    Block testBlock;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeFixedString>(10), "test_pad_1");
    ColumnWithTypeAndName ctn2 = ColumnWithTypeAndName(csp2, std::make_shared<DataTypeInt64>(), "test_pad_2");
    ColumnWithTypeAndName ctn3 = ColumnWithTypeAndName(csp3, std::make_shared<DataTypeString>(), "test_pad_3");
    ColumnsWithTypeAndName ctns{ctn, ctn2, ctn3};
    testBlock.insert(ctn);

    testBlock.insert(ctn2);
    testBlock.insert(ctn3);
    testBlock.insert({});
    ColumnNumbers cns{0, 1, 2};

    // test lpad
    auto bp = factory.tryGet("lpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field resField;

    std::vector<String> results{"上你好,", "你好， ", "这是 T", "    ", "上海上海"};
    for (size_t t = 0; t < results.size(); t++)
    {
        String s;
        c0_string->get(t, resField);
        s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rpad
    bp = factory.tryGet("rpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"你好,上", "你好， ", "这是 T", "    ", "上海上海"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}


TEST_F(StringPad, string_pad_const_utf8_padding_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    MutableColumnPtr cp = ColumnString::create();
    String word = "你好,";
    cp->insert(Field(word.c_str(), word.size()));
    ColumnPtr csp = ColumnConst::create(cp->getPtr(), 5);

    auto cp2 = ColumnInt64::create();
    ColumnInt64::Container & vec = cp2->getData();
    vec.resize(1);
    vec[0] = 4;
    ColumnPtr csp2 = ColumnConst::create(cp2->getPtr(), 5);

    MutableColumnPtr cp3 = ColumnString::create();
    String padding = "上海";
    cp3->insert(Field(padding.c_str(), padding.size()));

    ColumnPtr csp3 = ColumnConst::create(cp3->getPtr(), 5);

    Block testBlock;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(csp, std::make_shared<DataTypeString>(), "test_pad_1");
    ColumnWithTypeAndName ctn2 = ColumnWithTypeAndName(csp2, std::make_shared<DataTypeInt64>(), "test_pad_2");
    ColumnWithTypeAndName ctn3 = ColumnWithTypeAndName(csp3, std::make_shared<DataTypeString>(), "test_pad_3");
    ColumnsWithTypeAndName ctns{ctn, ctn2, ctn3};
    testBlock.insert(ctn);

    testBlock.insert(ctn2);
    testBlock.insert(ctn3);
    testBlock.insert({});
    ColumnNumbers cns{0, 1, 2};

    // test lpad
    auto bp = factory.tryGet("lpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);
    ASSERT_TRUE(c0_string != nullptr);

    Field resField;

    std::vector<String> results{"上你好,", "上你好,", "上你好,", "上你好,", "上你好,"};
    for (size_t t = 0; t < results.size(); t++)
    {
        String s;
        c0_string->get(t, resField);
        s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rpad
    bp = factory.tryGet("rpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"你好,上", "你好,上", "你好,上", "你好,上", "你好,上"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}


TEST_F(StringPad, string_pad_const_fixed_utf8_padding_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    MutableColumnPtr cp = ColumnFixedString::create(30);
    String word = "你好,";
    cp->insert(Field(word.c_str(), 30));
    ColumnPtr csp = ColumnConst::create(cp->getPtr(), 5);

    auto cp2 = ColumnInt64::create();
    ColumnInt64::Container & vec = cp2->getData();
    vec.resize(1);
    vec[0] = 4;
    ColumnPtr csp2 = ColumnConst::create(cp2->getPtr(), 5);

    MutableColumnPtr cp3 = ColumnString::create();
    String padding = "上海";
    cp3->insert(Field(padding.c_str(), padding.size()));

    ColumnPtr csp3 = ColumnConst::create(cp3->getPtr(), 5);

    Block testBlock;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(csp, std::make_shared<DataTypeFixedString>(30), "test_pad_1");
    ColumnWithTypeAndName ctn2 = ColumnWithTypeAndName(csp2, std::make_shared<DataTypeInt64>(), "test_pad_2");
    ColumnWithTypeAndName ctn3 = ColumnWithTypeAndName(csp3, std::make_shared<DataTypeString>(), "test_pad_3");
    ColumnsWithTypeAndName ctns{ctn, ctn2, ctn3};
    testBlock.insert(ctn);

    testBlock.insert(ctn2);
    testBlock.insert(ctn3);
    testBlock.insert({});
    ColumnNumbers cns{0, 1, 2};

    // test lpad
    auto bp = factory.tryGet("lpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);
    ASSERT_TRUE(c0_string != nullptr);

    Field resField;

    std::vector<String> results{"上你好,", "上你好,", "上你好,", "上你好,", "上你好,"};
    for (size_t t = 0; t < results.size(); t++)
    {
        String s;
        c0_string->get(t, resField);
        s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rpad
    bp = factory.tryGet("rpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"你好,上", "你好,上", "你好,上", "你好,上", "你好,上"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}


TEST_F(StringPad, string_pad_empty_utf8_padding_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> strs{"你好,", "你好， 世界", "这是 TiFlash", "     ", ""};

    MutableColumnPtr csp = ColumnString::create();
    for (const auto & str : strs)
    {
        csp->insert(Field(str.c_str(), str.size()));
    }
    auto cp2 = ColumnInt64::create();
    ColumnInt64::Container & vec = cp2->getData();
    vec.resize(1);
    vec[0] = 4;
    ColumnPtr csp2 = ColumnConst::create(cp2->getPtr(), 5);

    MutableColumnPtr cp3 = ColumnString::create();
    String padding = "";
    cp3->insert(Field(padding.c_str(), padding.size()));

    ColumnPtr csp3 = ColumnConst::create(cp3->getPtr(), 5);

    Block testBlock;
    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeString>(), "test_pad_1");
    ColumnWithTypeAndName ctn2 = ColumnWithTypeAndName(csp2, std::make_shared<DataTypeInt64>(), "test_pad_2");
    ColumnWithTypeAndName ctn3 = ColumnWithTypeAndName(csp3, std::make_shared<DataTypeString>(), "test_pad_3");
    ColumnsWithTypeAndName ctns{ctn, ctn2, ctn3};
    testBlock.insert(ctn);

    testBlock.insert(ctn2);
    testBlock.insert(ctn3);
    testBlock.insert({});
    ColumnNumbers cns{0, 1, 2};

    // test lpad
    auto bp = factory.tryGet("lpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);

    Field resField;

    std::vector<String> results{"你好,", "你好， ", "这是 T", "    ", ""};
    for (size_t t = 0; t < results.size(); t++)
    {
        String s;
        c0_string->get(t, resField);
        s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rpad
    bp = factory.tryGet("rpad", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"你好,", "你好， ", "这是 T", "    ", ""};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

} // namespace tests
} // namespace DB
