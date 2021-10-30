#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
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
class StringPad : public DB::tests::FunctionTest
{
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
    auto bp = factory.tryGet("lpadUTF8", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnNullable * c0_nullable = checkAndGetColumn<ColumnNullable>(res);

    Field resField;

    std::vector<String> results{"abcabcahello", "hello, world", "It's TiFlash", "abcabca     ", "abcabcabcabc"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_nullable->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rpad
    bp = factory.tryGet("rpadUTF8", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_nullable = checkAndGetColumn<ColumnNullable>(res);

    results = {"helloabcabca", "hello, world", "It's TiFlash", "     abcabca", "abcabcabcabc"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_nullable->get(t, resField);
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
    auto bp = factory.tryGet("lpadUTF8", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnConst * c0_const = checkAndGetColumn<ColumnConst>(res);


    Field resField;
    c0_const->get(0, resField);
    String s = resField.get<String>();
    EXPECT_STREQ("abcabhello", s.c_str());

    // test rpad
    bp = factory.tryGet("rpadUTF8", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_const = checkAndGetColumn<ColumnConst>(res);

    c0_const->get(0, resField);
    s = resField.get<String>();
    EXPECT_STREQ("helloabcab", s.c_str());
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
    auto bp = factory.tryGet("lpadUTF8", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnNullable * c0_nullable = checkAndGetColumn<ColumnNullable>(res);

    Field resField;

    std::vector<bool> null_bit_map{1, 0, 0, 1, 1};
    std::vector<String> results{"", "hello, world", "It's TiFlash", "     ", ""};
    for (size_t t = 0; t < results.size(); t++)
    {
        if (null_bit_map[t]) {
            EXPECT_TRUE(c0_nullable->isNullAt(t));
        } else {
            c0_nullable->get(t, resField);
            String s = resField.get<String>();
            EXPECT_EQ(results[t], s);
        }
    }

    // test rpad
    bp = factory.tryGet("rpadUTF8", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_nullable = checkAndGetColumn<ColumnNullable>(res);

    null_bit_map = {1, 0, 0, 1, 1};
    results = {"hello", "hello, world", "It's TiFlash", "     ", ""};
    for (size_t t = 0; t < results.size(); t++)
    {
        if (null_bit_map[t]) {
            EXPECT_TRUE(c0_nullable->isNullAt(t));
        } else {
            c0_nullable->get(t, resField);
            String s = resField.get<String>();
            EXPECT_EQ(results[t], s);
        }
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
    auto bp = factory.tryGet("lpadUTF8", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnNullable * c0_nullable = checkAndGetColumn<ColumnNullable>(res);

    Field resField;

    std::vector<String> results{"上你好,", "你好， ", "这是 T", "    ", "上海上海"};
    for (size_t t = 0; t < results.size(); t++)
    {
        String s;
        c0_nullable->get(t, resField);
        s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rpad
    bp = factory.tryGet("rpadUTF8", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_nullable = checkAndGetColumn<ColumnNullable>(res);

    results = {"你好,上", "你好， ", "这是 T", "    ", "上海上海"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_nullable->get(t, resField);
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
    auto bp = factory.tryGet("lpadUTF8", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnConst * c0_const = checkAndGetColumn<ColumnConst>(res);
    ASSERT_TRUE(c0_const != nullptr);

    Field resField;
    String s;
    c0_const->get(0, resField);
    s = resField.get<String>();
    EXPECT_EQ("上你好,", s);

    // test rpad
    bp = factory.tryGet("rpadUTF8", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_const = checkAndGetColumn<ColumnConst>(res);

    c0_const->get(0, resField);
    s = resField.get<String>();
    EXPECT_EQ("你好,上", s);
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
    auto bp = factory.tryGet("lpadUTF8", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    const IColumn * res = testBlock.getByPosition(3).column.get();
    const ColumnNullable * c0_nullable = checkAndGetColumn<ColumnNullable>(res);

    Field resField;

    std::vector<bool> null_bit_map{1, 0, 0, 0, 1};
    std::vector<String> results{"你好,", "你好， ", "这是 T", "    ", ""};
    for (size_t t = 0; t < results.size(); t++)
    {
        if (null_bit_map[t]) {
            EXPECT_TRUE(c0_nullable->isNullAt(t));
        } else {
            c0_nullable->get(t, resField);
            String s = resField.get<String>();
            EXPECT_EQ(results[t], s);
        }
    }

    // test rpad
    bp = factory.tryGet("rpadUTF8", context);
    ASSERT_TRUE(bp != nullptr);

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_nullable = checkAndGetColumn<ColumnNullable>(res);

    null_bit_map = {1, 0, 0, 0, 1};
    results = {"你好,", "你好， ", "这是 T", "    ", ""};
    for (size_t t = 0; t < results.size(); t++)
    {
        if (null_bit_map[t]) {
            EXPECT_TRUE(c0_nullable->isNullAt(t));
        } else {
            c0_nullable->get(t, resField);
            String s = resField.get<String>();
            EXPECT_EQ(results[t], s);
        }
    }
}

TEST_F(StringPad, lpad_mixed_col_const_test)
    try
{
    // pad(const, const, const)
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "xxxxxxxxxabc"),
            executeFunction("lpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<UInt64>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));

    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "abcxxxxxxxxx"),
            executeFunction("rpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<UInt64>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));


    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, {}),
            executeFunction("lpadUTF8",
                            createConstColumn<Nullable<String>>(5, {}),
                            createConstColumn<Nullable<UInt64>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));

    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, {}),
            executeFunction("rpadUTF8",
                            createConstColumn<Nullable<String>>(5, {}),
                            createConstColumn<Nullable<UInt64>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));

    // pad(const, const, column)
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"xxxxxxxxxabc", "yyyyyyyyyabc", "zzzzzzzzzabc", "eeeeeeeeeabc", "fffffffffabc"}),
            executeFunction("lpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<UInt64>>(5, 12),
                            createColumn<Nullable<String>>({"xxx", "yyy", "zzz", "eee", "fff"})));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"abcxxxxxxxxx", "abcyyyyyyyyy", "abczzzzzzzzz", "abceeeeeeeee", "abcfffffffff"}),
            executeFunction("rpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<UInt64>>(5, 12),
                            createColumn<Nullable<String>>({"xxx", "yyy", "zzz", "eee", "fff"})));
     
    // pad(const, column, const)
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"a", "ab", "abc", "xabc", "xxabc"}),
            executeFunction("lpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createColumn<Nullable<Int64>>({1, 2, 3, 4, 5}),
                            createConstColumn<Nullable<String>>(5, "xxx")));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"a", "ab", "abc", "abcx", "abcxx"}),
            executeFunction("rpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createColumn<Nullable<Int64>>({1, 2, 3, 4, 5}),
                            createConstColumn<Nullable<String>>(5, "xxx")));

    // pad(const, column, column)
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"a", "ab", "abc", "eabc", "ffabc"}),
            executeFunction("lpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createColumn<Nullable<Int64>>({1, 2, 3, 4, 5}),
                            createColumn<Nullable<String>>({"xxx", "yyy", "zzz", "eee", "fff"})));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"a", "ab", "abc", "abce", "abcff"}),
            executeFunction("rpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createColumn<Nullable<Int64>>({1, 2, 3, 4, 5}),
                            createColumn<Nullable<String>>({"xxx", "yyy", "zzz", "eee", "fff"})));

    // pad(column, const, const)
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"1231231a", "123123ab", "12312abc", "1231abcd", "123abcde"}),
            executeFunction("lpadUTF8",
                            createColumn<Nullable<String>>({"a", "ab", "abc", "abcd", "abcde"}),
                            createConstColumn<Nullable<UInt64>>(5, 8),
                            createConstColumn<Nullable<String>>(5, "123")));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"a1231231", "ab123123", "abc12312", "abcd1231", "abcde123"}),
            executeFunction("rpadUTF8",
                            createColumn<Nullable<String>>({"a", "ab", "abc", "abcd", "abcde"}),
                            createConstColumn<Nullable<UInt64>>(5, 8),
                            createConstColumn<Nullable<String>>(5, "123")));

    // pad(column, const, column)
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"1212121a", "343434ab", "56565abc", "7878abcd", "999abcde"}),
            executeFunction("lpadUTF8",
                            createColumn<Nullable<String>>({"a", "ab", "abc", "abcd", "abcde"}),
                            createConstColumn<Nullable<UInt64>>(5, 8),
                            createColumn<Nullable<String>>({"12", "34", "56", "78", "99"})));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"a1212121", "ab343434", "abc56565", "abcd7878", "abcde999"}),
            executeFunction("rpadUTF8",
                            createColumn<Nullable<String>>({"a", "ab", "abc", "abcd", "abcde"}),
                            createConstColumn<Nullable<UInt64>>(5, 8),
                            createColumn<Nullable<String>>({"12", "34", "56", "78", "99"})));

    // pad(column, column, const)
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"1231231a", "123123ab", "12312abc", "1231abcd", "123abcde"}),
            executeFunction("lpadUTF8",
                            createColumn<Nullable<String>>({"a", "ab", "abc", "abcd", "abcde"}),
                            createColumn<Nullable<UInt64>>({8, 8, 8, 8, 8}),
                            createConstColumn<Nullable<String>>(5, "123")));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"a1231231", "ab123123", "abc12312", "abcd1231", "abcde123"}),
            executeFunction("rpadUTF8",
                            createColumn<Nullable<String>>({"a", "ab", "abc", "abcd", "abcde"}),
                            createColumn<Nullable<UInt64>>({8, 8, 8, 8, 8}),
                            createConstColumn<Nullable<String>>(5, "123")));
   
    // pad(column, column, column)
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"123a", "123ab", "123abc", "121abcd", "111abcde"}),
            executeFunction("lpadUTF8",
                            createColumn<Nullable<String>>({"a", "ab", "abc", "abcd", "abcde"}),
                            createColumn<Nullable<UInt64>>({4, 5, 6, 7, 8}),
                            createColumn<Nullable<String>>({"12345", "1234", "123", "12", "1"})));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"a123", "ab123", "abc123", "abcd121", "abcde111"}),
            executeFunction("rpadUTF8",
                            createColumn<Nullable<String>>({"a", "ab", "abc", "abcd", "abcde"}),
                            createColumn<Nullable<UInt64>>({4, 5, 6, 7, 8}),
                            createColumn<Nullable<String>>({"12345", "1234", "123", "12", "1"})));
}
CATCH

TEST_F(StringPad, special_argument)
    try
{
    // Empty str.
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"123 ", "12312", "ab", "    def"}),
            executeFunction("lpadUTF8",
                            createColumn<Nullable<String>>({" ", "", "abc", "def"}),
                            createColumn<Nullable<UInt64>>({4, 5, 2, 7}),
                            createColumn<Nullable<String>>({"123", "123", "", " "})));
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({" 123", "12312", "ab", "def    "}),
            executeFunction("rpadUTF8", createColumn<Nullable<String>>({" ", "", "abc", "def"}), createColumn<Nullable<UInt64>>({4, 5, 2, 7}),
                            createColumn<Nullable<String>>({"123", "123", "", " "})));
    // Chinese.
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"你 好 ", "杭州 西湖", "¿¿¿¿!!"}),
            executeFunction("lpadUTF8",
                            createColumn<Nullable<String>>({" ", "西湖", "!!"}),
                            createColumn<Nullable<UInt64>>({4, 5, 6}),
                            createColumn<Nullable<String>>({"你 好", "杭州 ll", "¿¿¿¿¿¿"})));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({" 你 好", "西湖杭州 ", "!!¿¿¿¿"}),
            executeFunction("rpadUTF8",
                            createColumn<Nullable<String>>({" ", "西湖", "!!"}),
                            createColumn<Nullable<UInt64>>({4, 5, 6}),
                            createColumn<Nullable<String>>({"你 好", "杭州 ll", "¿¿¿¿¿¿"})));

    // Length value is zero.
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"", "", "", "", ""}),
            executeFunction("lpadUTF8",
                            createColumn<Nullable<String>>({" ", "西湖", "!!", "test", "test1"}),
                            createConstColumn<Nullable<UInt64>>(5, 0),
                            createColumn<Nullable<String>>({"你 好", "杭州 ll", "¿¿¿¿¿¿", "123", " "})));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"", "", "", "", ""}),
            executeFunction("rpadUTF8",
                            createColumn<Nullable<String>>({" ", "西湖", "!!", "test", "test1"}),
                            createConstColumn<Nullable<UInt64>>(5, 0),
                            createColumn<Nullable<String>>({"你 好", "杭州 ll", "¿¿¿¿¿¿", "123", " "})));
    
    // Length value is less than zero. Expect Null.
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({{}, "a"}),
            executeFunction("lpadUTF8",
                            createColumn<Nullable<String>>({"abc", "abc"}),
                            createColumn<Nullable<Int64>>({-1, 1}),
                            createColumn<Nullable<String>>({"123", "123"})));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({{}, "a"}),
            executeFunction("rpadUTF8",
                            createColumn<Nullable<String>>({"abc", "abc"}),
                            createColumn<Nullable<Int64>>({-1, 1}),
                            createColumn<Nullable<String>>({"123", "123"})));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({{}, "a"}),
            executeFunction("lpadUTF8",
                            createColumn<Nullable<String>>({"abc", "abc"}),
                            createColumn<Nullable<Int8>>({-1, 1}),
                            createColumn<Nullable<String>>({"123", "123"})));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({{}, "a"}),
            executeFunction("rpadUTF8",
                            createColumn<Nullable<String>>({"abc", "abc"}),
                            createColumn<Nullable<Int8>>({-1, 1}),
                            createColumn<Nullable<String>>({"123", "123"})));

    // Test padding_str is empty.
    // lpad("abc", 10, "") -> NULL
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({{}, {}, "xy"}),
            executeFunction("lpadUTF8",
                            createColumn<Nullable<String>>({"abc", "def", "xyz"}),
                            createColumn<Nullable<UInt64>>({10, 100, 2}),
                            createColumn<Nullable<String>>({"", "", ""})));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({{}, {}, "xy"}),
            executeFunction("rpadUTF8",
                            createColumn<Nullable<String>>({"abc", "def", "xyz"}),
                            createColumn<Nullable<UInt64>>({10, 100, 2}),
                            createColumn<Nullable<String>>({"", "", ""})));
    // Null test
    // Result is Null if any column is all null.
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({{}, {}, {}}),
            executeFunction("lpadUTF8",
                            createColumn<Nullable<String>>({{}, "abc", "def"}),
                            createColumn<Nullable<UInt64>>({10, {}, 11}),
                            createColumn<Nullable<String>>({"123", "456", {}})));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({{}, {}, {}}),
            executeFunction("rpadUTF8",
                            createColumn<Nullable<String>>({{}, "abc", "def"}),
                            createColumn<Nullable<UInt64>>({10, {}, 11}),
                            createColumn<Nullable<String>>({"123", "456", {}})));

    // Test non-Nullable column.
    // Result is nullable even if arguments are all non-nullable.
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "xxxxxxxxxabc"),
            executeFunction("lpadUTF8",
                            createConstColumn<String>(5, "abc"),
                            createConstColumn<UInt8>(5, 12),
                            createConstColumn<String>(5, "xxx")));

    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "abcxxxxxxxxx"),
            executeFunction("rpadUTF8",
                            createConstColumn<String>(5, "abc"),
                            createConstColumn<UInt8>(5, 12),
                            createConstColumn<String>(5, "xxx")));
}
CATCH

TEST_F(StringPad, different_col_type)
    try
{
    // Different type of length column.
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "xxxxxxxxxabc"),
            executeFunction("lpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<UInt8>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "xxxxxxxxxabc"),
            executeFunction("lpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<UInt16>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "xxxxxxxxxabc"),
            executeFunction("lpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<UInt32>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "xxxxxxxxxabc"),
            executeFunction("lpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<UInt64>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "xxxxxxxxxabc"),
            executeFunction("lpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<Int8>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "xxxxxxxxxabc"),
            executeFunction("lpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<Int16>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "xxxxxxxxxabc"),
            executeFunction("lpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<Int32>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "xxxxxxxxxabc"),
            executeFunction("lpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<Int64>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    // rpad
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "abcxxxxxxxxx"),
            executeFunction("rpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<UInt8>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "abcxxxxxxxxx"),
            executeFunction("rpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<UInt16>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "abcxxxxxxxxx"),
            executeFunction("rpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<UInt32>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "abcxxxxxxxxx"),
            executeFunction("rpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<UInt64>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "abcxxxxxxxxx"),
            executeFunction("rpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<Int8>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "abcxxxxxxxxx"),
            executeFunction("rpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<Int16>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "abcxxxxxxxxx"),
            executeFunction("rpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<Int32>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
    ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(5, "abcxxxxxxxxx"),
            executeFunction("rpadUTF8",
                            createConstColumn<Nullable<String>>(5, "abc"),
                            createConstColumn<Nullable<Int64>>(5, 12),
                            createConstColumn<Nullable<String>>(5, "xxx")));
}
CATCH

} // namespace tests
} // namespace DB
