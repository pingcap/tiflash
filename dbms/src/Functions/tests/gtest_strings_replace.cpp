#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
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


class StringReplace: public ::testing::Test
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

namespace
{
ColumnWithTypeAndName buildStringColumn(
        const String & name,
        const std::vector<String> & data)
{
    auto col = ColumnString::create();
    for (const auto & s : data)
    {
        col->insertData(s.data(), s.size());
    }
    return ColumnWithTypeAndName(std::move(col), std::make_shared<DataTypeString>(), name);
}

ColumnWithTypeAndName buildFixedStringColumn(
        const String & name,
        size_t n,
        const std::vector<String> & data)
{
    auto col = ColumnFixedString::create(n);
    for (const auto & s : data)
    {
        col->insertData(s.data(), std::min(n, s.size()));
    }
    return ColumnWithTypeAndName(std::move(col), std::make_shared<DataTypeFixedString>(n), name);
}

ColumnWithTypeAndName buildConstColumn(
        const String & name,
        const String & str,
        size_t size)
{
    MutableColumnPtr nestCol = ColumnString::create();
    nestCol->insert(Field(str.c_str(), str.size()));
    MutableColumnPtr col = ColumnConst::create(nestCol->getPtr(), size);
    return ColumnWithTypeAndName(std::move(col), std::make_shared<DataTypeString>(), name);
}

std::vector<String> executeReplaceImpl(
        const FunctionBuilderPtr & funcBuilder,
        const ColumnWithTypeAndName & data,
        const ColumnWithTypeAndName & needle,
        const ColumnWithTypeAndName & replacement)
{
    Block block;
    block.insert(data);
    block.insert(needle);
    block.insert(replacement);
    // result
    block.insert({});
    ColumnNumbers cns{0, 1, 2};

    funcBuilder->build({data, needle, replacement})->execute(block, cns, 3);
    const IColumn * res = block.getByPosition(3).column.get();
    const ColumnString & strRes = *checkAndGetColumn<ColumnString>(res);

    std::vector<String> result;
    for (size_t i = 0; i < strRes.size(); ++i)
    {
        result.push_back(strRes[i].get<String>());
    }
    return result;
}

std::vector<String> executeReplace(
        const FunctionBuilderPtr & funcBuilder,
        const std::vector<String> & rawStrs,
        const String & needleStr,
        const String & replacementStr)
{
    ColumnWithTypeAndName data = buildStringColumn("test", rawStrs);
    ColumnWithTypeAndName needle = buildConstColumn("needle", needleStr, rawStrs.size());
    ColumnWithTypeAndName replacement = buildConstColumn("replacement", replacementStr, rawStrs.size());

    return executeReplaceImpl(funcBuilder, data, needle, replacement);
}

std::vector<String> executeReplace(
        const FunctionBuilderPtr & funcBuilder,
        const std::vector<String> & rawStrs,
        const std::vector<String> & needleStrs,
        const String & replacementStr)
{
    ColumnWithTypeAndName data = buildStringColumn("test", rawStrs);
    ColumnWithTypeAndName needle = buildStringColumn("needle", needleStrs);
    ColumnWithTypeAndName replacement = buildConstColumn("replacement", replacementStr, rawStrs.size());

    return executeReplaceImpl(funcBuilder, data, needle, replacement);
}

std::vector<String> executeReplace(
        const FunctionBuilderPtr & funcBuilder,
        const std::vector<String> & rawStrs,
        const String & needleStr,
        const std::vector<String> & replacementStrs)
{
    ColumnWithTypeAndName data = buildStringColumn("test", rawStrs);
    ColumnWithTypeAndName needle = buildConstColumn("needle", needleStr, rawStrs.size());
    ColumnWithTypeAndName replacement = buildStringColumn("replacement", replacementStrs);

    return executeReplaceImpl(funcBuilder, data, needle, replacement);
}

std::vector<String> executeReplace(
        const FunctionBuilderPtr & funcBuilder,
        const std::vector<String> & rawStrs,
        const std::vector<String> & needleStrs,
        const std::vector<String> & replacementStrs)
{
    ColumnWithTypeAndName data = buildStringColumn("test", rawStrs);
    ColumnWithTypeAndName needle = buildStringColumn("needle", needleStrs);
    ColumnWithTypeAndName replacement = buildStringColumn("replacement", replacementStrs);

    return executeReplaceImpl(funcBuilder, data, needle, replacement);
}

std::vector<String> executeReplaceFixed(
        const FunctionBuilderPtr & funcBuilder,
        size_t n,
        const std::vector<String> & rawStrs,
        const String & needleStr,
        const String & replacementStr)
{
    ColumnWithTypeAndName data = buildFixedStringColumn("test", n, rawStrs);
    ColumnWithTypeAndName needle = buildConstColumn("needle", needleStr, rawStrs.size());
    ColumnWithTypeAndName replacement = buildConstColumn("replacement", replacementStr, rawStrs.size());

    return executeReplaceImpl(funcBuilder, data, needle, replacement);
}

std::vector<String> executeReplaceFixed(
        const FunctionBuilderPtr & funcBuilder,
        size_t n,
        const std::vector<String> & rawStrs,
        const std::vector<String> & needleStrs,
        const String & replacementStr)
{
    ColumnWithTypeAndName data = buildFixedStringColumn("test", n, rawStrs);
    ColumnWithTypeAndName needle = buildStringColumn("needle", needleStrs);
    ColumnWithTypeAndName replacement = buildConstColumn("replacement", replacementStr, rawStrs.size());

    return executeReplaceImpl(funcBuilder, data, needle, replacement);
}

std::vector<String> executeReplaceFixed(
        const FunctionBuilderPtr & funcBuilder,
        size_t n,
        const std::vector<String> & rawStrs,
        const String & needleStr,
        const std::vector<String> & replacementStrs)
{
    ColumnWithTypeAndName data = buildFixedStringColumn("test", n, rawStrs);
    ColumnWithTypeAndName needle = buildConstColumn("needle", needleStr, rawStrs.size());
    ColumnWithTypeAndName replacement = buildStringColumn("replacement", replacementStrs);

    return executeReplaceImpl(funcBuilder, data, needle, replacement);
}

std::vector<String> executeReplaceFixed(
        const FunctionBuilderPtr & funcBuilder,
        size_t n,
        const std::vector<String> & rawStrs,
        const std::vector<String> & needleStrs,
        const std::vector<String> & replacementStrs)
{
    ColumnWithTypeAndName data = buildFixedStringColumn("test", n, rawStrs);
    ColumnWithTypeAndName needle = buildStringColumn("needle", needleStrs);
    ColumnWithTypeAndName replacement = buildStringColumn("replacement", replacementStrs);

    return executeReplaceImpl(funcBuilder, data, needle, replacement);
}

String padZero(const String & s, size_t n)
{
    String res = s;
    res.resize(n, '\0');
    return res;
}
} // namespace


TEST_F(StringReplace, string_replace_all_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();

    auto bp = factory.tryGet("replaceAll", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(!bp->isVariadic());
    EXPECT_EQ(bp->getNumberOfArguments(), static_cast<size_t>(3));

    std::vector<String> data;
    std::vector<String> needle;
    std::vector<String> replacement;
    std::vector<String> expect;
    std::vector<String> actual;

    /// const needle and const replacement
    data = {"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"};
    actual = executeReplace(bp, data, " ", "");
    expect = {"hello", "hello", "hello", "", "hello,world"};
    EXPECT_EQ(expect, actual);

    data = {"", "w", "ww", " www ", "w w w"};
    actual = executeReplace(bp, data, "w", "ww");
    expect = {"", "ww", "wwww", " wwwwww ", "ww ww ww"};
    EXPECT_EQ(expect, actual);

    data = {"", "w", "ww", " www ", "w w w"};
    actual = executeReplace(bp, data, "ww", "w");
    expect = {"", "w", "w", " ww ", "w w w"};
    EXPECT_EQ(expect, actual);

    data = {"", "w", "ww", " www ", "w w w"};
    actual = executeReplace(bp, data, "", " ");
    EXPECT_EQ(data, actual);

    /// non-const needle and const replacement
    data = {"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"};
    needle = {" ", "h", "", "h", ","};
    actual = executeReplace(bp, data, needle, "");
    expect = {"hello", "    e llo", "hello    ", "     ", "hello world"};
    EXPECT_EQ(expect, actual);

    data = {"", "w", "ww", " www ", "w w w"};
    needle = {" ", "w", "w", "www", " w"};
    actual = executeReplace(bp, data, needle, "ww");
    expect = {"", "ww", "wwww", " ww ", "wwwww"};
    EXPECT_EQ(expect, actual);

    /// const needle and non-const replacement
    data = {"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"};
    replacement = {"", "x", "xx", " ", ","};
    actual = executeReplace(bp, data, " ", replacement);
    expect = {"hello", "xxxhxexllo", "helloxxxxxxxx", "     ", "hello,,world"};
    EXPECT_EQ(expect, actual);

    /// non-const needle and non-const replacement
    data = {"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"};
    needle = {" ", "h", "", "h", ","};
    replacement = {"", "x", "xx", " ", ","};
    actual = executeReplace(bp, data, needle, replacement);
    expect = {"hello", "   x e llo", "hello    ", "     ", "hello, world"};
    EXPECT_EQ(expect, actual);
}

TEST_F(StringReplace, string_replace_all_utf_8_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();

    auto bp = factory.tryGet("replaceAll", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(!bp->isVariadic());
    EXPECT_EQ(bp->getNumberOfArguments(), static_cast<size_t>(3));

    std::vector<String> data;
    std::vector<String> needle;
    std::vector<String> replacement;
    std::vector<String> expect;
    std::vector<String> actual;

    /// const needle and const replacement
    data = {"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"};
    actual = executeReplace(bp, data, "你好", "");
    expect = {"     ", "   你 好", " ", "你 好     ", "你不好"};
    EXPECT_EQ(expect, actual);

    data = {"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"};
    actual = executeReplace(bp, data, "你", "您");
    expect = {"  您好   ", "   您 好", "您好 您好", "您 好     ", "您不好"};
    EXPECT_EQ(expect, actual);

    /// non-const needle and const replacement
    data = {"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"};
    needle = {"", " ", "你好", " 你", "你好"};
    actual = executeReplace(bp, data, needle, "");
    expect = {"  你好   ", "你好", " ", "你 好     ", "你不好"};
    EXPECT_EQ(expect, actual);

    data = {"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"};
    needle = {" ", " 你", "你好", " 你", "你好"};
    actual = executeReplace(bp, data, needle, "x");
    expect = {"xx你好xxx", "  x 好", "x x", "你 好     ", "你不好"};
    EXPECT_EQ(expect, actual);

    /// const needle and non-const replacement
    data = {"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"};
    replacement = {"", " 你", "你好", " 你", "你好"};
    actual = executeReplace(bp, data, "你", replacement);
    expect = {"  好   ", "    你 好", "你好好 你好好", " 你 好     ", "你好不好"};
    EXPECT_EQ(expect, actual);

    /// non-const needle and non-const replacement
    data = {"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"};
    needle = {"", " ", "你好", "你 ", "你好"};
    replacement = {" ", " 你", "好", " 你", "你好"};
    actual = executeReplace(bp, data, needle, replacement);
    expect = {"  你好   ", " 你 你 你你 你好", "好 好", " 你好     ", "你不好"};
    EXPECT_EQ(expect, actual);
}

TEST_F(StringReplace, fixed_string_replace_all_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();

    auto bp = factory.tryGet("replaceAll", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(!bp->isVariadic());
    EXPECT_EQ(bp->getNumberOfArguments(), static_cast<size_t>(3));

    std::vector<String> data;
    std::vector<String> needle;
    std::vector<String> replacement;
    std::vector<String> expect;
    std::vector<String> actual;

    /// const needle and const replacement
    data = {"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"};
    actual = executeReplaceFixed(bp, 20, data, " ", "");
    expect = {padZero("hello", 15), padZero("hello", 15), padZero("hello", 16), padZero("", 15), padZero("hello,world", 19)};
    EXPECT_EQ(expect, actual);

    data = {"", "w", "ww", " www ", "w w w"};
    actual = executeReplaceFixed(bp, 10, data, "w", "ww");
    expect = {padZero("", 10), padZero("ww", 11), padZero("wwww", 12), padZero(" wwwwww ", 13), padZero("ww ww ww", 13)};
    EXPECT_EQ(expect, actual);

    data = {"", "w", "ww", " www ", "w w w"};
    actual = executeReplaceFixed(bp, 10, data, "ww", "w");
    expect = {padZero("", 10), padZero("w", 10), padZero("w", 9), padZero(" ww ", 9), padZero("w w w", 10)};
    EXPECT_EQ(expect, actual);

    data = {"", "w", "ww", " www ", "w w w"};
    actual = executeReplaceFixed(bp, 10, data, "", " ");
    expect = {padZero("", 10), padZero("w", 10), padZero("ww", 10), padZero(" www ", 10), padZero("w w w", 10)};
    EXPECT_EQ(expect, actual);

    /// non-const needle and const replacement
    data = {"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"};
    needle = {" ", "h", "", "h", ","};
    actual = executeReplaceFixed(bp, 20, data, needle, "");
    expect = {padZero("hello", 15), padZero("    e llo", 19), padZero("hello    ", 20), padZero("     ", 20), padZero("hello world", 19)};
    EXPECT_EQ(expect, actual);

    data = {"", "w", "ww", " www ", "w w w"};
    needle = {" ", "w", "w", "www", " w"};
    actual = executeReplaceFixed(bp, 10, data, needle, "ww");
    expect = {padZero("", 10), padZero("ww", 11), padZero("wwww", 12), padZero(" ww ", 9), padZero("wwwww", 10)};
    EXPECT_EQ(expect, actual);

    /// const needle and non-const replacement
    data = {"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"};
    replacement = {"", "x", "xx", " ", ","};
    actual = executeReplaceFixed(bp, 20, data, " ", replacement);
    expect = {padZero("hello", 15), padZero("xxxhxexllo", 20), padZero("helloxxxxxxxx", 24), padZero("     ", 20), padZero("hello,,world", 20)};
    EXPECT_EQ(expect, actual);

    /// non-const needle and non-const replacement
    data = {"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"};
    needle = {" ", "h", "", "h", ","};
    replacement = {"", "x", "xx", " ", ","};
    actual = executeReplaceFixed(bp, 20, data, needle, replacement);
    expect = {padZero("hello", 15), padZero("   x e llo", 20), padZero("hello    ", 20), padZero("     ", 20), padZero("hello, world", 20)};
    EXPECT_EQ(expect, actual);
}

} // namespace tests
} // namespace DB

