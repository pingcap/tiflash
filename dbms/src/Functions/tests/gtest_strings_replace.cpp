#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
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
ColumnWithTypeAndName buildDataColumn(
        const String & name,
        const std::vector<String> & data)
{
    MutableColumnPtr col = ColumnString::create();
    for (const auto & s : data)
    {
        col->insert(Field(s.c_str(), s.size()));
    }
    return ColumnWithTypeAndName(std::move(col), std::make_shared<DataTypeString>(), name);
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

std::vector<String> executeReplace(
        const FunctionBuilderPtr & funcBuilder,
        const std::vector<String> & rawStrs,
        const String & needleStr,
        const String & replacementStr)
{
    ColumnWithTypeAndName data = buildDataColumn("test", rawStrs);
    ColumnWithTypeAndName needle = buildConstColumn("needle", needleStr, rawStrs.size());
    ColumnWithTypeAndName replacement = buildConstColumn("replacement", replacementStr, rawStrs.size());

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
} // namespace


TEST_F(StringReplace, string_replace_all_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();

    auto bp = factory.tryGet("replaceAll", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(!bp->isVariadic());
    EXPECT_EQ(bp->getNumberOfArguments(), static_cast<size_t>(3));

    std::vector<String> data{"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"};
    std::vector<String> actual = executeReplace(bp, data, " ", "");
    std::vector<String> expect{"hello", "hello", "hello", "", "hello,world"};
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
}

TEST_F(StringReplace, string_replace_all_utf_8_unit_Test)
{
    const Context context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();

    auto bp = factory.tryGet("replaceAll", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(!bp->isVariadic());
    EXPECT_EQ(bp->getNumberOfArguments(), static_cast<size_t>(3));

    std::vector<String> data{"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"};
    std::vector<String> actual = executeReplace(bp, data, "你好", "");
    std::vector<String> expect{"     ", "   你 好", " ", "你 好     ", "你不好"};
    EXPECT_EQ(expect, actual);

    data = {"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"};
    actual = executeReplace(bp, data, "你", "您");
    expect = {"  您好   ", "   您 好", "您好 您好", "您 好     ", "您不好"};
    EXPECT_EQ(expect, actual);
}

} // namespace tests
} // namespace DB

