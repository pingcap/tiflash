#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
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
class StringReplace : public DB::tests::FunctionTest
{
protected:
    ColumnWithTypeAndName toVec(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }

    ColumnWithTypeAndName toConst(const String & s)
    {
        return createConstColumn<Nullable<String>>(1, s);
    }
};

TEST_F(StringReplace, string_replace_all_unit_Test)
try
{
    /// const needle and const replacement
    ASSERT_COLUMN_EQ(
        toVec({"hello", "hello", "hello", "", "hello,world"}),
        executeFunction(
            "replaceAll",
            toVec({"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"}),
            toConst(" "),
            toConst("")));

    ASSERT_COLUMN_EQ(
        toVec({"", "ww", "wwww", " wwwwww ", "ww ww ww"}),
        executeFunction(
            "replaceAll",
            toVec({"", "w", "ww", " www ", "w w w"}),
            toConst("w"),
            toConst("ww")));

    ASSERT_COLUMN_EQ(
        toVec({"", "w", "w", " ww ", "w w w"}),
        executeFunction(
            "replaceAll",
            toVec({"", "w", "ww", " www ", "w w w"}),
            toConst("ww"),
            toConst("w")));

    ASSERT_COLUMN_EQ(
        toVec({"", "w", "ww", " www ", "w w w"}),
        executeFunction(
            "replaceAll",
            toVec({"", "w", "ww", " www ", "w w w"}),
            toConst(""),
            toConst(" ")));

    /// non-const needle and const replacement
    ASSERT_COLUMN_EQ(
        toVec({"hello", "    e llo", "hello    ", "     ", "hello world"}),
        executeFunction(
            "replaceAll",
            toVec({"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"}),
            toVec({" ", "h", "", "h", ","}),
            toConst("")));

    ASSERT_COLUMN_EQ(
        toVec({"", "ww", "wwww", " ww ", "wwwww"}),
        executeFunction(
            "replaceAll",
            toVec({"", "w", "ww", " www ", "w w w"}),
            toVec({" ", "w", "w", "www", " w"}),
            toConst("ww")));

    /// const needle and non-const replacement
    ASSERT_COLUMN_EQ(
        toVec({"hello", "xxxhxexllo", "helloxxxxxxxx", "     ", "hello,,world"}),
        executeFunction(
            "replaceAll",
            toVec({"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"}),
            toConst(" "),
            toVec({"", "x", "xx", " ", ","})));

    /// non-const needle and non-const replacement
    ASSERT_COLUMN_EQ(
        toVec({"hello", "   x e llo", "hello    ", "     ", "hello, world"}),
        executeFunction(
            "replaceAll",
            toVec({"  hello   ", "   h e llo", "hello    ", "     ", "hello, world"}),
            toVec({" ", "h", "", "h", ","}),
            toVec({"", "x", "xx", " ", ","})));
}
CATCH

TEST_F(StringReplace, string_replace_all_utf_8_unit_Test)
try
{
    /// const needle and const replacement
    ASSERT_COLUMN_EQ(
        toVec({"     ", "   你 好", " ", "你 好     ", "你不好"}),
        executeFunction(
            "replaceAll",
            toVec({"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"}),
            toConst("你好"),
            toConst("")));

    ASSERT_COLUMN_EQ(
        toVec({"  您好   ", "   您 好", "您好 您好", "您 好     ", "您不好"}),
        executeFunction(
            "replaceAll",
            toVec({"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"}),
            toConst("你"),
            toConst("您")));

    /// non-const needle and const replacement
    ASSERT_COLUMN_EQ(
        toVec({"  你好   ", "你好", " ", "你 好     ", "你不好"}),
        executeFunction(
            "replaceAll",
            toVec({"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"}),
            toVec({"", " ", "你好", " 你", "你好"}),
            toConst("")));

    ASSERT_COLUMN_EQ(
        toVec({"xx你好xxx", "  x 好", "x x", "你 好     ", "你不好"}),
        executeFunction(
            "replaceAll",
            toVec({"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"}),
            toVec({" ", " 你", "你好", " 你", "你好"}),
            toConst("x")));

    /// const needle and non-const replacement
    ASSERT_COLUMN_EQ(
        toVec({"  好   ", "    你 好", "你好好 你好好", " 你 好     ", "你好不好"}),
        executeFunction(
            "replaceAll",
            toVec({"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"}),
            toConst("你"),
            toVec({"", " 你", "你好", " 你", "你好"})));

    /// non-const needle and non-const replacement
    ASSERT_COLUMN_EQ(
        toVec({"  你好   ", " 你 你 你你 你好", "好 好", " 你好     ", "你不好"}),
        executeFunction(
            "replaceAll",
            toVec({"  你好   ", "   你 好", "你好 你好", "你 好     ", "你不好"}),
            toVec({"", " ", "你好", "你 ", "你好"}),
            toVec({" ", " 你", "好", " 你", "你好"})));
}
CATCH

} // namespace tests
} // namespace DB
