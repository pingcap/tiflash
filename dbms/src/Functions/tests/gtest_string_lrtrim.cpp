#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <string>

namespace DB
{
namespace tests
{
class StringLRTrim : public DB::tests::FunctionTest
{
};

TEST_F(StringLRTrim, strLRTrimTest)
try
{
    // ltrim(const)
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, "x "),
        executeFunction("tidbLTrim", createConstColumn<Nullable<String>>(5, " x ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, "测试 "),
        executeFunction("tidbLTrim", createConstColumn<Nullable<String>>(5, " 测试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, "x x x"),
        executeFunction("tidbLTrim", createConstColumn<Nullable<String>>(5, "x x x")));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, "测 试 "),
        executeFunction("tidbLTrim", createConstColumn<Nullable<String>>(5, "测 试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "x "),
        executeFunction("tidbLTrim", createConstColumn<String>(5, " x ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "测试 "),
        executeFunction("tidbLTrim", createConstColumn<String>(5, " 测试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "x x x"),
        executeFunction("tidbLTrim", createConstColumn<String>(5, "x x x")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "测 试 "),
        executeFunction("tidbLTrim", createConstColumn<String>(5, "测 试 ")));

    // rtrim(const)
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, " x"),
        executeFunction("tidbRTrim", createConstColumn<Nullable<String>>(5, " x ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, " 测试"),
        executeFunction("tidbRTrim", createConstColumn<Nullable<String>>(5, " 测试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, "x x x"),
        executeFunction("tidbRTrim", createConstColumn<Nullable<String>>(5, "x x x")));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, "测 试"),
        executeFunction("tidbRTrim", createConstColumn<Nullable<String>>(5, "测 试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, " x"),
        executeFunction("tidbRTrim", createConstColumn<String>(5, " x ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, " 测试"),
        executeFunction("tidbRTrim", createConstColumn<String>(5, " 测试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "x x x"),
        executeFunction("tidbRTrim", createConstColumn<String>(5, "x x x")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "测 试"),
        executeFunction("tidbRTrim", createConstColumn<String>(5, "测 试 ")));


    // ltrim(column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "\t aa \t", "", {}}),
        executeFunction("tidbLTrim", createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}})));
    // rtrim(column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"  xx aa", "  xxaa xx", "\t aa \t", "", {}}),
        executeFunction("tidbRTrim", createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}})));
}
CATCH

} // namespace tests
} // namespace DB
