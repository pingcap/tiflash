#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <Functions/registerFunctions.h>
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
class SubStringIndex : public ::testing::Test
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

TEST_F(SubStringIndex, str_string_index_str_Test)
try
{
    // Test string
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"www.pingcap", "pingcap.com", "", "www.pingcap.com"}),
        executeFunction(
            "substringIndex",
            createColumn<Nullable<String>>(
                {"www.pingcap.com", "www.pingcap.com", "www.pingcap.com", "www.pingcap.com"}),
            createColumn<Nullable<String>>({".", ".", ".", "."}),
            createColumn<Nullable<Int64>>({2, -2, 0, 10})));
    // Test Null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({{}, {}, {}, "www.pingcap.com"}),
        executeFunction(
            "substringIndex",
            createColumn<Nullable<String>>(
                {{}, "www.pingcap.com", "www.pingcap.com", "www.pingcap.com"}),
            createColumn<Nullable<String>>({".", {}, ".", "."}),
            createColumn<Nullable<Int64>>({2, -2, {}, 10})));
    // More Test 1
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"pingcap.com", "www", "www.", ".com", "pingcap.com", ".com"}),
        executeFunction(
            "substringIndex",
            createColumn<Nullable<String>>(
                {"www.pingcap.com", "www.pingcap.com", "www.pingcap.com", "www.pingcap.com", ".www.pingcap.com", ".www.pingcap.com"}),
            createColumn<Nullable<String>>({".", ".", "pingcap", "pingcap", ".", ".pingcap"}),
            createColumn<Nullable<Int64>>({-2, 1, 1, -1, -2, -1})));
    // More Test 2
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "", "aa", "aaaa", "aaaaaa", "aaaaaaaaa1"}),
        executeFunction(
            "substringIndex",
            createColumn<Nullable<String>>(
                {"aaaaaaaaa1", "aaaaaaaaa1", "aaaaaaaaa1", "aaaaaaaaa1", "aaaaaaaaa1", "aaaaaaaaa1"}),
            createColumn<Nullable<String>>({"a", "aa", "aa", "aa", "aa", "aa"}),
            createColumn<Nullable<Int64>>({1, 1, 2, 3, 4, 5})));
    // More Test 3
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "aaa", "aaaaaa", "aaaaaaaaa1", "", "aaaa"}),
        executeFunction(
            "substringIndex",
            createConstColumn<Nullable<String>>(6, "aaaaaaaaa1"),
            createColumn<Nullable<String>>({"aaa", "aaa", "aaa", "aaa", "aaaa", "aaaa"}),
            createColumn<Nullable<Int64>>({1, 2, 3, 4, 1, 2})));
    // More Test 4
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"aaaaaaaaa", "1", "a1", "aaa1", "aaaaa1", "aaaaaaa1", "aaaaaaaaa1"}),
        executeFunction(
            "substringIndex",
            createConstColumn<Nullable<String>>(7, "aaaaaaaaa1"),
            createColumn<Nullable<String>>({"1", "a", "aa", "aa", "aa", "aa", "aa"}),
            createColumn<Nullable<Int64>>({1, -1, -1, -2, -3, -4, -5})));
    // More Test 5
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"1", "aaa1", "aaaaaa1", "aaaaaaaaa1"}),
        executeFunction(
            "substringIndex",
            createConstColumn<Nullable<String>>(4, "aaaaaaaaa1"),
            createColumn<Nullable<String>>({"aaa", "aaa", "aaa", "aaa"}),
            createColumn<Nullable<Int64>>({-1, -2, -3, -4})));

    // Test vector_const_const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"www.pingcap", "www.", "中文.测", "www.www"}),
        executeFunction(
            "substringIndex",
            createColumn<Nullable<String>>({"www.pingcap.com", "www...www", "中文.测.试。。。", "www.www"}),
            createConstColumn<Nullable<String>>(4, "."),
            createConstColumn<Nullable<Int64>>(4, 2)));
}
CATCH

} // namespace tests
} // namespace DB