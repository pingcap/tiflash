#include <DataTypes/DataTypeNullable.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

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

TEST_F(SubStringIndex, strStringIndexStrTest)
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

    // Test Uint64
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"aaa.bbb.ccc.ddd.eee.fff", "aaa.bbb.ccc.ddd.eee.fff", "aaa.bbb.ccc.ddd.eee.fff", "aaa.bbb.ccc.ddd.eee.fff"}),
        executeFunction(
            "substringIndex",
            createConstColumn<Nullable<String>>(4, "aaa.bbb.ccc.ddd.eee.fff"),
            createColumn<Nullable<String>>({".", ".", ".", "."}),
            createColumn<Nullable<UInt64>>({18446744073709551615llu, 18446744073709551614llu, 18446744073709551613llu, 18446744073709551612llu})));

    // Test Int8
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"begin" + std::string(INT8_MAX - 1, '.'), "begin" + std::string(INT8_MAX - 2, '.'), std::string(INT8_MAX, '.') + "end", std::string(INT8_MAX - 1, '.') + "end"}),
        executeFunction(
            "substringIndex",
            createConstColumn<Nullable<String>>(4, "begin" + std::string(300, '.') + "end"),
            createColumn<Nullable<String>>({".", ".", ".", "."}),
            createColumn<Nullable<Int8>>({INT8_MAX, INT8_MAX - 1, INT8_MIN, INT8_MIN + 1})));

    // Test UInt8
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"begin" + std::string(UINT8_MAX - 1, '.'), "begin" + std::string(UINT8_MAX - 2, '.')}),
        executeFunction(
            "substringIndex",
            createConstColumn<Nullable<String>>(2, "begin" + std::string(300, '.') + "end"),
            createColumn<Nullable<String>>({".", "."}),
            createColumn<Nullable<UInt8>>({UINT8_MAX, UINT8_MAX - 1})));

    // Test Int64
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"ping.cap", "ping.cap", "ping.cap", "ping.cap"}),
        executeFunction(
            "substringIndex",
            createConstColumn<Nullable<String>>(4, "ping.cap"),
            createColumn<Nullable<String>>({".", ".", ".", "."}),
            createColumn<Nullable<Int64>>({INT64_MAX, INT64_MAX - 1, INT64_MIN, INT64_MIN + 1})));

    // Test vector_const_const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"www.pingcap", "www.", "中文.测", "www.www"}),
        executeFunction(
            "substringIndex",
            createColumn<Nullable<String>>({"www.pingcap.com", "www...www", "中文.测.试。。。", "www.www"}),
            createConstColumn<Nullable<String>>(4, "."),
            createConstColumn<Nullable<Int64>>(4, 2)));

    // Test type/nullable/const
    auto data_string_col = createColumn<String>({"www.pingcap.com", "www.pingcap.com", "www.pingcap.com", "www.pingcap.com"});
    auto data_nullable_string_col = createColumn<Nullable<String>>({"www.pingcap.com", "www.pingcap.com", "www.pingcap.com", "www.pingcap.com"});
    auto data_const_col = createConstColumn<String>(4, "www.pingcap.com");

    auto delim_string_col = createColumn<String>({".", ".", ".", "."});
    auto delim_nullable_string_col = createColumn<Nullable<String>>({".", ".", ".", "."});
    auto delim_const_col = createConstColumn<String>(4, ".");

    auto count_int64_col = createColumn<Int64>({2, 2, 2, 2});
    auto count_nullable_int64_col = createColumn<Nullable<Int64>>({2, 2, 2, 2});
    auto count_const_col = createConstColumn<Int64>(4, 2);

    auto result_string_col = createColumn<String>({"www.pingcap", "www.pingcap", "www.pingcap", "www.pingcap"});
    auto result_nullable_string_col = createColumn<Nullable<String>>({"www.pingcap", "www.pingcap", "www.pingcap", "www.pingcap"});
    auto result_const_col = createConstColumn<String>(4, "www.pingcap");
    // Test type, type, type/nullable/const
    ASSERT_COLUMN_EQ(result_string_col, executeFunction("substringIndex", data_string_col, delim_string_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_string_col, delim_string_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_string_col, executeFunction("substringIndex", data_string_col, delim_string_col, count_const_col));
    // Test type, nullable, type/nullable/const
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_string_col, delim_nullable_string_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_string_col, delim_nullable_string_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_string_col, delim_nullable_string_col, count_const_col));
    // Test type, const, type/nullable/const
    ASSERT_COLUMN_EQ(result_string_col, executeFunction("substringIndex", data_string_col, delim_const_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_string_col, delim_const_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_string_col, executeFunction("substringIndex", data_string_col, delim_const_col, count_const_col));

    // Test nullable, type, type/nullable/const
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_nullable_string_col, delim_string_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_nullable_string_col, delim_string_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_nullable_string_col, delim_string_col, count_const_col));
    // Test nullable, nullable, type/nullable/const
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_nullable_string_col, delim_nullable_string_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_nullable_string_col, delim_nullable_string_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_nullable_string_col, delim_nullable_string_col, count_const_col));
    // Test nullable, const, type/nullable/const
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_nullable_string_col, delim_const_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_nullable_string_col, delim_const_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_nullable_string_col, delim_const_col, count_const_col));

    // Test const, type, type/nullable/const
    ASSERT_COLUMN_EQ(result_string_col, executeFunction("substringIndex", data_const_col, delim_string_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_const_col, delim_string_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_string_col, executeFunction("substringIndex", data_const_col, delim_string_col, count_const_col));
    // Test const, nullable, type/nullable/const
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_const_col, delim_nullable_string_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_const_col, delim_nullable_string_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_const_col, delim_nullable_string_col, count_const_col));
    // Test const, const, type/nullable/const
    ASSERT_COLUMN_EQ(result_string_col, executeFunction("substringIndex", data_const_col, delim_const_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction("substringIndex", data_const_col, delim_const_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_const_col, executeFunction("substringIndex", data_const_col, delim_const_col, count_const_col));

    // Test vector, empty const, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "", "", ""}),
        executeFunction(
            "substringIndex",
            createColumn<Nullable<String>>({"www.pingcap.com", "www...www", "中文.测.试。。。", "www.www"}),
            createConstColumn<Nullable<String>>(4, ""),
            createConstColumn<Nullable<Int64>>(4, 2)));

    // Test vector, empty vector, vector
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "", "", ""}),
        executeFunction(
            "substringIndex",
            createColumn<Nullable<String>>({"www.pingcap.com", "www...www", "中文.测.试。。。", "www.www"}),
            createColumn<Nullable<String>>({"", "", "", ""}),
            createColumn<Nullable<Int64>>({2, 2, 2, 2})));
}
CATCH

} // namespace tests
} // namespace DB