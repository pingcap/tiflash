#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class LeastTest : public DB::tests::FunctionTest
{
};

TEST_F(LeastTest, testInt)
try
{
    const String & func_name = "tidbLeast";

    ASSERT_COLUMN_EQ(
        createColumn<Int32>({1}),
        executeFunction(
            func_name,
            createColumn<Int8>({2}),
            createColumn<Int8>({1}),
            createColumn<Int8>({3}),
            createColumn<Int8>({4}),
            createColumn<Int32>({5})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({7}),
        executeFunction(
            func_name,
            createColumn<Int16>({10}),
            createColumn<Int32>({7}),
            createColumn<Int64>({8})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({7}),
        executeFunction(
            func_name,
            createColumn<Int8>({10}),
            createColumn<Int8>({7}),
            createColumn<Int64>({8})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({1}),
        executeFunction(
            func_name,
            createColumn<Int8>({2}),
            createColumn<Int32>({1}),
            createColumn<Int64>({3}),
            createColumn<Int16>({4}),
            createColumn<Int8>({5})));

    // // consider null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int8>>({}),
            createColumn<Nullable<Int16>>({4}),
            createColumn<Nullable<Int32>>({}),
            createColumn<Nullable<Int64>>({})));
}
CATCH
} // namespace DB::tests
