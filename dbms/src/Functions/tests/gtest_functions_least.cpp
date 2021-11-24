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
        createColumn<Int64>({1}),
        executeFunction(
            func_name,
            createColumn<Int8>({2}),
            createColumn<Int16>({1}),
            createColumn<Int8>({3}),
            createColumn<Int8>({4}),
            createColumn<Int8>({5})
            ));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({1}),
        executeFunction(
            func_name,
            createColumn<Int8>({2}),
            createColumn<Int32>({1}),
            createColumn<Int8>({3}),
            createColumn<Int8>({4}),
            createColumn<Int8>({5})
            ));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({1}),
        executeFunction(
            func_name,
            createColumn<Int8>({2}),
            createColumn<Int32>({1}),
            createColumn<Int64>({3}),
            createColumn<Int16>({4}),
            createColumn<Int8>({5})
            ));

}
CATCH
} // namespace DB::tests
