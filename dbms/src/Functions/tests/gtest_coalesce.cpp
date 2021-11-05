#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class CoalesceTest : public DB::tests::FunctionTest
{
};

TEST_F(CoalesceTest, testOnlyNull)
try
{
    const String & func_name = "coalesce";

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"a"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"a"}),
            createConstColumn<Nullable<String>>(1, std::nullopt)));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"a"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"a"}),
            createOnlyNullColumn(1)));
}
CATCH
} // namespace DB::tests
