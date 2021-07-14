#include <Columns/ColumnConst.h>
#include <Functions/registerFunctions.h>
#include <Functions/tests/utils.h>

namespace DB
{
namespace tests
{
class TestFunctionUtils : public ::testing::Test
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

TEST_F(TestFunctionUtils, DISABLED_Print)
try
{
    auto t = Table({{"a", "Int"}, {"b", "String"}}).insert(1, "hello").build();
    PRINT_TABLE(t);
}
CATCH

TEST_F(TestFunctionUtils, Table)
try
{
    {
        auto t = Table({{"a", "Int32"}, {"b", "Int64"}}).insert(1, 1).eval("plus", "a+a", "a", "b").build();
        auto expect = createColumn(DATA_TYPE("Int64"), 2);
        ASSERT_EQ(t.getByName("a+a").type->getName(), "Int64");
        ASSERT_EQ(*t.getByName("a+a").column, *expect);
    }

    {
        auto t = Table({{"a", "Nullable(Int32)"}, {"b", "Int64"}}).insert(1, 1).insert(Null(), 1).eval("plus", "a+a", "a", "b").build();
        auto expect = createColumn(DATA_TYPE("Nullable(Int64)"), 2, Null());
        ASSERT_EQ(t.getByName("a+a").type->getName(), "Nullable(Int64)");
        ASSERT_EQ(*t.getByName("a+a").column, *expect);
    }

    {
        auto t = Table({{"a", "String"}}).insert("Hello, World").insert("Hello, World!\n").eval("length", "b", "a").build();
        auto expect = createColumn(DATA_TYPE("UInt64"), 12, 14);
        ASSERT_EQ(t.getByName("b").type->getName(), "UInt64");
        ASSERT_EQ(*t.getByName("b").column, *expect);
    }
}
CATCH

} // namespace tests
} // namespace DB
