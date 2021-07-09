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


TEST_F(TestFunctionUtils, Common)
try
{
    {
        CREATE_TABLE(t, {"a", "Int64"}, {"b", "Int32"});
        INSERT_INTO(t, 1, 2);
        EVAL_FUNC(t, "plus", "a+b", {"a", "b"});
        CREATE_COLUMN(expect, "Int64", {3});
        auto actual = t.getByName("a+b").column;
        ASSERT_EQ(t.getByName("a+b").type->getName(), "Int64");
        ASSERT_EQ(*actual, *expect);
    }

    {
        CREATE_TABLE(t, {"a", "Decimal(10,2)"}, {"b", "Decimal(10,3)"});
        INSERT_INTO(t, 1'23, 1'111);
        EVAL_FUNC(t, "plus", "a+b", {"a", "b"});
        CREATE_COLUMN(expect, "Decimal(12,3)", {2'341});
        auto actual = t.getByName("a+b").column;
        ASSERT_EQ(t.getByName("a+b").type->getName(), "Decimal(12,3)");
        ASSERT_EQ(*actual, *expect);
    }

    {
        CREATE_TABLE(t, {"a", "String"});
        INSERT_INTO(t, "hello world");
        EVAL_FUNC(t, "length", "b", {"a"});
        CREATE_COLUMN(expect, "UInt64", {11});
        auto actual = t.getByName("b").column;
        ASSERT_EQ(t.getByName("b").type->getName(), "UInt64");
        ASSERT_EQ(*actual, *expect);
    }

    {
        CREATE_TABLE(t, {"a", "MyDatetime"});
        INSERT_INTO(t, MyDateTime(2021, 7, 9, 15, 26, 30, 0).toPackedUInt());
        CREATE_COLUMN(fmt, "String", {"%Y-%m-%d"});
        ADD_COLUMN(t, "b", "String", ColumnConst::create(fmt, 1));
        EVAL_FUNC(t, "dateFormat", "c", {"a", "b"});
        CREATE_COLUMN(expect, "String", {"2021-07-09"});
        auto actual = t.getByName("c").column;
        ASSERT_EQ(t.getByName("c").type->getName(), "String");
        ASSERT_EQ(*actual, *expect);
    }
}
CATCH

TEST_F(TestFunctionUtils, DISABLED_Print)
try
{
    CREATE_TABLE(t, {"a", "Int"}, {"b", "String"});
    INSERT_INTO(t, 1, "hello");
    PRINT_TABLE(t);
}
CATCH

} // namespace tests
} // namespace DB
