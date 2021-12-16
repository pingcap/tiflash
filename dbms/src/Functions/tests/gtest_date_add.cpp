#include <Common/MyTime.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class Dateadd : public DB::tests::FunctionTest
{
protected:
    static ColumnWithTypeAndName toNullableVec(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }

    static ColumnWithTypeAndName toStringVec(const std::vector<String> & v)
    {
        return createColumn<String>(v);
    }

    static ColumnWithTypeAndName toIntVec(const std::vector<UInt64> & v)
    {
        return createColumn<UInt64>(v);
    }

    static ColumnWithTypeAndName toConst(const int s)
    {
        return createConstColumn<int>(1, s);
    }

    static ColumnWithTypeAndName toConst(const String & s)
    {
        return createConstColumn<String>(1, s);
    }
};

TEST_F(Dateadd, date_add_string_int_unit_Test)
{

    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-13", "2020-12-14", {}}),
        executeFunction("addDays",toNullableVec({"20121212", "20201212", "20201212"}), toIntVec({1, 2, 2914289}))
        );

    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-13", "2020-12-14", {}}),
        executeFunction("addDays",toStringVec({"20121212", "20201212", "20201212"}), toIntVec({1, 2, 2914289}))
    );
}

} // namespace DB::tests
