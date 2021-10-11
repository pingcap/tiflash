#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
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

namespace DB::tests
{
class StringUpper : public ::testing ::Test
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

    ColumnWithTypeAndName toVec(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }

    ColumnWithTypeAndName toConst(const String & s)
    {
        return createConstColumn<Nullable<String>>(1, s);
    }
};

TEST_F(StringUpper, string_upper_all_unit_Test)
{
    ASSERT_COLUMN_EQ(
        toVec({"ONE WEEK’S TIME TEST", "ABC测试DEF", "ABCテストABC", "ЀЁЂЃЄЅІЇЈЉЊЋЌЍЎЏ", "ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩΣ", "ԹՓՁՋՐՉՃԺԾՔՈԵՌՏԸՒԻՕՊԱՍԴՖԳՀՅԿԼԽԶՂՑՎԲՆՄՇ"}),
        executeFunction(
        "upperUTF8",
        toVec({"one week’s time TEST", "abc测试DeF", "AbCテストAbC", "ѐёђѓєѕіїјЉЊЋЌЍЎЏ", "αβγδεζηθικλμνξοπρστυφχψως", "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"})
        ));

    ASSERT_COLUMN_EQ(
        toVec({"one week’s time TEST", "abc测试DeF", "AbCテストAbC", "ѐёђѓєѕіїјЉЊЋЌЍЎЏ", "αβγδεζηθικλμνξοπρστυφχψως", "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"}),
        executeFunction(
            "upperBinary",
            toVec({"one week’s time TEST", "abc测试DeF", "AbCテストAbC", "ѐёђѓєѕіїјЉЊЋЌЍЎЏ", "αβγδεζηθικλμνξοπρστυφχψως", "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"})
        ));
}


} // namespace DB::tests