#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
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
class Sysdate : public DB::tests::FunctionTest
{
protected:
    void ASSERT_CHECK_SYSDATE(const ColumnWithTypeAndName base_col, ColumnWithTypeAndName actual_col, int offset)
    {
        UInt64 base_packed = base_col.column.get()->get64(0);
        MyDateTime base_mdt = MyDateTime(base_packed);
        UInt64 packed = actual_col.column.get()->get64(0);
        MyDateTime mdt = MyDateTime(packed);
        ASSERT_LE(base_packed, packed);
        ASSERT_EQ(((mdt.yearDay() - base_mdt.yearDay()) * 24 + (mdt.hour - base_mdt.hour)), offset);
    }
};

TEST_F(Sysdate, sysdate_all_unit_Test)
{
    auto constColumn = createConstColumn<UInt32>(1, 3);
    ColumnWithTypeAndName base_col = executeFunctionAndSetTimezone(0 * 3600, "sysDateWithFsp", constColumn);

    int offset = 1;
    ASSERT_CHECK_SYSDATE(base_col, executeFunctionAndSetTimezone(offset * 3600, "sysDateWithFsp", constColumn), offset);

    offset = 2;
    ASSERT_CHECK_SYSDATE(base_col, executeFunctionAndSetTimezone(offset * 3600, "sysDateWithFsp", constColumn), offset);

    offset = 3;
    ASSERT_CHECK_SYSDATE(base_col, executeFunctionAndSetTimezone(offset * 3600, "sysDateWithFsp", constColumn), offset);

    offset = 4;
    ASSERT_CHECK_SYSDATE(base_col, executeFunctionAndSetTimezone(offset * 3600, "sysDateWithFsp", constColumn), offset);
}
} // namespace DB::tests
