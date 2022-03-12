#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsDateTime.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class TestLastDay : public DB::tests::FunctionTest
{
};

TEST_F(TestLastDay, BasicTest)
try
{
    const String func_name = TiDBLastDayTransformerImpl<DataTypeMyDate::FieldType>::name;

    // Ignore invalid month error
    DAGContext * dag_context = context.getDAGContext();
    UInt64 ori_flags = dag_context->getFlags();
    dag_context->addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);
    dag_context->clearWarnings();

    // nullable column test
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<MyDate>>({MyDate{2001, 2, 28}.toPackedUInt(),
                                        MyDate{2000, 2, 29}.toPackedUInt(),
                                        MyDate{2000, 6, 30}.toPackedUInt(),
                                        MyDate{2000, 5, 31}.toPackedUInt(),
                                        {}}),
        executeFunction(func_name,
                        {createColumn<MyDate>({MyDate{2001, 2, 10}.toPackedUInt(),
                                               MyDate{2000, 2, 10}.toPackedUInt(),
                                               MyDate{2000, 6, 10}.toPackedUInt(),
                                               MyDate{2000, 5, 10}.toPackedUInt(),
                                               MyDate{2000, 0, 10}.toPackedUInt()})}));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<MyDate>>({MyDate{2001, 2, 28}.toPackedUInt(),
                                        MyDate{2000, 2, 29}.toPackedUInt(),
                                        MyDate{2000, 6, 30}.toPackedUInt(),
                                        MyDate{2000, 5, 31}.toPackedUInt(),
                                        {}}),
        executeFunction(func_name,
                        {createColumn<MyDateTime>({MyDateTime{2001, 2, 10, 10, 10, 10, 0}.toPackedUInt(),
                                                   MyDateTime{2000, 2, 10, 10, 10, 10, 0}.toPackedUInt(),
                                                   MyDateTime{2000, 6, 10, 10, 10, 10, 0}.toPackedUInt(),
                                                   MyDateTime{2000, 5, 10, 10, 10, 10, 0}.toPackedUInt(),
                                                   MyDateTime{2000, 0, 10, 10, 10, 10, 0}.toPackedUInt()})}));

    // const test
    UInt64 input[] = {
        MyDateTime{2001, 2, 10, 10, 10, 10, 0}.toPackedUInt(),
        MyDateTime{2000, 2, 10, 10, 10, 10, 0}.toPackedUInt(),
        MyDateTime{2000, 6, 10, 10, 10, 10, 0}.toPackedUInt(),
        MyDateTime{2000, 5, 10, 10, 10, 10, 0}.toPackedUInt(),
    };

    UInt64 output[] = {
        MyDate{2001, 2, 28}.toPackedUInt(),
        MyDate{2000, 2, 29}.toPackedUInt(),
        MyDate{2000, 6, 30}.toPackedUInt(),
        MyDate{2000, 5, 31}.toPackedUInt(),
    };

    for (size_t i = 0; i < sizeof(input) / sizeof(UInt64); ++i)
    {
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<MyDate>>(3, output[i]),
            executeFunction(func_name,
                            {createConstColumn<MyDate>(3, input[i])}));
    }

    // special const test, month is zero.
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<MyDate>>(3, {}),
        executeFunction(func_name,
                        {createConstColumn<MyDate>(3, MyDateTime{2000, 0, 10, 10, 10, 10, 0}.toPackedUInt())}));

    dag_context->setFlags(ori_flags);
}
CATCH

} // namespace tests
} // namespace DB
