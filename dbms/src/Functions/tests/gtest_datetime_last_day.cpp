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
        createColumn<Nullable<MyDate>>({MyDate{2001, 2, 28}.toPackedUInt(), MyDate{2000, 2, 29}.toPackedUInt(), MyDate{2000, 6, 30}.toPackedUInt(), MyDate{2000, 5, 31}.toPackedUInt(), {}}),
        executeFunction(func_name,
                        {createColumn<MyDate>({MyDate{2001, 2, 10}.toPackedUInt(), MyDate{2000, 2, 10}.toPackedUInt(), MyDate{2000, 6, 10}.toPackedUInt(), MyDate{2000, 5, 10}.toPackedUInt(), MyDate{2000, 0, 10}.toPackedUInt()})}));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<MyDate>>({MyDate{2001, 2, 28}.toPackedUInt(), MyDate{2000, 2, 29}.toPackedUInt(), MyDate{2000, 6, 30}.toPackedUInt(), MyDate{2000, 5, 31}.toPackedUInt(), {}}),
        executeFunction(func_name,
                        {createColumn<MyDateTime>({MyDateTime{2001, 2, 10, 10, 10, 10, 0}.toPackedUInt(),
                                                   MyDateTime{2000, 2, 10, 10, 10, 10, 0}.toPackedUInt(),
                                                   MyDateTime{2000, 6, 10, 10, 10, 10, 0}.toPackedUInt(),
                                                   MyDateTime{2000, 5, 10, 10, 10, 10, 0}.toPackedUInt(),
                                                   MyDateTime{2000, 0, 10, 10, 10, 10, 0}.toPackedUInt()})}));

    // const test
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<MyDate>>(1, MyDate{2001, 2, 28}.toPackedUInt()),
        executeFunction(func_name,
                        {createConstColumn<MyDate>(1, MyDate{2001, 2, 10}.toPackedUInt())}));

    // no_zero_date sql mode test
    UInt64 ori_sql_mode = dag_context->getSQLMode();
    dag_context->addSQLMode(TiDBSQLMode::NO_ZERO_DATE);

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<MyDate>>({{}}),
        executeFunction(func_name, {createColumn<MyDate>({MyDate{2001, 2, 0}.toPackedUInt()})}));

    dag_context->delSQLMode(TiDBSQLMode::NO_ZERO_DATE);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<MyDate>>({MyDate{2001, 2, 28}.toPackedUInt()}),
        executeFunction(func_name, {createColumn<MyDate>({MyDate{2001, 2, 0}.toPackedUInt()})}));

    dag_context->setSQLMode(ori_sql_mode);
    dag_context->setFlags(ori_flags);
    dag_context->clearWarnings();
}
CATCH

} // namespace tests
} // namespace DB
