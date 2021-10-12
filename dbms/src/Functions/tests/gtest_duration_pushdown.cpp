#include <DataTypes/DataTypeNullable.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <Common/MyDuration.h>
#include <DataTypes/DataTypeMyDuration.h>

#include <string>
#include <vector>

namespace DB
{
    namespace tests
    {
        class DurationPushDown : public ::testing::Test
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

        TEST_F(DurationPushDown, durationPushDownTest)
        try
        {
            ColumnWithTypeAndName result_col(
                    createColumn<Nullable<DataTypeMyDuration::FieldType >>({-1,0,1,{},INT64_MAX,INT64_MIN,(838*3600+59*60+59)*SECOND,-(838*3600+59*60+59)*SECOND}).column,
                    makeNullable(std::make_shared<DataTypeMyDuration>(1)),
                    "result");
            ASSERT_COLUMN_EQ(
                result_col,
                executeFunction(
                        "FunctionConvertDurationFromNanos",
                        createColumn<Nullable<Int64>>({-1,0,1,{},INT64_MAX,INT64_MIN,(838*3600+59*60+59)*SECOND,-(838*3600+59*60+59)*SECOND}),
                        createConstColumn<Int64>(8,1))
                        );
        }
        CATCH
    } // namespace tests
} // namespace DB