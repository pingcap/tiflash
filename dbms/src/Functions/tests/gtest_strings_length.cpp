#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <Poco/Types.h>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{
class StringLength : public DB::tests::FunctionTest
{
};

TEST_F(StringLength, length)
{
    {
        // test const
        ASSERT_COLUMN_EQ(createConstColumn<Int64>(0, 0), executeFunction("length", createConstColumn<String>(0, "")));
        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(1, 3),
            executeFunction("length", createConstColumn<String>(1, "aaa")));
        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(3, 3),
            executeFunction("length", createConstColumn<String>(3, "aaa")));
    }

    {
        // test vec
        ASSERT_COLUMN_EQ(createColumn<Int64>({}), executeFunction("length", createColumn<String>({})));

        ASSERT_COLUMN_EQ(
            createColumn<Int64>({0, 3, 5, 7, 6, 9, 0, 9, 16, 0}),
            executeFunction(
                "length",
                createColumn<String>(
                    {"", "hi~", "23333", "pingcap", "你好", "233哈哈", "", "asdの的", "ヽ(￣▽￣)و", ""})));
    }

    {
        // test nullable const
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<Int64>>(0, {}),
            executeFunction("length", createConstColumn<Nullable<String>>(0, "aaa")));
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<Int64>>(1, {3}),
            executeFunction("length", createConstColumn<Nullable<String>>(1, "aaa")));
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<Int64>>(3, {3}),
            executeFunction("length", createConstColumn<Nullable<String>>(3, "aaa")));
    }

    {
        // test nullable vec
        std::vector<Int32> null_map{1, 0, 1, 0, 0, 1};
        ASSERT_COLUMN_EQ(
            createNullableColumn<Int64>({0, 4, 0, 6, 6, 0}, null_map),
            executeFunction(
                "length",
                createNullableColumn<String>({"a", "abcd", "嗯", "饼干", "馒头", "?？?"}, null_map)));
    }
}
} // namespace tests
} // namespace DB
