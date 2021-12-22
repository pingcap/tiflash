#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class StringTiDBConcatWS : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "tidbConcatWS";
};

TEST_F(StringTiDBConcatWS, TwoArgsTest)
try
{
    /// column, column
    for (const auto * separator : {"", "www.pingcap", "中文.测.试。。。"})
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", ""}),
            executeFunction(
                StringTiDBConcatWS::func_name,
                createColumn<Nullable<String>>({separator, separator, separator, separator}),
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));
    }
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({{}, {}, {}, {}}),
        executeFunction(
            StringTiDBConcatWS::func_name,
            createColumn<Nullable<String>>({{}, {}, {}, {}}),
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));
    ASSERT_COLUMN_EQ(
        createOnlyNullColumn(4),
        executeFunction(
            StringTiDBConcatWS::func_name,
            createOnlyNullColumn(4),
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "", "", {}}),
        executeFunction(
            StringTiDBConcatWS::func_name,
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
            createOnlyNullColumn(4)));

    /// column, const
    for (const auto * value : {"", "www.pingcap", "中文.测.试。。。"})
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({value, value, value, {}}),
            executeFunction(
                StringTiDBConcatWS::func_name,
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
                createConstColumn<Nullable<String>>(4, value)));
    }
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "", "", {}}),
        executeFunction(
            StringTiDBConcatWS::func_name,
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
            createOnlyNullColumnConst(4)));

    /// const, column
    for (const auto * separator : {"", "www.pingcap", "中文.测.试。。。"})
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", ""}),
            executeFunction(
                StringTiDBConcatWS::func_name,
                createConstColumn<Nullable<String>>(4, separator),
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));
    }
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({{}, {}, {}, {}}),
        executeFunction(
            StringTiDBConcatWS::func_name,
            createConstColumn<Nullable<String>>(4, {}),
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(4),
        executeFunction(
            StringTiDBConcatWS::func_name,
            createOnlyNullColumnConst(4),
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));

    /// const, const
    for (const auto * separator : {"", "www.pingcap", "中文.测.试。。。"})
    {
        for (const auto * value : {"", "www.pingcap", "中文.测.试。。。"})
        {
            ASSERT_COLUMN_EQ(
                createConstColumn<Nullable<String>>(1, value),
                executeFunction(
                    StringTiDBConcatWS::func_name,
                    createConstColumn<Nullable<String>>(1, separator),
                    createConstColumn<Nullable<String>>(1, value)));
        }
    }
    auto null_consts = {createOnlyNullColumn(1), createConstColumn<Nullable<String>>(1, {})};
    for (auto & null_const : null_consts)
    {
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(1, ""),
            executeFunction(
                StringTiDBConcatWS::func_name,
                createConstColumn<Nullable<String>>(1, "中文.测.试。。。"),
                null_const));
        ASSERT_COLUMN_EQ(
            null_const,
            executeFunction(
                StringTiDBConcatWS::func_name,
                null_const,
                createConstColumn<Nullable<String>>(1, "中文.测.试。。。")));
        for (auto & null_const_ : null_consts)
        {
            ASSERT_COLUMN_EQ(
                null_const,
                executeFunction(
                    StringTiDBConcatWS::func_name,
                    null_const,
                    null_const_));
        }
    }
}
CATCH

TEST_F(StringTiDBConcatWS, ThreeArgsTest)
try
{
}
CATCH

} // namespace DB::tests
