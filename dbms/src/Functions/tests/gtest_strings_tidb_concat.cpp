#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class StringTidbConcat : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "tidbConcat";
};

TEST_F(StringTidbConcat, OneArgTest)
try
{
    /// column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
        executeFunction(
            StringTidbConcat::func_name,
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));

    /// const not null
    auto test_const_not_null = [&](const std::string & value) {
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(1, value),
            executeFunction(
                StringTidbConcat::func_name,
                createConstColumn<Nullable<String>>(1, value)));
    };
    test_const_not_null("");
    test_const_not_null("www.pingcap");
    test_const_not_null("中文.测.试。。。");

    /// only null
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(2),
        executeFunction(
            StringTidbConcat::func_name,
            createOnlyNullColumn(2)));
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(1),
        executeFunction(
            StringTidbConcat::func_name,
            createOnlyNullColumnConst(1)));
}
CATCH

TEST_F(StringTidbConcat, TwoArgsTest)
try
{
    /// column, column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "www.pingcapwww.pingcap", "中文.测.试。。。中文.测.试。。。", {}, "中文.测.试。。。", "中文.测.试。。。www.pingcap", {}, {}}),
        executeFunction(
            StringTidbConcat::func_name,
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}, "中文.测.试。。。", "中文.测.试。。。", "www.pingcap", {}}),
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}, "", "www.pingcap", {}, "www.pingcap"})));

    /// not null const, not null const
    auto test_const_const_not_null = [&](const std::string & left, const std::string & right, const std::string & result) {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({result}),
            executeFunction(
                StringTidbConcat::func_name,
                createConstColumn<Nullable<String>>(1, left),
                createConstColumn<Nullable<String>>(1, right)));
    };
    test_const_const_not_null("", "", "");
    test_const_const_not_null("www.pingcap", "www.pingcap", "www.pingcapwww.pingcap");
    test_const_const_not_null("中文.测.试。。。", "中文.测.试。。。", "中文.测.试。。。中文.测.试。。。");
    test_const_const_not_null("中文.测.试。。。", "", "中文.测.试。。。");
    test_const_const_not_null("中文.测.试。。。", "www.pingcap", "中文.测.试。。。www.pingcap");

    /// column, not null const
    auto test_const_column_not_null = [&](const std::string & const_str, const InferredDataVector<Nullable<String>> & result) {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>(result),
            executeFunction(
                StringTidbConcat::func_name,
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
                createConstColumn<Nullable<String>>(4, const_str)));
    };
    test_const_column_not_null("", {"", "www.pingcap", "中文.测.试。。。", {}});
    test_const_column_not_null("www.pingcap", {"www.pingcap", "www.pingcapwww.pingcap", "中文.测.试。。。www.pingcap", {}});
    test_const_column_not_null("中文.测.试。。。", {"中文.测.试。。。", "www.pingcap中文.测.试。。。", "中文.测.试。。。中文.测.试。。。", {}});

    /// one arg only null
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(2),
        executeFunction(
            StringTidbConcat::func_name,
            createConstColumn<Nullable<String>>(2, "中文.测.试。。。"),
            createOnlyNullColumn(2)));
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(1),
        executeFunction(
            StringTidbConcat::func_name,
            createConstColumn<Nullable<String>>(1, "中文.测.试。。。"),
            createOnlyNullColumnConst(1)));
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(4),
        executeFunction(
            StringTidbConcat::func_name,
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
            createOnlyNullColumnConst(4)));
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(4),
        executeFunction(
            StringTidbConcat::func_name,
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
            createOnlyNullColumn(4)));

    /// two args only null
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(2),
        executeFunction(
            StringTidbConcat::func_name,
            createOnlyNullColumnConst(2),
            createOnlyNullColumn(2)));
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(2),
        executeFunction(
            StringTidbConcat::func_name,
            createOnlyNullColumnConst(2),
            createOnlyNullColumnConst(2)));
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(2),
        executeFunction(
            StringTidbConcat::func_name,
            createOnlyNullColumn(2),
            createOnlyNullColumn(2)));
}
CATCH

} // namespace DB::tests
