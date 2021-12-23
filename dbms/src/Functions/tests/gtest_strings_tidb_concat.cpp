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

    /// const
    auto test_const = [&](const InferredFieldType<Nullable<String>> & value) {
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(1, value),
            executeFunction(
                StringTidbConcat::func_name,
                createConstColumn<Nullable<String>>(1, value)));
    };
    test_const("");
    test_const("www.pingcap");
    test_const("中文.测.试。。。");
    test_const({});

    /// only null
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(1),
        executeFunction(
            StringTidbConcat::func_name,
            createOnlyNullColumn(1)));
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
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(4),
        executeFunction(
            StringTidbConcat::func_name,
            createOnlyNullColumnConst(4),
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));

    /// const, const
    auto test_const_const_not_null = [&](const String & left, const String & right, const String & result) {
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
    auto test_const_const_has_null = [&](const InferredFieldType<Nullable<String>> & left, const InferredFieldType<Nullable<String>> & right) {
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(1, {}),
            executeFunction(
                StringTidbConcat::func_name,
                createConstColumn<Nullable<String>>(1, left),
                createConstColumn<Nullable<String>>(1, right)));
    };
    test_const_const_has_null("中文.测.试。。。", {});
    test_const_const_has_null({}, "中文.测.试。。。");
    test_const_const_has_null({}, {});

    /// column, const
    auto test_const_not_null_column = [&](const InferredFieldType<Nullable<String>> & const_value, const InferredDataVector<Nullable<String>> & result) {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>(result),
            executeFunction(
                StringTidbConcat::func_name,
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
                createConstColumn<Nullable<String>>(4, const_value)));
    };
    test_const_not_null_column("", {"", "www.pingcap", "中文.测.试。。。", {}});
    test_const_not_null_column("www.pingcap", {"www.pingcap", "www.pingcapwww.pingcap", "中文.测.试。。。www.pingcap", {}});
    test_const_not_null_column("中文.测.试。。。", {"中文.测.试。。。", "www.pingcap中文.测.试。。。", "中文.测.试。。。中文.测.试。。。", {}});
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(4, {}),
        executeFunction(
            StringTidbConcat::func_name,
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
            createConstColumn<Nullable<String>>(4, {})));

    /// only null
    auto test_only_null = [&](const ColumnWithTypeAndName & left, const ColumnWithTypeAndName & right) {
        assert(left.column->size() == right.column->size());
        ASSERT_COLUMN_EQ(createOnlyNullColumnConst(left.column->size()), executeFunction(StringTidbConcat::func_name, left, right));
    };
    test_only_null(createConstColumn<Nullable<String>>(1, "中文.测.试。。。"), createOnlyNullColumn(1));
    test_only_null(createConstColumn<Nullable<String>>(1, "中文.测.试。。。"), createOnlyNullColumnConst(1));
    test_only_null(createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}), createOnlyNullColumnConst(4));
    test_only_null(createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}), createOnlyNullColumn(4));
    test_only_null(createOnlyNullColumnConst(1), createOnlyNullColumn(1));
    test_only_null(createOnlyNullColumnConst(1), createOnlyNullColumnConst(1));
    test_only_null(createOnlyNullColumn(1), createOnlyNullColumn(1));
}
CATCH

} // namespace DB::tests
