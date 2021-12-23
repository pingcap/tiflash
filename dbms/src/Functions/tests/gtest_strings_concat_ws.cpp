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
    std::vector<String> not_null_strings = {"", "www.pingcap", "中文.测.试。。。"};

    // column, column
    auto test_not_null_column_column = [&](const String & value) {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", ""}),
            executeFunction(
                StringTiDBConcatWS::func_name,
                createColumn<Nullable<String>>({value, value, value, value}),
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));
    };
    for (const auto & not_null_string : not_null_strings)
        test_not_null_column_column(not_null_string);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({{}, {}, {}, {}}),
        executeFunction(
            StringTiDBConcatWS::func_name,
            createColumn<Nullable<String>>({{}, {}, {}, {}}),
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));

    // column, const
    auto test_not_null_column_const = [&](const String & value) {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", ""}),
            executeFunction(
                StringTiDBConcatWS::func_name,
                createColumn<Nullable<String>>({value, value, value, value}),
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));
    };
    for (const auto & not_null_string : not_null_strings)
        test_not_null_column_const(not_null_string);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({{}, {}, {}, {}}),
        executeFunction(
            StringTiDBConcatWS::func_name,
            createColumn<Nullable<String>>({{}, {}, {}, {}}),
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));

    // const, column
    auto test_not_null_const_column = [&](const String & value) {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", ""}),
            executeFunction(
                StringTiDBConcatWS::func_name,
                createConstColumn<Nullable<String>>(4, value),
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));
    };
    for (const auto & not_null_string : not_null_strings)
        test_not_null_const_column(not_null_string);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({{}, {}, {}, {}}),
        executeFunction(
            StringTiDBConcatWS::func_name,
            createConstColumn<Nullable<String>>(4, {}),
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));

    // const, const
    auto test_const_const = [&](const InferredFieldType<Nullable<String>> & separator, const InferredFieldType<Nullable<String>> & value, const InferredFieldType<Nullable<String>> & result) {
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(1, result),
            executeFunction(
                StringTiDBConcatWS::func_name,
                createConstColumn<Nullable<String>>(1, separator),
                createConstColumn<Nullable<String>>(1, value)));
    };
    for (const auto & separator : not_null_strings)
    {
        for (const auto & value : not_null_strings)
        {
            test_const_const(separator, value, value);
        }
    }

    test_const_const({}, "中文.测.试。。。", {});
    test_const_const({}, {}, {});
    test_const_const("中文.测.试。。。", {}, "");

    // only null
    auto test_const_only_null = [&](const ColumnWithTypeAndName & only_null) {
        ASSERT_COLUMN_EQ(
            only_null.column->isColumnConst() ? createConstColumn<Nullable<String>>(1, "") : createColumn<Nullable<String>>({""}),
            executeFunction(
                StringTiDBConcatWS::func_name,
                createConstColumn<Nullable<String>>(1, "中文.测.试。。。"),
                only_null));
    };
    test_const_only_null(createOnlyNullColumn(1));
    test_const_only_null(createOnlyNullColumnConst(1));

    auto test_column_only_null = [&](const ColumnWithTypeAndName & only_null) {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"", "", "", {}}),
            executeFunction(
                StringTiDBConcatWS::func_name,
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
                only_null));
    };
    test_column_only_null(createOnlyNullColumn(4));
    test_column_only_null(createOnlyNullColumnConst(4));

    auto test_only_null_const = [&](const ColumnWithTypeAndName & only_null) {
        ASSERT_COLUMN_EQ(
            createOnlyNullColumnConst(1),
            executeFunction(
                StringTiDBConcatWS::func_name,
                only_null,
                createConstColumn<Nullable<String>>(1, "中文.测.试。。。")));
    };
    test_only_null_const(createOnlyNullColumn(1));
    test_only_null_const(createOnlyNullColumnConst(1));

    auto test_only_null_column = [&](const ColumnWithTypeAndName & only_null) {
        ASSERT_COLUMN_EQ(
            createOnlyNullColumnConst(4),
            executeFunction(
                StringTiDBConcatWS::func_name,
                only_null,
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));
    };
    test_only_null_column(createOnlyNullColumn(4));
    test_only_null_column(createOnlyNullColumnConst(4));

    auto test_only_null_only_null = [&](const ColumnWithTypeAndName & left, const ColumnWithTypeAndName & right) {
        ASSERT_COLUMN_EQ(createOnlyNullColumnConst(left.column->size()), executeFunction(StringTiDBConcatWS::func_name, left, right));
    };
    test_only_null_only_null(createOnlyNullColumnConst(1), createOnlyNullColumn(1));
    test_only_null_only_null(createOnlyNullColumn(1), createOnlyNullColumn(1));
    test_only_null_only_null(createOnlyNullColumnConst(1), createOnlyNullColumnConst(1));
}
CATCH

TEST_F(StringTiDBConcatWS, ThreeArgsTest)
try
{
    // test null
    std::vector<ColumnWithTypeAndName> nulls = {
        createOnlyNullColumnConst(4),
        createOnlyNullColumn(4),
        createColumn<Nullable<String>>({{}, {}, {}, {}}),
        createConstColumn<Nullable<String>>(4, {})};

    auto test_separator_is_null = [&](const ColumnWithTypeAndName & null_separator) {
        ASSERT_COLUMN_EQ(
            null_separator.type->onlyNull() ? createOnlyNullColumnConst(4) : createColumn<Nullable<String>>({{}, {}, {}, {}}),
            executeFunction(
                StringTiDBConcatWS::func_name,
                null_separator,
                createConstColumn<Nullable<String>>(4, "www.pingcap"),
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));
    };
    for (const auto & item : nulls)
        test_separator_is_null(item);

    auto test_value_is_null = [&](const ColumnWithTypeAndName & null_value) {
        ASSERT_COLUMN_EQ(
            executeFunction(
                StringTiDBConcatWS::func_name,
                createConstColumn<Nullable<String>>(4, "分隔符"),
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})),
            executeFunction(
                StringTiDBConcatWS::func_name,
                createConstColumn<Nullable<String>>(4, "分隔符"),
                null_value,
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})));
        ASSERT_COLUMN_EQ(
            executeFunction(
                StringTiDBConcatWS::func_name,
                createConstColumn<Nullable<String>>(4, "分隔符"),
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}})),
            executeFunction(
                StringTiDBConcatWS::func_name,
                createConstColumn<Nullable<String>>(4, "分隔符"),
                createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
                null_value));
        ASSERT_COLUMN_EQ(
            executeFunction(
                StringTiDBConcatWS::func_name,
                createConstColumn<Nullable<String>>(4, "分隔符"),
                null_value),
            executeFunction(
                StringTiDBConcatWS::func_name,
                createConstColumn<Nullable<String>>(4, "分隔符"),
                null_value,
                null_value));
    };
    for (const auto & item : nulls)
        test_value_is_null(item);

    // test not null
    auto test_not_null = [&](const String & separator, const String & first_value, const String & second_value) {
        String expected_result = first_value + separator + second_value;
        auto inner_test = [&](bool is_separator_const, bool is_first_value_const, bool is_second_value_const) {
            bool is_result_const = is_separator_const && is_first_value_const && is_second_value_const;
            ASSERT_COLUMN_EQ(
                is_result_const ? createConstColumn<Nullable<String>>(1, expected_result) : createColumn<Nullable<String>>({expected_result}),
                executeFunction(
                    StringTiDBConcatWS::func_name,
                    is_separator_const ? createConstColumn<Nullable<String>>(1, separator) : createColumn<Nullable<String>>({separator}),
                    is_first_value_const ? createConstColumn<Nullable<String>>(1, first_value) : createColumn<Nullable<String>>({first_value}),
                    is_second_value_const ? createConstColumn<Nullable<String>>(1, second_value) : createColumn<Nullable<String>>({second_value})));
        };
        std::vector<bool> is_consts = {true, false};
        for (const auto & is_separator_const : is_consts)
            for (const auto & is_first_value_const : is_consts)
                for (const auto & is_second_value_const : is_consts)
                    inner_test(is_separator_const, is_first_value_const, is_second_value_const);
    };
    std::vector<String> not_null_strings = {"", "www.pingcap", "中文.测.试。。。", "分隔符"};
    for (const auto & separator : not_null_strings)
        for (const auto & first_value : not_null_strings)
            for (const auto & second_value : not_null_strings)
                test_not_null(separator, first_value, second_value);
}
CATCH

} // namespace DB::tests
