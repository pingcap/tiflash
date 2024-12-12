// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Columns/ColumnNothing.h>
#include <Columns/ColumnSet.h>
#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Core/ColumnNumbers.h>
#include <Core/Row.h>
#include <DataTypes/DataTypeNothing.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzerHelper.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsJson.h>
#include <Interpreters/Context.h>
#include <TestUtils/ColumnsToTiPBExpr.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <algorithm>
#include <ext/enumerate.h>
#include <random>
#include <set>


namespace DB
{
namespace tests
{
template <typename ExpectedT, typename ActualT, typename ExpectedDisplayT, typename ActualDisplayT>
::testing::AssertionResult assertEqual(
    const char * expected_expr,
    const char * actual_expr,
    const ExpectedT & expected_v,
    const ActualT & actual_v,
    const ExpectedDisplayT & expected_display,
    const ActualDisplayT & actual_display,
    const String & title = "")
{
    if (expected_v != actual_v)
    {
        auto expected_str = fmt::format("\n  {}:\n    {}", expected_expr, expected_display);
        auto actual_str = fmt::format("\n  {}:\n    {}", actual_expr, actual_display);
        return ::testing::AssertionFailure() << title << expected_str << actual_str;
    }
    return ::testing::AssertionSuccess();
}


#define ASSERT_EQUAL_WITH_TEXT(expected_value, actual_value, title, expected_display, actual_display) \
    do                                                                                                \
    {                                                                                                 \
        if (auto result = assertEqual(                                                                \
                #expected_value,                                                                      \
                #actual_value,                                                                        \
                (expected_value),                                                                     \
                (actual_value),                                                                       \
                (expected_display),                                                                   \
                (actual_display),                                                                     \
                title);                                                                               \
            !result)                                                                                  \
            return result;                                                                            \
    } while (false)

#define ASSERT_EQUAL(expected_value, actual_value, title)                                                     \
    do                                                                                                        \
    {                                                                                                         \
        auto expected_v = (expected_value);                                                                   \
        auto actual_v = (actual_value);                                                                       \
        if (auto result                                                                                       \
            = assertEqual(#expected_value, #actual_value, expected_v, actual_v, expected_v, actual_v, title); \
            !result)                                                                                          \
            return result;                                                                                    \
    } while (false)

::testing::AssertionResult dataTypeEqual(const DataTypePtr & expected, const DataTypePtr & actual)
{
    ASSERT_EQUAL(expected->getName(), actual->getName(), "DataType name mismatch");
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult columnEqual(
    const ColumnPtr & expected,
    const ColumnPtr & actual,
    const TiDB::ITiDBCollator * collator,
    bool is_floating_point,
    bool exact_match_for_floating_point)
{
    ASSERT_EQUAL(expected->getName(), actual->getName(), "Column name mismatch");
    ASSERT_EQUAL(expected->size(), actual->size(), "Column size mismatch");

    if unlikely (typeid_cast<const ColumnNothing *>(expected.get()) || typeid_cast<const ColumnNothing *>(actual.get()))
    {
        /// ColumnNothing compares size only
        const auto * expected_nothing = typeid_cast<const ColumnNothing *>(expected.get());
        const auto * actual_nothing = typeid_cast<const ColumnNothing *>(actual.get());
        ASSERT_EQUAL(
            expected_nothing && actual_nothing,
            true,
            "One of columns is ColumnNothing, while the other is not");
        ASSERT_EQUAL(
            expected_nothing->size(),
            actual_nothing->size(),
            fmt::format(
                "Column size not match, expected {} actual {}",
                actual_nothing->size(),
                expected_nothing->size()));
        return ::testing::AssertionSuccess();
    }

    for (size_t i = 0, size = expected->size(); i < size; ++i)
    {
        auto expected_field = (*expected)[i];
        auto actual_field = (*actual)[i];

        if (!is_floating_point || exact_match_for_floating_point)
        {
            if (collator != nullptr && !expected_field.isNull() && !actual_field.isNull())
            {
                auto e_string = expected_field.get<String>();
                auto a_string = actual_field.get<String>();
                if (collator->compare(e_string.data(), e_string.size(), a_string.data(), a_string.size()) == 0)
                    continue;
                /// if not equal, fallback to the original compare so we can reuse the code to get error message
            }
            ASSERT_EQUAL_WITH_TEXT(
                expected_field,
                actual_field,
                fmt::format("Value at index {} mismatch", i),
                expected_field.toString(),
                actual_field.toString());
        }
        else
        {
            auto expected_field_expr = expected_field.toString();
            auto actual_field_expr = actual_field.toString();
            if (auto res = ::testing::internal::CmpHelperFloatingPointEQ(
                    expected_field_expr.c_str(),
                    actual_field_expr.c_str(),
                    expected_field.safeGet<Float64>(),
                    actual_field.safeGet<Float64>());
                !res)
                return ::testing::AssertionFailure() << fmt::format("Value at index {} mismatch, ", i) << res.message();
        }
    }
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult columnEqual(
    const ColumnWithTypeAndName & expected,
    const ColumnWithTypeAndName & actual,
    const TiDB::ITiDBCollator * collator,
    bool exact_match_for_floating_point)
{
    if (auto ret = dataTypeEqual(expected.type, actual.type); !ret)
        return ret;

    return columnEqual(
        expected.column,
        actual.column,
        collator,
        expected.type->isFloatingPoint(),
        exact_match_for_floating_point);
}

::testing::AssertionResult blockEqual(const Block & expected, const Block & actual)
{
    size_t columns = actual.columns();
    size_t expected_columns = expected.columns();

    ASSERT_EQUAL(
        expected_columns,
        columns,
        fmt::format(
            "Block column size mismatch\nexpected_structure: {}\nstructure: {}",
            expected.dumpJsonStructure(),
            actual.dumpJsonStructure()));

    for (size_t i = 0; i < columns; ++i)
    {
        const auto & expected_col = expected.getByPosition(i);
        const auto & actual_col = actual.getByPosition(i);

        auto cmp_res = columnEqual(expected_col, actual_col);
        if (!cmp_res)
            return cmp_res;
    }
    return ::testing::AssertionSuccess();
}

/// size of each column should be the same
std::multiset<Row> columnsToRowSet(const ColumnsWithTypeAndName & cols)
{
    if (cols.empty())
        return {};
    if (cols[0].column->empty())
        return {};

    size_t cols_size = cols.size();
    std::vector<Row> rows{cols[0].column->size()};

    for (auto & r : rows)
    {
        r.resize(cols_size, true);
    }

    for (auto && [col_id, col] : ext::enumerate(cols))
    {
        for (size_t i = 0, size = col.column->size(); i < size; ++i)
        {
            new (rows[i].place(col_id)) Field((*col.column)[i]);
        }
    }
    return {std::make_move_iterator(rows.begin()), std::make_move_iterator(rows.end())};
}

::testing::AssertionResult columnsEqual(
    const ColumnsWithTypeAndName & expected,
    const ColumnsWithTypeAndName & actual,
    bool _restrict)
{
    if (_restrict)
        return blockEqual(Block(expected), Block(actual));

    auto check_empty = [](const ColumnsWithTypeAndName & column) -> bool {
        auto cols_size = column.size();
        for (size_t i = 0; i < cols_size; ++i)
        {
            if (!column[i].column->empty())
                return false;
        }
        return true;
    };

    if ((expected.empty() || actual.empty()) && check_empty(expected) && check_empty(actual))
        return testing::AssertionSuccess();

    auto expect_cols_size = expected.size();
    auto actual_cols_size = actual.size();

    ASSERT_EQUAL(expect_cols_size, actual_cols_size, "Columns size mismatch");

    for (size_t i = 0; i < expect_cols_size; ++i)
    {
        auto const & expect_col = expected[i];
        auto const & actual_col = actual[i];
        ASSERT_EQUAL(
            expect_col.column->getName(),
            actual_col.column->getName(),
            fmt::format("Column {} name mismatch", i));
        ASSERT_EQUAL(expect_col.column->size(), actual_col.column->size(), fmt::format("Column {} size mismatch", i));
        auto type_eq = dataTypeEqual(expected[i].type, actual[i].type);
        if (!type_eq)
            return type_eq;
    }

    auto const expected_row_set = columnsToRowSet(expected);
    auto const actual_row_set = columnsToRowSet(actual);

    if (expected_row_set != actual_row_set)
    {
        FmtBuffer buf;

        auto expect_it = expected_row_set.begin();
        auto actual_it = actual_row_set.begin();

        buf.append("Columns row set mismatch\n").append("expected_row_set:\n");
        for (; expect_it != expected_row_set.end(); ++expect_it, ++actual_it)
        {
            buf.joinStr(
                   expect_it->begin(),
                   expect_it->end(),
                   [](const auto & v, FmtBuffer & fb) { fb.append(v.toString()); },
                   " ")
                .append("\n");
            if (*expect_it != *actual_it)
                break;
        }

        ++actual_it;

        buf.append("...\nactual_row_set:\n");
        for (auto it = actual_row_set.begin(); it != actual_it; ++it)
        {
            buf.joinStr(
                   it->begin(),
                   it->end(),
                   [](const auto & v, FmtBuffer & fb) { fb.append(v.toString()); },
                   " ")
                .append("\n");
        }
        buf.append("...\n");

        return testing::AssertionFailure() << buf.toString();
    }

    return testing::AssertionSuccess();
}

std::pair<ExpressionActionsPtr, String> buildFunction(
    Context & context,
    const String & func_name,
    const ColumnNumbers & argument_column_numbers,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator,
    const String & val)
{
    tipb::Expr tipb_expr = columnsToTiPBExpr(func_name, argument_column_numbers, columns, collator, val);

    NamesAndTypes source_columns;
    for (size_t index : argument_column_numbers)
        source_columns.emplace_back(columns[index].name, columns[index].type);
    DAGExpressionAnalyzer analyzer(source_columns, context);
    ExpressionActionsChain chain;
    auto & last_step = analyzer.initAndGetLastStep(chain);
    auto result_name = DB::DAGExpressionAnalyzerHelper::buildFunction(&analyzer, tipb_expr, last_step.actions);
    last_step.required_output.push_back(result_name);
    chain.finalize();
    return std::make_pair(last_step.actions, result_name);
}

ColumnsWithTypeAndName toColumnsWithUniqueName(const ColumnsWithTypeAndName & columns)
{
    ColumnsWithTypeAndName columns_with_distinct_name = columns;
    std::string base_name = "col";
    for (size_t i = 0; i < columns.size(); ++i)
    {
        columns_with_distinct_name[i].name = fmt::format("{}_{}", base_name, i);
    }
    return columns_with_distinct_name;
}

ColumnsWithTypeAndName toColumnsReordered(const ColumnsWithTypeAndName & columns, const ColumnNumbers & new_offsets)
{
    ColumnsWithTypeAndName columns_reordered(columns.size());
    for (auto && [i, offset] : ext::enumerate(new_offsets))
    {
        columns_reordered[offset] = columns[i];
    }
    return columns_reordered;
}

ColumnWithTypeAndName executeFunction(
    Context & context,
    const String & func_name,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator,
    const String & val,
    bool raw_function_test)
{
    ColumnNumbers argument_column_numbers;
    for (size_t i = 0; i < columns.size(); ++i)
        argument_column_numbers.push_back(i);

    /// Replace `std::random_device` with `std::chrono::system_clock` here to avoid
    /// exceptions like 'random_device failed to open /dev/urandom: Operation not permitted'.
    /// The reason of exceptions is unknown, but the probability of its occurrence in unittests
    /// TestDateTimeDayMonthYear.dayMonthYearTest is not low.
    /// Since this function is just used for testing, using current timestamp as a random seed is not a problem.
    std::mt19937 g(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
    /// shuffle input columns to assure function correctly use physical offsets instead of logical offsets
    std::shuffle(argument_column_numbers.begin(), argument_column_numbers.end(), g);
    const auto columns_reordered = toColumnsReordered(columns, argument_column_numbers);

    return executeFunction(
        context,
        func_name,
        argument_column_numbers,
        columns_reordered,
        collator,
        val,
        raw_function_test);
}

ColumnWithTypeAndName executeFunction(
    Context & context,
    const String & func_name,
    const ColumnNumbers & argument_column_numbers,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator,
    const String & val,
    bool raw_function_test)
{
    if (raw_function_test)
    {
        auto & factory = FunctionFactory::instance();

        ColumnsWithTypeAndName arguments;
        for (const auto argument_column_number : argument_column_numbers)
            arguments.push_back(columns.at(argument_column_number));

        auto builder = factory.tryGet(func_name, context);
        if (!builder)
            throw TiFlashTestException(fmt::format("Function {} not found!", func_name));

        Block block(columns);
        auto func = builder->build(arguments, collator);

        block.insert({nullptr, func->getReturnType(), "res"});
        func->execute(block, argument_column_numbers, columns.size());

        return block.getByPosition(columns.size());
    }

    auto columns_with_unique_name = toColumnsWithUniqueName(columns);
    auto [actions, result_name]
        = buildFunction(context, func_name, argument_column_numbers, columns_with_unique_name, collator, val);

    Block block(columns_with_unique_name);
    actions->execute(block);

    return block.getByName(result_name);
}

DataTypePtr getReturnTypeForFunction(
    Context & context,
    const String & func_name,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator,
    bool raw_function_test)
{
    if (raw_function_test)
    {
        auto & factory = FunctionFactory::instance();

        Block block(columns);
        ColumnNumbers cns;
        for (size_t i = 0; i < columns.size(); ++i)
            cns.push_back(i);

        auto bp = factory.tryGet(func_name, context);
        if (!bp)
            throw TiFlashTestException(fmt::format("Function {} not found!", func_name));
        auto func = bp->build(columns, collator);
        return func->getReturnType();
    }
    else
    {
        ColumnNumbers argument_column_numbers;
        for (size_t i = 0; i < columns.size(); ++i)
            argument_column_numbers.push_back(i);
        auto columns_with_unique_name = toColumnsWithUniqueName(columns);
        auto [actions, result_name]
            = buildFunction(context, func_name, argument_column_numbers, columns_with_unique_name, collator, "");
        return actions->getSampleBlock().getByName(result_name).type;
    }
}

ColumnWithTypeAndName createOnlyNullColumnConst(size_t size, const String & name)
{
    DataTypePtr data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
    return {data_type->createColumnConst(size, Null()), data_type, name};
}

ColumnWithTypeAndName createOnlyNullColumn(size_t size, const String & name)
{
    DataTypePtr data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
    auto col = data_type->createColumn();
    for (size_t i = 0; i < size; i++)
        col->insert(Null());
    return {std::move(col), data_type, name};
}

ColumnWithTypeAndName toDatetimeVec(String name, const std::vector<String> & v, int fsp)
{
    std::vector<typename TypeTraits<MyDateTime>::FieldType> vec;
    vec.reserve(v.size());
    for (const auto & value_str : v)
    {
        Field value = parseMyDateTime(value_str, fsp);
        vec.push_back(value.template safeGet<UInt64>());
    }
    DataTypePtr data_type = std::make_shared<DataTypeMyDateTime>(fsp);
    return {makeColumn<MyDateTime>(data_type, vec), data_type, name, 0};
}

ColumnWithTypeAndName toNullableDatetimeVec(String name, const std::vector<String> & v, int fsp)
{
    std::vector<std::optional<typename TypeTraits<MyDateTime>::FieldType>> vec;
    vec.reserve(v.size());
    for (const auto & value_str : v)
    {
        if (!value_str.empty())
        {
            Field value = parseMyDateTime(value_str, fsp);
            vec.push_back(value.template safeGet<UInt64>());
        }
        else
        {
            vec.push_back({});
        }
    }
    DataTypePtr data_type = makeNullable(std::make_shared<DataTypeMyDateTime>(fsp));
    return {makeColumn<Nullable<MyDateTime>>(data_type, vec), data_type, name, 0};
}

String getColumnsContent(const ColumnsWithTypeAndName & cols)
{
    if (cols.empty())
        return "";
    return getColumnsContent(cols, 0, cols[0].column->size());
}

String getColumnsContent(const ColumnsWithTypeAndName & cols, size_t begin, size_t end)
{
    const size_t col_num = cols.size();
    if (col_num <= 0)
        return "";

    const size_t col_size = cols[0].column->size();
    if (col_size <= 0)
        return "";
    assert(begin <= end);
    assert(col_size >= end);
    assert(col_size > begin);

    /// Ensure the sizes of columns in cols have the same number of rows
    for (size_t i = 1; i < col_num; ++i)
    {
        RUNTIME_CHECK_MSG(
            cols[i].column->size() == col_size,
            "col_size={} actual_col_size={} col_name={} col_id={}",
            col_size,
            cols[i].column->size(),
            cols[i].name,
            cols[i].column_id);
    }

    std::vector<std::pair<size_t, String>> col_content;
    FmtBuffer fmt_buf;
    for (size_t i = 0; i < col_num; ++i)
    {
        /// Push the column name
        fmt_buf.append(fmt::format("{}: (", cols[i].name));
        for (size_t j = begin; j < end; ++j)
            col_content.push_back(std::make_pair(j, (*cols[i].column)[j].toString()));

        /// Add content
        fmt_buf.joinStr(
            col_content.begin(),
            col_content.end(),
            [](const auto & content, FmtBuffer & fmt_buf) {
                fmt_buf.append(fmt::format("{}: {}", content.first, content.second));
            },
            ", ");

        fmt_buf.append(")\n");
        col_content.clear();
    }

    return fmt_buf.toString();
}

ColumnsWithTypeAndName createColumns(const ColumnsWithTypeAndName & cols)
{
    return cols;
}

FunctionTest::FunctionTest()
    : context(TiFlashTestEnv::getContext())
{}

void FunctionTest::initializeDAGContext()
{
    dag_context_ptr = std::make_unique<DAGContext>(1024);
    context->setDAGContext(dag_context_ptr.get());
}

ColumnWithTypeAndName FunctionTest::executeFunction(
    const String & func_name,
    const ColumnsWithTypeAndName & columns,
    TiDB::TiDBCollatorPtr const & collator,
    bool raw_function_test)
{
    return DB::tests::executeFunction(*context, func_name, columns, collator, "", raw_function_test);
}

ColumnWithTypeAndName FunctionTest::executeFunction(
    const String & func_name,
    const ColumnNumbers & argument_column_numbers,
    const ColumnsWithTypeAndName & columns,
    TiDB::TiDBCollatorPtr const & collator,
    bool raw_function_test)
{
    return DB::tests::executeFunction(
        *context,
        func_name,
        argument_column_numbers,
        columns,
        collator,
        "",
        raw_function_test);
}

ColumnWithTypeAndName FunctionTest::executeFunctionWithMetaData(
    const String & func_name,
    const ColumnsWithTypeAndName & columns,
    const FuncMetaData & meta,
    const TiDB::TiDBCollatorPtr & collator)
{
    return DB::tests::executeFunction(*context, func_name, columns, collator, meta.val, false);
}

ColumnWithTypeAndName FunctionTest::executeFunctionWithMetaData(
    const String & func_name,
    const ColumnNumbers & argument_column_numbers,
    const ColumnsWithTypeAndName & columns,
    const FuncMetaData & meta,
    const TiDB::TiDBCollatorPtr & collator)
{
    return DB::tests::executeFunction(*context, func_name, argument_column_numbers, columns, collator, meta.val, false);
}

ColumnWithTypeAndName FunctionTest::executeCastJsonAsStringFunction(
    const ColumnWithTypeAndName & input_column,
    const tipb::FieldType & field_type)
{
    auto & factory = FunctionFactory::instance();
    ColumnsWithTypeAndName columns({input_column});
    ColumnNumbers argument_column_numbers;
    for (size_t i = 0; i < columns.size(); ++i)
        argument_column_numbers.push_back(i);

    ColumnsWithTypeAndName arguments;
    for (const auto argument_column_number : argument_column_numbers)
        arguments.push_back(columns.at(argument_column_number));

    const String func_name = "cast_json_as_string";
    auto builder = factory.tryGet(func_name, *context);
    if (!builder)
        throw TiFlashTestException(fmt::format("Function {} not found!", func_name));
    auto func = builder->build(arguments, nullptr);
    auto * function_build_ptr = builder.get();
    if (auto * default_function_builder = dynamic_cast<DefaultFunctionBuilder *>(function_build_ptr);
        default_function_builder)
    {
        auto * function_impl = default_function_builder->getFunctionImpl().get();
        if (auto * function_cast_json_as_string = dynamic_cast<FunctionCastJsonAsString *>(function_impl);
            function_cast_json_as_string)
        {
            function_cast_json_as_string->setOutputTiDBFieldType(field_type);
        }
        else
        {
            throw TiFlashTestException(fmt::format("Function {} not found!", func_name));
        }
    }

    Block block(columns);
    block.insert({nullptr, func->getReturnType(), "res"});
    func->execute(block, argument_column_numbers, columns.size());

    return block.getByPosition(columns.size());
}

} // namespace tests
} // namespace DB
