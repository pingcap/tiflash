#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypeNothing.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/core.h>

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
        auto expected_str = fmt::format("\n{}: {}", expected_expr, expected_display);
        auto actual_str = fmt::format("\n{}: {}", actual_expr, actual_display);
        return ::testing::AssertionFailure() << title << expected_str << actual_str;
    }
    return ::testing::AssertionSuccess();
}


#define ASSERT_EQUAL_WITH_TEXT(expected_value, actual_value, title, expected_display, actual_display)                                             \
    do                                                                                                                                            \
    {                                                                                                                                             \
        auto result = assertEqual(#expected_value, #actual_value, (expected_value), (actual_value), (expected_display), (actual_display), title); \
        if (!result)                                                                                                                              \
            return result;                                                                                                                        \
    } while (false)

#define ASSERT_EQUAL(expected_value, actual_value, title)                                                             \
    do                                                                                                                \
    {                                                                                                                 \
        auto expected_v = (expected_value);                                                                           \
        auto actual_v = (actual_value);                                                                               \
        auto result = assertEqual(#expected_value, #actual_value, expected_v, actual_v, expected_v, actual_v, title); \
        if (!result)                                                                                                  \
            return result;                                                                                            \
    } while (false)

::testing::AssertionResult dataTypeEqual(
    const DataTypePtr & expected,
    const DataTypePtr & actual)
{
    ASSERT_EQUAL(expected->getName(), actual->getName(), "DataType name mismatch");
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult columnEqual(
    const ColumnPtr & expected,
    const ColumnPtr & actual)
{
    ASSERT_EQUAL(expected->getName(), actual->getName(), "Column name mismatch");
    ASSERT_EQUAL(expected->size(), actual->size(), "Column size mismatch");

    for (size_t i = 0, size = expected->size(); i < size; ++i)
    {
        auto expected_field = (*expected)[i];
        auto actual_field = (*actual)[i];

        ASSERT_EQUAL_WITH_TEXT(expected_field, actual_field, fmt::format("Value {} mismatch", i), expected_field.toString(), actual_field.toString());
    }
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult columnEqual(
    const ColumnWithTypeAndName & expected,
    const ColumnWithTypeAndName & actual)
{
    auto ret = dataTypeEqual(expected.type, actual.type);
    if (!ret)
        return ret;

    return columnEqual(expected.column, actual.column);
}

ColumnWithTypeAndName FunctionTest::executeFunction(const String & func_name, const ColumnsWithTypeAndName & columns, const TiDB::TiDBCollatorPtr & collator)
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
    block.insert({nullptr, func->getReturnType(), "res"});
    func->execute(block, cns, columns.size());
    return block.getByPosition(columns.size());
}

ColumnWithTypeAndName FunctionTest::executeFunction(const String & func_name, const ColumnNumbers & argument_column_numbers, const ColumnsWithTypeAndName & columns)
{
    auto & factory = FunctionFactory::instance();
    Block block(columns);
    ColumnsWithTypeAndName arguments;
    for (size_t i = 0; i < argument_column_numbers.size(); ++i)
        arguments.push_back(columns.at(i));
    auto bp = factory.tryGet(func_name, context);
    if (!bp)
        throw TiFlashTestException(fmt::format("Function {} not found!", func_name));
    auto func = bp->build(arguments);
    block.insert({nullptr, func->getReturnType(), "res"});
    func->execute(block, argument_column_numbers, columns.size());
    return block.getByPosition(columns.size());
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

ColumnWithTypeAndName createDateTimeColumnNullable(std::initializer_list<std::optional<MyDateTime>> init, int fraction)
{
    auto data_type_ptr = makeNullable(std::make_shared<DataTypeMyDateTime>(fraction));
    auto col = data_type_ptr->createColumn();
    for (const auto dt : init)
    {
        if (dt.has_value())
            col->insert(Field(dt->toPackedUInt()));
        else
            col->insert(Null());
    }
    return {std::move(col), data_type_ptr, "datetime"};
}

ColumnWithTypeAndName createDateTimeColumnConst(size_t size, const MyDateTime & dt, int fraction)
{
    auto data_type_ptr = std::make_shared<DataTypeMyDateTime>(fraction);
    auto col = data_type_ptr->createColumnConst(size, Field(dt.toPackedUInt()));
    return {std::move(col), data_type_ptr, "datetime"};
}

} // namespace tests
} // namespace DB
