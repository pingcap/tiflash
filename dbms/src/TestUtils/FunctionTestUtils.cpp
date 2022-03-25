// Copyright 2022 PingCAP, Ltd.
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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/ColumnNumbers.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeNothing.h>
#include <Encryption/MockKeyManager.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Aggregator.h>
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

ColumnWithTypeAndName executeFunction(Context & context, const String & func_name, const ColumnsWithTypeAndName & columns, const TiDB::TiDBCollatorPtr & collator)
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

ColumnWithTypeAndName executeFunction(Context & context, const String & func_name, const ColumnNumbers & argument_column_numbers, const ColumnsWithTypeAndName & columns)
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

ColumnWithTypeAndName executeAggregateFunction(const String & func_name,
                                               const ColumnsWithTypeAndName & cols,
                                               const ColumnNumbers & group_keys_offset,
                                               const bool empty_result_for_aggregation_by_empty_set,
                                               const bool allow_to_use_two_level_group_by,
                                               const TiDB::TiDBCollators & collators)
{
    auto & factory = AggregateFunctionFactory::instance();
    AggregateDescriptions aggregate_descriptions(1);
    DataTypes empty_list_of_types;
    aggregate_descriptions[0].function = factory.get(func_name, empty_list_of_types);

    Block block;
    for (ColumnWithTypeAndName col : cols)
    {
        block.insert(col);
    }
    BlockInputStreamPtr stream = std::make_shared<OneBlockInputStream>(block);
    Aggregator::Params params(
        stream->getHeader(),
        group_keys_offset,
        aggregate_descriptions,
        false,
        SettingUInt64(0), // max_rows_to_group_by
        SettingOverflowMode<true>(OverflowMode::THROW), // group_by_overflow_mode
        allow_to_use_two_level_group_by ? SettingUInt64(100000) : SettingUInt64(0), // group_by_two_level_threshold
        allow_to_use_two_level_group_by ? SettingUInt64(100000000) : SettingUInt64(0), // group_by_two_level_threshold_bytes
        SettingUInt64(0), // max_bytes_before_external_group_by
        empty_result_for_aggregation_by_empty_set,
        "", // temporary path
        collators);
    Aggregator aggregator(params);
    KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(false);
    FileProviderPtr file_provider = std::make_shared<FileProvider>(key_manager, false);
    AggregatedDataVariantsPtr data_variants = std::make_shared<AggregatedDataVariants>();
    aggregator.execute(stream, *data_variants, file_provider);
    if (aggregator.hasTemporaryFiles())
    {
        throw TiFlashTestException("aggregator.hasTemporaryFiles() should be false");
    }
    ManyAggregatedDataVariants many_data{data_variants};
    std::unique_ptr<IBlockInputStream> result = aggregator.mergeAndConvertToBlocks(many_data, true, 1);
    return result->read().getByPosition(cols.size() - 1);
}

DataTypePtr getReturnTypeForFunction(
    Context & context,
    const String & func_name,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator)
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

} // namespace tests
} // namespace DB
