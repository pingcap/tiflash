// Copyright 2026 PingCAP, Inc.
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

#include <Common/config.h>
#if ENABLE_NEXT_GEN_COLUMNAR

#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageDisaggregatedColumnar.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TimeZoneTestData.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
constexpr size_t rows_above_hint_threshold = 20;
constexpr size_t rows_at_hint_threshold = 10;

DM::ColumnDefines makeColumnDefines()
{
    DM::ColumnDefines columns;
    columns.emplace_back(1, "s", std::make_shared<DataTypeString>(), Field{});
    columns.emplace_back(2, "ns", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), Field{});
    columns.emplace_back(3, "i", std::make_shared<DataTypeInt64>(), Field{});
    columns.emplace_back(4, "d", std::make_shared<DataTypeDecimal256>(), Field{});
    return columns;
}

MutableColumnPtr makeStringColumn(size_t rows, size_t value_size)
{
    auto column = std::make_shared<DataTypeString>()->createColumn();
    const String value(value_size, 'x');
    for (size_t i = 0; i < rows; ++i)
        column->insert(Field(value));
    return column;
}
} // namespace

class RNColumnarInputStreamTest : public ::testing::Test
{
protected:
    std::unique_ptr<RNColumnarInputStream> createStream()
    {
        const String executor_id = "test_executor";
        RNColumnarInputStream::Options options{
            *context,
            Logger::get("RNColumnarInputStreamTest"),
            nullptr,
            nullptr,
            columns,
            MutSup::invalid_col_id,
            1,
            executor_id};
        return std::make_unique<RNColumnarInputStream>(options);
    }

    ContextPtr context = TiFlashTestEnv::getContext();
    DM::ColumnDefines columns = makeColumnDefines();
};

TEST_F(RNColumnarInputStreamTest, HintsSizedPerColumn)
{
    auto stream = createStream();
    ASSERT_EQ(stream->avgValueSizeHintsSizeForTest(), columns.size());
}

TEST_F(RNColumnarInputStreamTest, StringHintLifecycle)
{
    auto stream = createStream();
    const auto & string_type = columns[0].type;

    // No hint is available before the first block is decoded.
    EXPECT_DOUBLE_EQ(stream->getColumnAvgValueSizeHintForTest(0, string_type), -1.0);

    // Columns that are too small are ignored, so the hint stays unset.
    auto small_column = makeStringColumn(rows_at_hint_threshold, 4);
    stream->updateColumnAvgValueSizeHintForTest(0, string_type, *small_column);
    EXPECT_DOUBLE_EQ(stream->getColumnAvgValueSizeHintForTest(0, string_type), -1.0);

    // A large enough column produces a positive hint that is reused by the next block.
    auto string_column = makeStringColumn(rows_above_hint_threshold, 4);
    stream->updateColumnAvgValueSizeHintForTest(0, string_type, *string_column);
    const double hint = stream->getColumnAvgValueSizeHintForTest(0, string_type);
    EXPECT_DOUBLE_EQ(hint, static_cast<double>(string_column->byteSize()) / rows_above_hint_threshold);
    EXPECT_GT(hint, 0.0);

    // Nullable(String) shares the same hint logic through removeNullable.
    auto nullable_column = columns[1].type->createColumn();
    for (size_t i = 0; i < rows_above_hint_threshold; ++i)
        nullable_column->insert(Field(String(4, 'x')));
    stream->updateColumnAvgValueSizeHintForTest(1, columns[1].type, *nullable_column);
    EXPECT_GT(stream->getColumnAvgValueSizeHintForTest(1, columns[1].type), 0.0);
}

TEST_F(RNColumnarInputStreamTest, NonStringColumnsKeepNegativeSentinel)
{
    auto stream = createStream();

    // Updating with a non-String type must not populate the hint slot.
    auto int_column = columns[2].type->createColumn();
    for (size_t i = 0; i < rows_above_hint_threshold; ++i)
        int_column->insert(Field(Int64(i)));
    stream->updateColumnAvgValueSizeHintForTest(2, columns[2].type, *int_column);
    EXPECT_DOUBLE_EQ(stream->getColumnAvgValueSizeHintForTest(2, columns[2].type), -1.0);

    // The sentinel is decided by the type, not by the stored value, so Decimal256
    // never receives a positive hint even when the slot contains a String hint.
    auto string_column = makeStringColumn(rows_above_hint_threshold, 4);
    stream->updateColumnAvgValueSizeHintForTest(2, columns[0].type, *string_column);
    ASSERT_GT(stream->getColumnAvgValueSizeHintForTest(2, columns[0].type), 0.0);
    EXPECT_DOUBLE_EQ(stream->getColumnAvgValueSizeHintForTest(2, columns[3].type), -1.0);
}
TEST_F(FunctionTest, ColumnarExactFilterRetainsUnsafeTimestamp)
try
{
    context->getTimezoneInfo().resetByTimezoneName(getTestTimeZoneName("America/New_York"));
    tipb::Expr timestamp;
    timestamp.set_tp(tipb::ColumnRef);
    timestamp.mutable_field_type()->set_tp(TiDB::TypeTimestamp);
    WriteBufferFromOwnString buffer;
    encodeDAGInt64(0, buffer);
    timestamp.set_val(buffer.releaseStr());
    tipb::Expr predicate;
    predicate.set_tp(tipb::ScalarFunc);
    predicate.set_sig(tipb::GTTime);
    predicate.mutable_field_type()->set_tp(TiDB::TypeLongLong);
    *predicate.add_children() = timestamp;
    *predicate.add_children() = constructDateTimeLiteralTiExpr(MyDateTime(2026, 11, 1, 1, 30, 0, 0).toPackedUInt());
    auto integer = predicate;
    integer.set_sig(tipb::EQInt);
    integer.mutable_children(0)->mutable_field_type()->set_tp(TiDB::TypeLongLong);
    WriteBufferFromOwnString integer_buffer;
    encodeDAGInt64(1, integer_buffer);
    integer.mutable_children(0)->set_val(integer_buffer.releaseStr());
    *integer.mutable_children(1) = constructInt64LiteralTiExpr(1);

    for (bool partition : {false, true})
    {
        SCOPED_TRACE(partition);
        tipb::Executor executor;
        executor.set_tp(partition ? tipb::TypePartitionTableScan : tipb::TypeTableScan);
        tipb::ColumnInfo ts_column;
        ts_column.set_column_id(10);
        ts_column.set_tp(TiDB::TypeTimestamp);
        auto int_column = ts_column;
        int_column.set_column_id(20);
        int_column.set_tp(TiDB::TypeLongLong);
        if (partition)
        {
            auto * scan = executor.mutable_partition_table_scan();
            scan->set_table_id(42);
            *scan->add_columns() = ts_column;
            *scan->add_columns() = int_column;
            *scan->add_pushed_down_filter_conditions() = integer;
            *scan->add_pushed_down_filter_conditions() = predicate;
        }
        else
        {
            auto * scan = executor.mutable_tbl_scan();
            scan->set_keep_order(false);
            scan->set_table_id(42);
            *scan->add_columns() = ts_column;
            *scan->add_columns() = int_column;
            *scan->add_pushed_down_filter_conditions() = integer;
            *scan->add_pushed_down_filter_conditions() = predicate;
        }
        TiDBTableScan scan(&executor, "scan", *context->getDAGContext());
        google::protobuf::RepeatedPtrField<tipb::Expr> selection;
        *selection.Add() = predicate;
        String normalized_scan;
        String normalized_selection;
        auto task = RNColumnarReadTask::buildForTest(
            *context,
            scan,
            FilterConditions("selection", selection),
            normalized_scan,
            normalized_selection);
        tipb::Executor normalized;
        ASSERT_TRUE(normalized.ParseFromString(normalized_scan));
        const auto & pushed = partition ? normalized.partition_table_scan().pushed_down_filter_conditions()
                                        : normalized.tbl_scan().pushed_down_filter_conditions();
        ASSERT_EQ(pushed.size(), 1);
        EXPECT_EQ(pushed[0].SerializeAsString(), integer.SerializeAsString());
        EXPECT_TRUE(normalized_selection.empty());
        EXPECT_EQ(task->getExactFilterColumnIDs(), (std::unordered_set<ColumnID>{10, 20}));
        String reason;
        EXPECT_FALSE(task->isLateMaterializationFilterEligible(&reason));
        EXPECT_EQ(reason, "unsupported_filter_column_type");
    }
}
CATCH

} // namespace DB::tests

#endif
