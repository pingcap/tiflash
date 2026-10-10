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
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Storages/StorageDisaggregatedColumnar.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TiDB/Schema/TiDB.h>

#include <cstring>

namespace DB::tests
{
namespace
{
tipb::Expr columnRef(Int64 index, TiDB::TP type, int fsp = 6)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ColumnRef);
    WriteBufferFromOwnString buffer;
    encodeDAGInt64(index, buffer);
    expr.set_val(buffer.releaseStr());
    expr.mutable_field_type()->set_tp(type);
    expr.mutable_field_type()->set_decimal(fsp);
    return expr;
}

tipb::Expr scalar(tipb::ScalarFuncSig sig, std::initializer_list<tipb::Expr> children)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ScalarFunc);
    expr.set_sig(sig);
    expr.mutable_field_type()->set_tp(TiDB::TypeLongLong);
    for (const auto & child : children)
        *expr.add_children() = child;
    return expr;
}

tipb::ColumnInfo scanColumn(ColumnID id, TiDB::TP type, int fsp = 6)
{
    tipb::ColumnInfo column;
    column.set_column_id(id);
    column.set_tp(type);
    column.set_decimal(fsp);
    return column;
}

ColumnWithTypeAndName withID(ColumnWithTypeAndName column, ColumnID id, const String & name)
{
    column.column_id = id;
    column.name = name;
    return column;
}
} // namespace

class ColumnarLateMaterializationTest : public FunctionTest
{
protected:
    RNColumnarReadTaskPtr buildTask(
        tipb::Executor & executor,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & selection = {})
    {
        TiDBTableScan scan(&executor, "scan", *context->getDAGContext());
        return RNColumnarReadTask::buildForTest(
            *context,
            scan,
            FilterConditions(selection.empty() ? "" : "selection", selection),
            normalized_scan,
            normalized_selection);
    }

    static tipb::Executor tableScan(std::initializer_list<tipb::ColumnInfo> columns, bool partition = false)
    {
        tipb::Executor executor;
        executor.set_tp(partition ? tipb::TypePartitionTableScan : tipb::TypeTableScan);
        if (partition)
        {
            executor.mutable_partition_table_scan()->set_table_id(42);
            for (const auto & column : columns)
                *executor.mutable_partition_table_scan()->add_columns() = column;
        }
        else
        {
            executor.mutable_tbl_scan()->set_table_id(42);
            executor.mutable_tbl_scan()->set_keep_order(false);
            for (const auto & column : columns)
                *executor.mutable_tbl_scan()->add_columns() = column;
        }
        return executor;
    }

    static void checkFilter(FilterTransformAction & action, const Block & raw, const std::vector<UInt8> & expected)
    {
        // Match the reader: evaluate casts on a copy and retain the UTC/nanosecond storage block.
        Block evaluation = raw;
        FilterPtr filter = nullptr;
        ASSERT_TRUE(action.transform(evaluation, filter, true));
        ASSERT_NE(filter, nullptr);
        EXPECT_EQ(std::vector<UInt8>(filter->begin(), filter->end()), expected);
        EXPECT_EQ(raw.rows(), expected.size());
    }

    String normalized_scan;
    String normalized_selection;
};

TEST_F(ColumnarLateMaterializationTest, TimestampSessionTimezoneAndNormalizedCopies)
try
{
    struct Case
    {
        String name;
        Int64 offset;
        MyDateTime utc_boundary;
    };
    const MyDateTime session_boundary(2026, 3, 2, 0, 0, 0, 1);
    for (const auto & test : {
             Case{"UTC", 0, {2026, 3, 2, 0, 0, 0, 1}},
             Case{"Asia/Singapore", 28800, {2026, 3, 1, 16, 0, 0, 1}},
             Case{"", 28800, {2026, 3, 1, 16, 0, 0, 1}},
             Case{"", -19800, {2026, 3, 2, 5, 30, 0, 1}},
         })
    {
        SCOPED_TRACE(fmt::format("timezone={} offset={}", test.name, test.offset));
        if (test.name.empty())
            context->getTimezoneInfo().resetByTimezoneOffset(test.offset);
        else
            context->getTimezoneInfo().resetByTimezoneName(test.name);
        for (bool partition : {false, true})
        {
            SCOPED_TRACE(partition);
            auto executor
                = tableScan({scanColumn(10, TiDB::TypeTimestamp), scanColumn(20, TiDB::TypeDatetime)}, partition);
            auto lower = scalar(
                tipb::GETime,
                {columnRef(0, TiDB::TypeTimestamp), constructDateTimeLiteralTiExpr(session_boundary.toPackedUInt())});
            auto upper = scalar(
                tipb::LTTime,
                {columnRef(0, TiDB::TypeTimestamp),
                 constructDateTimeLiteralTiExpr(MyDateTime(2026, 3, 2, 0, 0, 0, 3).toPackedUInt())});
            auto same_datetime
                = scalar(tipb::EQTime, {columnRef(0, TiDB::TypeTimestamp), columnRef(1, TiDB::TypeDatetime)});
            auto * pushed = partition ? executor.mutable_partition_table_scan()->mutable_pushed_down_filter_conditions()
                                      : executor.mutable_tbl_scan()->mutable_pushed_down_filter_conditions();
            *pushed->Add() = upper;
            *pushed->Add() = same_datetime;
            google::protobuf::RepeatedPtrField<tipb::Expr> selection;
            *selection.Add() = lower;
            const auto original_executor = executor.SerializeAsString();
            auto task = buildTask(executor, selection);
            ASSERT_TRUE(task->isLateMaterializationFilterEligible());

            tipb::Executor normalized;
            ASSERT_TRUE(normalized.ParseFromString(normalized_scan));
            const auto & normalized_pushed = partition
                ? normalized.partition_table_scan().pushed_down_filter_conditions()
                : normalized.tbl_scan().pushed_down_filter_conditions();
            EXPECT_EQ(
                decodeLiteral(normalized_pushed[0].children(1)).get<UInt64>(),
                test.utc_boundary.toPackedUInt() + 2);
            // Comparisons between two columns must not be normalized as column-literal predicates.
            EXPECT_EQ(normalized_pushed[1].SerializeAsString(), same_datetime.SerializeAsString());
            UInt32 length = 0;
            ASSERT_GE(normalized_selection.size(), sizeof(length));
            std::memcpy(&length, normalized_selection.data(), sizeof(length));
            ASSERT_EQ(normalized_selection.size(), sizeof(length) + length);
            tipb::Expr normalized_lower;
            ASSERT_TRUE(normalized_lower.ParseFromArray(normalized_selection.data() + sizeof(length), length));
            EXPECT_EQ(decodeLiteral(normalized_lower.children(1)).get<UInt64>(), test.utc_boundary.toPackedUInt());
            EXPECT_EQ(executor.SerializeAsString(), original_executor);
            EXPECT_EQ(selection[0].SerializeAsString(), lower.SerializeAsString());

            auto before = test.utc_boundary;
            before.micro_second = 0;
            auto inside = test.utc_boundary;
            inside.micro_second = 2;
            auto outside = test.utc_boundary;
            outside.micro_second = 3;
            // Early columns have a different order from TableScan and contain system columns absent from scan metadata.
            Block raw{
                withID(createColumn<UInt64>({1, 2, 3, 4, 5, 6}), MutSup::version_col_id, "version"),
                withID(
                    createDateTimeColumn(
                        {session_boundary,
                         session_boundary,
                         MyDateTime(2026, 3, 2, 0, 0, 0, 2),
                         session_boundary,
                         {},
                         MyDateTime(2026, 3, 2, 0, 0, 0, 3)},
                        6),
                    20,
                    "datetime"),
                withID(
                    createDateTimeColumn({before, test.utc_boundary, inside, inside, {}, outside}, 6),
                    10,
                    "timestamp"),
                withID(createColumn<Int64>({1, 2, 3, 4, 5, 6}), MutSup::extra_handle_id, "handle"),
            };
            const auto exact = task->getLateMaterializationFilterConditions(raw);
            ASSERT_EQ(exact.size(), 3);
            EXPECT_EQ(decodeDAGInt64(exact[0].children(0).val()), 2);
            EXPECT_EQ(decodeLiteral(exact[0].children(1)).get<UInt64>(), session_boundary.toPackedUInt());
            EXPECT_EQ(
                decodeLiteral(exact[1].children(1)).get<UInt64>(),
                MyDateTime(2026, 3, 2, 0, 0, 0, 3).toPackedUInt());
            EXPECT_EQ(decodeDAGInt64(exact[2].children(1).val()), 1);
            auto action = task->buildLateMaterializationFilterAction(raw.cloneEmpty());
            auto original_timestamp = raw.getByName("timestamp");
            original_timestamp.column = original_timestamp.column->cloneResized(raw.rows());
            for (int batch = 0; batch < 2; ++batch)
            {
                checkFilter(*action, raw, {0, 1, 1, 0, 0, 0});
                ASSERT_COLUMN_EQ(original_timestamp, raw.getByName("timestamp"));
                EXPECT_EQ(raw.columns(), 4);
            }
            Block next_batch = raw.cloneEmpty();
            for (size_t i = 0; i < raw.columns(); ++i)
                next_batch.getByPosition(i).column = raw.getByPosition(i).column->cut(1, 2);
            checkFilter(*action, next_batch, {1, 1});
        }
    }
}
CATCH

TEST_F(ColumnarLateMaterializationTest, DurationCastWithNullableNegativeAndFractionalValues)
try
{
    context->getTimezoneInfo().resetByTimezoneOffset(28800);
    for (int fsp : {0, 4, 6})
    {
        SCOPED_TRACE(fsp);
        auto executor = tableScan({scanColumn(10, TiDB::TypeTime, fsp)});
        auto hour = scalar(tipb::Hour, {columnRef(0, TiDB::TypeTime, fsp)});
        *executor.mutable_tbl_scan()->add_pushed_down_filter_conditions()
            = scalar(tipb::EQInt, {hour, constructInt64LiteralTiExpr(25)});
        const Int64 fraction = fsp == 0 ? 0 : (fsp == 4 ? 123400 : 123456);
        google::protobuf::RepeatedPtrField<tipb::Expr> selection;
        *selection.Add() = scalar(
            tipb::EQInt,
            {scalar(tipb::MicroSecond, {columnRef(0, TiDB::TypeTime, fsp)}), constructInt64LiteralTiExpr(fraction)});
        auto task = buildTask(executor, selection);
        ASSERT_TRUE(task->isLateMaterializationFilterEligible());
        const Int64 value = 25LL * 3600 * 1000000000 + fraction * 1000;
        Block raw{
            withID(createColumn<UInt64>({1, 2, 3, 4, 5}), MutSup::version_col_id, "version"),
            withID(
                createColumn<Nullable<Int64>>({value, -value, value + 3600LL * 1000000000, {}, value - 1000000000}),
                10,
                "duration"),
        };
        auto action = task->buildLateMaterializationFilterAction(raw.cloneEmpty());
        auto expected_type = makeNullable(std::make_shared<DataTypeMyDuration>(fsp));
        ASSERT_DATATYPE_EQ(expected_type, action->getHeader().getByName("duration").type);
        auto original_duration = raw.getByName("duration");
        original_duration.column = original_duration.column->cloneResized(raw.rows());
        for (int batch = 0; batch < 2; ++batch)
        {
            // HOUR ignores the sign, and durations can exceed 24 hours.
            checkFilter(*action, raw, {1, 1, 0, 0, 0});
            ASSERT_COLUMN_EQ(original_duration, raw.getByName("duration"));
            EXPECT_EQ(raw.getByName("duration").type->getName(), "Nullable(Int64)");
        }
    }
}
CATCH

TEST_F(ColumnarLateMaterializationTest, TimestampAndDurationInOneAction)
try
{
    context->getTimezoneInfo().resetByTimezoneOffset(-19800);
    auto executor = tableScan({scanColumn(10, TiDB::TypeTimestamp), scanColumn(20, TiDB::TypeTime)});
    *executor.mutable_tbl_scan()->add_pushed_down_filter_conditions() = scalar(
        tipb::LogicalAnd,
        {scalar(
             tipb::GETime,
             {columnRef(0, TiDB::TypeTimestamp),
              constructDateTimeLiteralTiExpr(MyDateTime(2026, 3, 1, 10, 30, 0, 0).toPackedUInt())}),
         scalar(tipb::EQInt, {scalar(tipb::Hour, {columnRef(1, TiDB::TypeTime)}), constructInt64LiteralTiExpr(25)})});
    auto task = buildTask(executor);
    ASSERT_TRUE(task->isLateMaterializationFilterEligible());
    const Int64 duration = 25LL * 3600 * 1000000000;
    Block raw{
        withID(createColumn<Nullable<Int64>>({duration, -duration, 0, duration}), 20, "duration"),
        withID(
            createDateTimeColumn(
                {MyDateTime(2026, 3, 1, 15, 59, 59, 999999),
                 MyDateTime(2026, 3, 1, 16, 0, 0, 0),
                 MyDateTime(2026, 3, 1, 16, 0, 0, 1),
                 {}},
                6),
            10,
            "timestamp"),
    };
    auto action = task->buildLateMaterializationFilterAction(raw.cloneEmpty());
    checkFilter(*action, raw, {0, 1, 0, 0});
    ASSERT_DATATYPE_EQ(
        makeNullable(std::make_shared<DataTypeMyDuration>(6)),
        action->getHeader().getByName("duration").type);
}
CATCH
} // namespace DB::tests
#endif
