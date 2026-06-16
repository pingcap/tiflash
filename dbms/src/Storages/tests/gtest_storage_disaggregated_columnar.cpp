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

#include <Common/config.h>
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Interpreters/Context.h>
#include <Operators/SharedQueue.h>
#include <Storages/Columnar/ColumnarSourceOp.h>
#include <Storages/StorageDisaggregatedColumnar.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Schema/TiDB.h>
#include <gtest/gtest.h>

#include <ext/scope_guard.h>

namespace DB::tests
{
namespace
{
tipb::Executor buildTableScanExecutor()
{
    tipb::Executor executor;
    executor.set_tp(tipb::ExecType::TypeTableScan);
    executor.set_executor_id("columnar_ts");

    auto * table_scan = executor.mutable_tbl_scan();
    table_scan->set_table_id(42);
    table_scan->set_next_read_engine(tipb::EngineType::Local);

    auto * column = table_scan->add_columns();
    column->set_column_id(1);
    column->set_tp(TiDB::TypeLongLong);
    column->set_columnlen(20);
    column->set_decimal(0);
    return executor;
}
} // namespace

TEST(StorageDisaggregatedColumnarTest, ReaderWorkStartsNotStarted)
{
    RNColumnarReaderPlan plan{
        .region_id = 1,
        .region_ver = 2,
        .region_conf_ver = 3,
        .physical_table_ranges = {},
    };

    RNColumnarReaderWork work(std::move(plan));

    // This baseline guards the source split: tests include both the new source header
    // and the shared reader-work contract it consumes.
    ASSERT_EQ(work.state, RNColumnarReaderMaterializeState::NotStarted);
    ASSERT_FALSE(work.reader.has_value());
    ASSERT_EQ(work.plan.region_id, 1);
}

TEST(StorageDisaggregatedColumnarTest, NullSourceRecordsTableScanProfiles)
{
    DAGContext dag_context(1024);
    auto context = TiFlashTestEnv::getContext();
    context->setDAGContext(&dag_context);
    SCOPE_EXIT({ context->setDAGContext(nullptr); });
    dag_context.log = Logger::get("columnar_source_test");

    auto executor = buildTableScanExecutor();
    TiDBTableScan table_scan(&executor, "columnar_ts", dag_context);
    PipelineExecutorContext exec_context;
    PipelineExecGroupBuilder group_builder;

    addColumnarNullSourceForTest(exec_context, group_builder, table_scan, dag_context.log);
    ASSERT_FALSE(group_builder.empty());
    const auto header = group_builder.getCurrentHeader();
    ASSERT_EQ(header.columns(), 1);
    ASSERT_EQ(header.getByPosition(0).name, "table_scan_0");

    addColumnarTableScanProfileInfosForTest(*context, group_builder, table_scan);
    ASSERT_EQ(dag_context.getOperatorProfileInfosMap().count("columnar_ts"), 1);
    ASSERT_EQ(dag_context.getOperatorProfileInfosMap()["columnar_ts"].size(), 1);
    ASSERT_EQ(dag_context.getInboundIOProfileInfosMap().count("columnar_ts"), 1);
    ASSERT_EQ(dag_context.getInboundIOProfileInfosMap()["columnar_ts"].size(), 1);
}

TEST(StorageDisaggregatedColumnarTest, QueueSourceReadsSharedQueue)
{
    PipelineExecutorContext exec_context;
    auto [sink_holder, source_holder] = SharedQueue::build(exec_context, 1, 1, -1, 4);
    Block header{createColumn<Int64>({}, "col")};
    Block block{createColumn<Int64>({1, 2}, "col")};

    ASSERT_EQ(sink_holder->tryPush(std::move(block)), MPMCQueueResult::OK);

    RNColumnarSourceOp source({
        .exec_context = exec_context,
        .req_id = "columnar_queue_source_test",
        .header = header,
        .shared_queue = source_holder,
    });

    Block output;
    ASSERT_EQ(source.read(output), OperatorStatus::HAS_OUTPUT);
    ASSERT_TRUE(output);
    ASSERT_EQ(output.rows(), 2);
    ASSERT_EQ(output.getByName("col").column->size(), 2);

    sink_holder->finish();
    Block eof;
    ASSERT_EQ(source.read(eof), OperatorStatus::HAS_OUTPUT);
    ASSERT_FALSE(eof);
}

TEST(StorageDisaggregatedColumnarTest, QueueSourceWaitsAndCancelsThroughSharedQueue)
{
    PipelineExecutorContext exec_context;
    auto [sink_holder, source_holder] = SharedQueue::build(exec_context, 1, 1, -1, 4);
    Block header{createColumn<Int64>({}, "col")};

    RNColumnarSourceOp source({
        .exec_context = exec_context,
        .req_id = "columnar_queue_source_wait_test",
        .header = header,
        .shared_queue = source_holder,
    });

    Block output;
    ASSERT_EQ(source.read(output), OperatorStatus::WAIT_FOR_NOTIFY);
    ASSERT_FALSE(output);

    // Cancellation is delivered through PipelineExecutorContext to registered shared queues.
    exec_context.cancel();
    ASSERT_EQ(source.read(output), OperatorStatus::CANCELLED);

    // Balance the manually created producer holder because no SharedQueueSinkOp owns it in this unit test.
    sink_holder->finish();
}

} // namespace DB::tests
#endif
