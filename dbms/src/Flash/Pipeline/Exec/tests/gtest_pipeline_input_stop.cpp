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

#include <Common/LooseBoundedMPMCQueue.h>
#include <Common/MemoryTrackerSetter.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Pipeline/Schedule/Tasks/Impls/SimplePipelineTask.h>
#include <Flash/Pipeline/Schedule/Tasks/OneTimeNotifyFuture.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>
#include <Flash/Planner/Plans/PhysicalJoinProbe.h>
#include <Interpreters/Context.h>
#include <Operators/ExchangeReceiverSourceOp.h>
#include <Operators/ExpressionTransformOp.h>
#include <Operators/SharedQueue.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

#include <atomic>
#include <chrono>
#include <ext/scope_guard.h>
#include <thread>

namespace DB::tests
{
namespace
{
class CountRowsSink : public SinkOp
{
public:
    CountRowsSink(PipelineExecutorContext & context, std::atomic_size_t & rows_)
        : SinkOp(context, "")
        , rows(rows_)
    {}
    String getName() const override { return "CountRowsSink"; }
    OperatorStatus writeImpl(Block && block) override
    {
        if (!block)
            return OperatorStatus::FINISHED;
        rows.fetch_add(block.rows());
        return OperatorStatus::NEED_INPUT;
    }

private:
    std::atomic_size_t & rows;
};

class FiniteSource : public SourceOp
{
public:
    FiniteSource(PipelineExecutorContext & context, const Block & header, size_t blocks_)
        : SourceOp(context, "")
        , blocks(blocks_)
    {
        setHeader(header);
    }
    String getName() const override { return "FiniteSource"; }
    OperatorStatus readImpl(Block & block) override
    {
        if (blocks > 0)
        {
            --blocks;
            block = getHeader().cloneEmpty();
            for (auto & column : block)
            {
                auto data = column.type->createColumn();
                data->insert(Field(Int64(1)));
                column.column = std::move(data);
            }
        }
        return OperatorStatus::HAS_OUTPUT;
    }

private:
    size_t blocks;
};

class WakeTask : public Task
{
public:
    WakeTask(PipelineExecutorContext & context, const TaskStopSourcePtr & stop_, std::atomic_size_t & wakes_)
        : Task(context, "", ExecTaskStatus::WAIT_FOR_NOTIFY)
        , stop(stop_)
        , wakes(wakes_)
    {}
    TaskStopSources getNotifyStopSources() const override { return {stop}; }

protected:
    ExecTaskStatus executeImpl() override { return ExecTaskStatus::FINISHED; }
    ExecTaskStatus notifyImpl() override
    {
        wakes.fetch_add(1);
        return ExecTaskStatus::RUNNING;
    }

private:
    TaskStopSourcePtr stop;
    std::atomic_size_t & wakes;
};

class SuspendedSource : public SourceOp
{
public:
    SuspendedSource(PipelineExecutorContext & context, const Block & header, OperatorStatus status_)
        : SourceOp(context, "")
        , status(status_)
    {
        setHeader(header);
    }
    String getName() const override { return "SuspendedSource"; }
    OperatorStatus readImpl(Block &) override { return status; }
    OperatorStatus awaitImpl() override
    {
        ADD_FAILURE() << "Stopped input must not be polled again";
        return OperatorStatus::WAITING;
    }
    OperatorStatus executeIOImpl() override
    {
        ADD_FAILURE() << "Stopped input must not resume IO";
        return OperatorStatus::IO_IN;
    }

private:
    OperatorStatus status;
};

class QueueFuture : public NotifyFuture
{
public:
    void registerTask(TaskPtr && task) override
    {
        queue.registerPipeReadTask(std::move(task), NotifyType::WAIT_ON_SHARED_QUEUE_READ);
    }
    LooseBoundedMPMCQueue<int> queue{1};
};

void finishTask(TaskPtr & task)
{
    if (task)
    {
        task->finalize();
        task.reset();
    }
}
} // namespace

class PipelineInputStopTest : public ExecutorTest
{
public:
    using SourceBuilder = std::function<void(PipelineExecGroupBuilder &, const Block &)>;

    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable(
            {"test", "input"},
            {{"v", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("v", {1, 2, 3})});
    }

    PipelineExecGroupBuilder buildPlan(
        const std::shared_ptr<tipb::DAGRequest> & request,
        PipelineExecutorContext & exec_context,
        size_t concurrency,
        const SourceBuilder & build_source = {})
    {
        dag_context = std::make_unique<DAGContext>(*request, "input_stop_test", concurrency);
        context.context->setDAGContext(dag_context.get());
        context.context->setMockStorage(context.mockStorage());
        context.context->setExecutorTest();
        PhysicalPlan physical_plan{*context.context, ""};
        physical_plan.build(request.get());
        plan_tree = physical_plan.outputAndOptimize();
        PipelineExecGroupBuilder builders;
        PhysicalPlanVisitor::visitPostOrder(plan_tree, [&](const auto & plan) {
            if (build_source && plan->childrenSize() == 0)
                build_source(builders, plan->getSampleBlock());
            else
                plan->buildPipelineExecGroup(exec_context, builders, *context.context, concurrency);
        });
        return builders;
    }

    static std::vector<TaskPtr> makeTasks(
        PipelineExecGroupBuilder & builders,
        PipelineExecutorContext & context,
        std::atomic_size_t & rows)
    {
        builders.transform([&](auto & builder) { builder.setSinkOp(std::make_unique<CountRowsSink>(context, rows)); });
        std::vector<TaskPtr> tasks;
        for (auto & pipeline : builders.build(false))
            tasks.push_back(std::make_unique<SimplePipelineTask>(context, "", std::move(pipeline)));
        return tasks;
    }

protected:
    PhysicalPlanNodePtr plan_tree;
    std::unique_ptr<DAGContext> dag_context;
};

TEST_F(PipelineInputStopTest, PlannerLimitWakesSourceBeforeAndAfterRegistration)
{
    // This exercises the registration race with an intentionally idle source. The finite Window case below
    // separately covers production backpressure. Removing PhysicalLimit's wiring must break this test.
    for (bool register_after_stop : {false, true})
    {
        MemoryTrackerSetter tracker(true, nullptr);
        PipelineExecutorContext exec_context;
        std::atomic_size_t rows{0};
        auto [active_sink, active_source] = SharedQueue::build(exec_context, 1, 1, -1, 1);
        auto [idle_sink, idle_source] = SharedQueue::build(exec_context, 1, 1, -1, 1);
        Block input;
        auto request = context.scan("test", "input").limit(1).build(context);
        auto builders = buildPlan(request, exec_context, 2, [&](auto & group, const auto & header) {
            input = header.cloneEmpty();
            auto column = header.getByPosition(0).type->createColumn();
            column->insert(Field(Int64(7)));
            column->insert(Field(Int64(8)));
            input.getByPosition(0).column = std::move(column);
            for (const auto & source : {active_source, idle_source})
                group.addConcurrency(std::make_unique<SharedQueueSourceOp>(exec_context, "", header, source));
        });
        auto tasks = makeTasks(builders, exec_context, rows);
        SCOPE_EXIT({
            clearNotifyFuture();
            exec_context.cancel();
            for (auto & task : tasks)
                finishTask(task);
            exec_context.wait();
        });
        ASSERT_EQ(tasks[1]->execute(), ExecTaskStatus::WAIT_FOR_NOTIFY);
        auto * future = current_notify_future;
        clearNotifyFuture();
        if (!register_after_stop)
        {
            setNotifyFuture(future);
            registerTaskToFuture(std::move(tasks[1]));
        }
        ASSERT_EQ(active_sink->tryPush(Block(input)), MPMCQueueResult::OK);
        ASSERT_EQ(tasks[0]->execute(), ExecTaskStatus::RUNNING);
        ASSERT_EQ(rows.load(), 1);
        ASSERT_EQ(tasks[0]->execute(), ExecTaskStatus::FINISHED);
        finishTask(tasks[0]);
        if (register_after_stop)
        {
            setNotifyFuture(future);
            registerTaskToFuture(std::move(tasks[1]));
        }
        exec_context.waitFor(std::chrono::seconds(10));
        EXPECT_FALSE(exec_context.isCancelled());
        EXPECT_TRUE(exec_context.toExecutionResult().is_success);
        EXPECT_EQ(rows.load(), 1);
        // The stop affects execution, not the underlying data queue.
        EXPECT_EQ(idle_sink->tryPush(Block(input)), MPMCQueueResult::OK);
    }
}

TEST_F(PipelineInputStopTest, PlannerLimitStopsBackpressuredProducer)
{
    MemoryTrackerSetter tracker(true, nullptr);
    PipelineExecutorContext exec_context;
    std::atomic_size_t rows{0};
    auto [queue_sink, queue_source] = SharedQueue::build(exec_context, 1, 1, -1, 1);
    Block input;
    auto request = context.scan("test", "input").limit(1).build(context);
    auto builders = buildPlan(request, exec_context, 2, [&](auto & group, const auto & header) {
        input = header.cloneEmpty();
        auto column = header.getByPosition(0).type->createColumn();
        column->insert(Field(Int64(1)));
        input.getByPosition(0).column = std::move(column);
        group.addConcurrency(std::make_unique<FiniteSource>(exec_context, header, 1000));
        group.getCurBuilder(0).setSinkOp(std::make_unique<SharedQueueSinkOp>(exec_context, "", queue_sink));
        group.addGroup();
        group.addConcurrency(std::make_unique<SharedQueueSourceOp>(exec_context, "", header, queue_source));
    });
    auto tasks = makeTasks(builders, exec_context, rows);
    SCOPE_EXIT({
        clearNotifyFuture();
        exec_context.cancel();
        for (auto & task : tasks)
            finishTask(task);
        exec_context.wait();
    });
    ASSERT_EQ(queue_sink->tryPush(Block(input)), MPMCQueueResult::OK);
    ASSERT_EQ(tasks[0]->execute(), ExecTaskStatus::WAIT_FOR_NOTIFY);
    registerTaskToFuture(std::move(tasks[0]));
    ASSERT_EQ(tasks[1]->execute(), ExecTaskStatus::RUNNING);
    ASSERT_EQ(tasks[1]->execute(), ExecTaskStatus::FINISHED);
    finishTask(tasks[1]);
    exec_context.waitFor(std::chrono::seconds(10));
    EXPECT_FALSE(exec_context.isCancelled());
    EXPECT_EQ(rows.load(), 1);
}

TEST_F(PipelineInputStopTest, LimitPreservesBackpressuredDownstreamOutput)
{
    MemoryTrackerSetter tracker(true, nullptr);
    PipelineExecutorContext exec_context;
    auto [output_sink, output_source] = SharedQueue::build(exec_context, 1, 1, -1, 1);
    Block input;
    auto request = context.scan("test", "input").limit(1).build(context);
    auto builders = buildPlan(request, exec_context, 1, [&](auto & group, const auto & header) {
        input = header.cloneEmpty();
        auto column = header.getByPosition(0).type->createColumn();
        column->insert(Field(Int64(1)));
        input.getByPosition(0).column = std::move(column);
        group.addConcurrency(std::make_unique<FiniteSource>(exec_context, header, 100));
    });
    builders.getCurBuilder(0).setSinkOp(std::make_unique<SharedQueueSinkOp>(exec_context, "", output_sink));
    auto pipelines = builders.build(false);
    TaskPtr task = std::make_unique<SimplePipelineTask>(exec_context, "", std::move(pipelines[0]));
    SCOPE_EXIT({
        clearNotifyFuture();
        exec_context.cancel();
        finishTask(task);
        exec_context.wait();
    });
    ASSERT_EQ(output_sink->tryPush(Block(input)), MPMCQueueResult::OK);
    ASSERT_EQ(task->execute(), ExecTaskStatus::WAIT_FOR_NOTIFY);
    EXPECT_TRUE(task->getNotifyStopSources().empty());
    registerTaskToFuture(std::move(task));
    Block output;
    ASSERT_EQ(output_source->tryPop(output), MPMCQueueResult::OK); // Remove the filler.
    exec_context.waitFor(std::chrono::seconds(10));
    ASSERT_EQ(output_source->tryPop(output), MPMCQueueResult::OK); // The last valid LIMIT block survives.
    EXPECT_EQ(output.rows(), 1);
    EXPECT_EQ(output_source->tryPop(output), MPMCQueueResult::FINISHED);
    EXPECT_FALSE(exec_context.isCancelled());
}

TEST_F(PipelineInputStopTest, FiniteWindowLimitPlan)
{
    MemoryTrackerSetter tracker(true, nullptr);
    PipelineExecutorContext exec_context;
    std::atomic_size_t rows{0};
    // Each result block exceeds the queue's byte limit, so a producer cannot buffer the remaining output.
    context.context->setSetting("max_block_size", Field(UInt64(1)));
    context.context->setSetting("max_buffered_bytes_in_executor", Field(UInt64(1)));
    ColumnWithNullableInt64 values;
    for (Int64 i = 0; i < 100; ++i)
        values.emplace_back(i);
    context.addMockTable(
        {"test", "window_input"},
        {{"v", TiDB::TP::TypeLongLong}},
        {toNullableVec<Int64>("v", values)});
    auto request
        = context.scan("test", "window_input")
              .sort({"v", false}, true)
              .window(RowNumber(), MockOrderByItemVec{{"v", false}}, MockPartitionByItemVec{}, buildDefaultRowsFrame())
              .limit(1)
              .build(context);
    auto builders = buildPlan(request, exec_context, 3);
    ASSERT_GT(builders.groupCnt(), 1);
    auto tasks = makeTasks(builders, exec_context, rows);
    SCOPE_EXIT({
        exec_context.cancel();
        for (auto & task : tasks)
            finishTask(task);
        exec_context.wait();
    });
    for (auto & task : tasks)
        TaskScheduler::instance->submit(std::move(task));
    exec_context.waitFor(std::chrono::seconds(10));
    EXPECT_FALSE(exec_context.isCancelled());
    EXPECT_TRUE(exec_context.toExecutionResult().is_success);
    EXPECT_EQ(rows.load(), 1);
}

TEST_F(PipelineInputStopTest, PlannerLimitLeavesExchangeReceiverOpen)
{
    for (const bool fine_grained : {false, true})
    {
        MemoryTrackerSetter tracker(true, nullptr);
        PipelineExecutorContext exec_context;
        std::atomic_size_t rows{0};
        tipb::ExchangeReceiver meta;
        meta.add_field_types()->set_tp(TiDB::TP::TypeLongLong);
        auto rpc_context = std::make_shared<GRPCReceiverContext>(meta, mpp::TaskMeta{}, nullptr, nullptr, false, false);
        Settings settings;
        settings.recv_queue_size = 1;
        // No RPCs are started: this unit test isolates receiver wait registration and queue state.
        auto receiver = std::make_shared<ExchangeReceiver>(rpc_context, 0, 2, "", "", fine_grained ? 2 : 0, settings);
        auto request = context.scan("test", "input").limit(1).build(context);
        auto builders = buildPlan(request, exec_context, 3, [&](auto & group, const auto & header) {
            group.addConcurrency(std::make_unique<FiniteSource>(exec_context, header, 1));
            for (size_t i = 0; i < 2; ++i)
            {
                group.addConcurrency(
                    std::make_unique<ExchangeReceiverSourceOp>(exec_context, "", receiver, fine_grained ? i : 0));
                auto & builder = group.getCurBuilder(i + 1);
                auto receiver_header = builder.getCurrentHeader();
                auto rename = std::make_shared<ExpressionActions>(receiver_header.getNamesAndTypesList());
                rename->add(
                    ExpressionAction::project({{receiver_header.getByPosition(0).name, header.getByPosition(0).name}}));
                builder.appendTransformOp(std::make_unique<ExpressionTransformOp>(exec_context, "", rename));
            }
        });
        auto tasks = makeTasks(builders, exec_context, rows);
        SCOPE_EXIT({
            clearNotifyFuture();
            receiver->close();
            exec_context.cancel();
            for (auto & task : tasks)
                finishTask(task);
            exec_context.wait();
        });
        for (size_t i = 1; i < tasks.size(); ++i)
        {
            ASSERT_EQ(tasks[i]->execute(), ExecTaskStatus::WAIT_FOR_NOTIFY);
            registerTaskToFuture(std::move(tasks[i]));
        }
        ASSERT_EQ(tasks[0]->execute(), ExecTaskStatus::RUNNING);
        ASSERT_EQ(tasks[0]->execute(), ExecTaskStatus::FINISHED);
        finishTask(tasks[0]);
        exec_context.waitFor(std::chrono::seconds(10));
        EXPECT_EQ(rows.load(), 1);
        EXPECT_FALSE(exec_context.isCancelled());
        for (size_t i = 0; i < 2; ++i)
        {
            ReceivedMessagePtr message;
            EXPECT_EQ(receiver->tryReceive(fine_grained ? i : 0, message), ReceiveStatus::empty);
            clearNotifyFuture();
        }
    }
}

TEST_F(PipelineInputStopTest, PlannerEmptyBuildStopsBackpressuredProducer)
{
    for (auto kind : {ASTTableJoin::Kind::Inner, ASTTableJoin::Kind::RightSemi})
    {
        MemoryTrackerSetter tracker(true, nullptr);
        PipelineExecutorContext exec_context;
        std::atomic_size_t rows{0};
        auto [queue_sink, queue_source] = SharedQueue::build(exec_context, 1, 2, -1, 1);
        auto request = context.scan("test", "input").build(context);
        auto builders = buildPlan(request, exec_context, 2, [&](auto & group, const auto & header) {
            group.addConcurrency(std::make_unique<FiniteSource>(exec_context, header, 100));
            group.getCurBuilder(0).setSinkOp(std::make_unique<SharedQueueSinkOp>(exec_context, "", queue_sink));
            group.addGroup();
            for (size_t i = 0; i < 2; ++i)
                group.addConcurrency(std::make_unique<SharedQueueSourceOp>(exec_context, "", header, queue_source));
        });
        auto header = builders.getCurrentHeader();
        auto key = header.getByPosition(0).name;
        NamesAndTypes schema{{key, header.getByPosition(0).type}};
        SpillConfig spill_config("/tmp", "input_stop_test", 0, 0, 0, nullptr);
        auto join = std::make_shared<Join>(
            Names{key},
            Names{key},
            std::vector<UInt8>{0},
            kind,
            "input_stop_test",
            0,
            0,
            spill_config,
            spill_config,
            RestoreConfig{1, 0, 0},
            schema,
            RegisterOperatorSpillContext{},
            nullptr,
            TiDB::TiDBCollators{},
            JoinNonEqualConditions{},
            1,
            0,
            "",
            "",
            0,
            true);
        join->initBuild(header, 1);
        join->finalize(Names{key});
        join->setLocalProbeSource(true);
        join->setInitActiveBuildThreads();
        ASSERT_TRUE(join->finishOneBuild(0));
        join->finalizeBuild();
        ASSERT_TRUE(join->shouldSkipProbe());
        // Exercise PhysicalJoinProbe's actual input-stop wiring and its normal empty-build completion path.
        PhysicalJoinProbe probe("join_probe", schema, "", plan_tree, join, nullptr);
        probe.buildPipelineExecGroup(exec_context, builders, *context.context, 2);
        auto tasks = makeTasks(builders, exec_context, rows);
        SCOPE_EXIT({
            clearNotifyFuture();
            exec_context.cancel();
            for (auto & task : tasks)
                finishTask(task);
            exec_context.wait();
        });
        ASSERT_EQ(tasks[0]->execute(), ExecTaskStatus::RUNNING); // Fill the queue.
        ASSERT_EQ(tasks[0]->execute(), ExecTaskStatus::WAIT_FOR_NOTIFY);
        registerTaskToFuture(std::move(tasks[0]));
        for (size_t i = 1; i < tasks.size(); ++i)
            TaskScheduler::instance->submit(std::move(tasks[i]));
        exec_context.waitFor(std::chrono::seconds(10));
        EXPECT_EQ(rows.load(), 0);
        EXPECT_FALSE(exec_context.isCancelled());
        EXPECT_FALSE(join->isProbeStopped());
        EXPECT_TRUE(join->isProbeFinishedForPipeline());
    }
}

TEST_F(PipelineInputStopTest, StopsPendingIOAndPollingWithoutMaskingErrors)
{
    for (const auto status : {OperatorStatus::IO_IN, OperatorStatus::WAITING})
    {
        for (const bool fail_query : {false, true})
        {
            MemoryTrackerSetter tracker(true, nullptr);
            PipelineExecutorContext exec_context;
            std::atomic_size_t rows{0};
            auto request = context.scan("test", "input").build(context);
            auto builders = buildPlan(request, exec_context, 1, [&](auto & group, const auto & header) {
                group.addConcurrency(std::make_unique<SuspendedSource>(exec_context, header, status));
            });
            auto stop = builders.addInputStop();
            auto tasks = makeTasks(builders, exec_context, rows);
            SCOPE_EXIT({
                clearNotifyFuture();
                exec_context.cancel();
                for (auto & task : tasks)
                    finishTask(task);
                exec_context.wait();
            });
            ASSERT_EQ(
                tasks[0]->execute(),
                status == OperatorStatus::IO_IN ? ExecTaskStatus::IO_IN : ExecTaskStatus::WAITING);
            stop->stop();
            if (fail_query)
                exec_context.onErrorOccurred("input stop must not mask this error");
            auto next = status == OperatorStatus::IO_IN ? tasks[0]->executeIO() : tasks[0]->await();
            ASSERT_EQ(next, fail_query ? ExecTaskStatus::CANCELLED : ExecTaskStatus::RUNNING);
            if (!fail_query)
                ASSERT_EQ(tasks[0]->execute(), ExecTaskStatus::FINISHED);
            finishTask(tasks[0]);
            exec_context.waitFor(std::chrono::seconds(10));
            EXPECT_EQ(exec_context.toExecutionResult().is_success, !fail_query);
            EXPECT_EQ(rows.load(), 0);
        }
    }
}

TEST_F(PipelineInputStopTest, ZeroLimitFinishesWithoutInput)
{
    MemoryTrackerSetter tracker(true, nullptr);
    PipelineExecutorContext exec_context;
    std::atomic_size_t rows{0};
    auto [queue_sink, queue_source] = SharedQueue::build(exec_context, 1, 2, -1, 1);
    auto request = context.scan("test", "input").limit(0).build(context);
    auto builders = buildPlan(request, exec_context, 2, [&](auto & group, const auto & header) {
        for (size_t i = 0; i < 2; ++i)
            group.addConcurrency(std::make_unique<SharedQueueSourceOp>(exec_context, "", header, queue_source));
    });
    auto tasks = makeTasks(builders, exec_context, rows);
    SCOPE_EXIT({
        clearNotifyFuture();
        exec_context.cancel();
        for (auto & task : tasks)
            finishTask(task);
        exec_context.wait();
    });
    for (auto & task : tasks)
    {
        ASSERT_EQ(task->execute(), ExecTaskStatus::FINISHED);
        finishTask(task);
    }
    EXPECT_EQ(rows.load(), 0);
    EXPECT_FALSE(exec_context.isCancelled());
}

TEST_F(PipelineInputStopTest, StopRacesWithResourceNotification)
{
    MemoryTrackerSetter tracker(true, nullptr);
    PipelineExecutorContext exec_context;
    std::atomic_size_t wakes{0};
    // Keep the context alive while constructing subsequent tasks.
    exec_context.incActiveRefCount();
    SCOPE_EXIT({
        clearNotifyFuture();
        exec_context.cancel();
        exec_context.decActiveRefCount();
        exec_context.wait();
    });
    for (size_t i = 0; i < 32; ++i)
    {
        OneTimeNotifyFuture future(NotifyType::WAIT_ON_SHARED_QUEUE_READ);
        auto stop = std::make_shared<TaskStopSource>();
        setNotifyFuture(&future);
        registerTaskToFuture(std::make_unique<WakeTask>(exec_context, stop, wakes));
        std::thread notifier([&] { future.finish(); });
        stop->stop();
        notifier.join();
    }
    // notifyImpl runs synchronously, so completion of both callers establishes exactly-once notification.
    EXPECT_EQ(wakes.load(), 32);
    EXPECT_FALSE(exec_context.isCancelled());
}

TEST_F(PipelineInputStopTest, NotifyOneSkipsAnAlreadyStoppedWaiter)
{
    MemoryTrackerSetter tracker(true, nullptr);
    PipelineExecutorContext exec_context;
    std::atomic_size_t wakes{0};
    QueueFuture future;
    auto first_stop = std::make_shared<TaskStopSource>();
    auto second_stop = std::make_shared<TaskStopSource>();
    exec_context.incActiveRefCount();
    SCOPE_EXIT({
        future.queue.cancel();
        exec_context.cancel();
        exec_context.decActiveRefCount();
        exec_context.wait();
    });
    setNotifyFuture(&future);
    registerTaskToFuture(std::make_unique<WakeTask>(exec_context, first_stop, wakes));
    first_stop->stop();
    setNotifyFuture(&future);
    registerTaskToFuture(std::make_unique<WakeTask>(exec_context, second_stop, wakes));
    ASSERT_EQ(future.queue.tryPush(1), MPMCQueueResult::OK);
    EXPECT_EQ(wakes.load(), 2);
}
} // namespace DB::tests
