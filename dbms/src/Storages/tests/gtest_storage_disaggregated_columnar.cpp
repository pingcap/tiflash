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

#include <Common/SyncPoint/SyncPoint.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageDisaggregatedColumnar.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

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

TEST(ColumnarCancellationTest, SuccessfulLimitCancelsConcurrentRead)
{
    auto context = TiFlashTestEnv::getContext();
    DM::ColumnDefines columns;
    columns.emplace_back(1, "i", std::make_shared<DataTypeInt64>(), Field{});
    auto cancellation = std::make_shared<ColumnarCancellation>();
    auto pool = RNColumnarReadTask::createForCancellationTest(cancellation);
    std::promise<void> entered;
    auto entered_future = entered.get_future().share();
    std::promise<void> release;
    auto release_future = release.get_future().share();
    class PausedColumnarStream : public RNColumnarInputStream
    {
    public:
        PausedColumnarStream(const Options & options, std::promise<void> & entered_, std::shared_future<void> release_)
            : RNColumnarInputStream(options)
            , entered(entered_)
            , release(std::move(release_))
        {}
        Block read(FilterPtr & filter, bool return_filter) override
        {
            entered.set_value();
            release.wait();
            return RNColumnarInputStream::read(filter, return_filter);
        }
        std::promise<void> & entered;
        std::shared_future<void> release;
    };
    const String executor_id = "columnar_review_limit";
    RNColumnarInputStream::Options options{
        *context,
        Logger::get("columnar_review"),
        pool,
        nullptr,
        columns,
        MutSup::invalid_col_id,
        1,
        executor_id};
    auto paused = std::make_shared<PausedColumnarStream>(options, entered, release_future);
    class OneBlockStream : public IProfilingBlockInputStream
    {
    public:
        OneBlockStream(Block header_, std::shared_future<void> entered_)
            : header(std::move(header_))
            , entered(std::move(entered_))
        {}
        String getName() const override { return "OneBlock"; }
        Block getHeader() const override { return header; }
        Block readImpl() override
        {
            if (done)
                return {};
            done = true;
            entered.wait();
            auto output = header.cloneEmpty();
            for (auto & column : output)
                column.column->assumeMutable()->insert(Field(Int64(1)));
            return output;
        }
        Block header;
        std::shared_future<void> entered;
        bool done = false;
    };
    auto ready = std::make_shared<OneBlockStream>(paused->getHeader(), entered_future);
    auto union_stream = std::make_shared<UnionBlockInputStream<StreamUnionMode::Basic>>(
        BlockInputStreams{ready, paused},
        BlockInputStreams{},
        2,
        0,
        "columnar_review");
    auto limit = std::make_shared<LimitBlockInputStream>(union_stream, 1, 0, "columnar_review");
    limit->readPrefix();
    EXPECT_EQ(limit->read().rows(), 1);
    EXPECT_FALSE(limit->read());
    release.set_value();
    EXPECT_NO_THROW(limit->readSuffix());
}

#ifdef FIU_ENABLE
TEST(ColumnarCancellationTest, SoftCancelInFlightReadPreservesQueryCancellation)
{
    for (const String mode : {"soft", "kill", "query", "context"})
    {
        SCOPED_TRACE(mode);
        auto context = TiFlashTestEnv::getContext();
        auto source = std::make_shared<CancellationSource>();
        context->setCancellationSource(source);
        auto cancellation = std::make_shared<ColumnarCancellation>();
        auto pool = RNColumnarReadTask::createForCancellationTest(cancellation);
        auto columns = makeColumnDefines();
        const String executor_id = "columnar_cancel_stream";
        RNColumnarInputStream stream(
            {*context, Logger::get(executor_id), pool, nullptr, columns, MutSup::invalid_col_id, 1, executor_id});
        auto sync = SyncPointCtl::enableInScope("columnar_stream_read");
        auto read = std::async(std::launch::async, [&] {
            try
            {
                FilterPtr filter;
                EXPECT_FALSE(stream.read(filter, false));
                return 0;
            }
            catch (const Exception & e)
            {
                return e.code();
            }
        });
        sync.waitAndPause();
        stream.cancel(false);
        if (mode == "kill")
            stream.cancel(true);
        else if (mode == "query")
            cancellation->cancel();
        else if (mode == "context")
            source->cancel();
        sync.next();
        EXPECT_EQ(read.get(), mode == "soft" ? 0 : ErrorCodes::QUERY_WAS_CANCELLED);
        EXPECT_EQ(cancellation->isCancelled(), mode == "query");
    }
}

TEST(ColumnarCancellationTest, CancelExecutingMaterializationDrainsExecutor)
{
    class MaterializeTask : public Task
    {
    public:
        MaterializeTask(PipelineExecutorContext & context, RNColumnarReadTaskPtr pool_, RNColumnarReaderWorkPtr work_)
            : Task(context, "columnar_cancel_test", ExecTaskStatus::IO_IN)
            , pool(std::move(pool_))
            , work(std::move(work_))
        {
            describeOperators("RNColumnarSourceOp");
        }
        ExecTaskStatus executeImpl() override { return ExecTaskStatus::IO_IN; }
        ExecTaskStatus executeIOImpl() override
        {
            pool->getOrCreateReader(work);
            return ExecTaskStatus::FINISHED;
        }
        RNColumnarReadTaskPtr pool;
        RNColumnarReaderWorkPtr work;
    };

    PipelineExecutorContext context;
    auto cancellation = std::make_shared<ColumnarCancellation>();
    context.addCancellationCallback([cancellation] { cancellation->cancel(); });
    auto pool = RNColumnarReadTask::createForCancellationTest(cancellation);
    auto work = std::make_shared<RNColumnarReaderWork>(RNColumnarReaderPlan{1, 1, 1, {}});
    work->state = RNColumnarReaderMaterializeState::Creating;
    auto sync = SyncPointCtl::enableInScope("columnar_reader_wait");
    std::promise<void> finished;
    auto result = finished.get_future();
    auto task = std::make_unique<MaterializeTask>(context, pool, work);
    std::thread worker([task = std::move(task), &finished]() mutable {
        EXPECT_EQ(task->executeIO(), ExecTaskStatus::ERROR);
        task->finalize();
        task.reset();
        finished.set_value();
    });
    sync.waitAndPause();
    auto dump = context.dumpActiveTasks();
    EXPECT_NE(dump.find("status=IO_IN"), String::npos);
    EXPECT_NE(dump.find("operators=RNColumnarSourceOp"), String::npos);
    context.cancel();
    sync.next();
    // No materializer notification is needed for cancellation to wake the waiter.
    EXPECT_EQ(result.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    // Always release a broken implementation so a regression fails instead of hanging CI.
    {
        std::lock_guard lock(work->mutex);
        work->exception = std::make_exception_ptr(Exception("test cleanup"));
        work->state = RNColumnarReaderMaterializeState::Failed;
    }
    work->cv.notify_all();
    worker.join();
    context.waitFor(std::chrono::seconds(1));
    EXPECT_NE(context.dumpActiveTasks().find("active_ref_count=0 tasks=0"), String::npos);
}

TEST(ColumnarCancellationTest, PrefetchRetainsExecutorUntilItStops)
{
    for (bool cancel_query : {true, false})
    {
        SCOPED_TRACE(cancel_query);
        PipelineExecutorContext context;
        auto cancellation = std::make_shared<ColumnarCancellation>();
        context.addCancellationCallback([cancellation] { cancellation->cancel(); });
        auto pool = RNColumnarReadTask::createForCancellationTest(cancellation, &context);
        std::weak_ptr<RNColumnarReadTask> weak_pool = pool;
        auto sync = SyncPointCtl::enableInScope("columnar_reader_create");
        // Claiming the first reader starts prefetch of the second.
        auto work = pool->tryAcquireReaderWork();
        EXPECT_TRUE(work.has_value());
        sync.waitAndPause();
        auto pool_for_finish = pool;
        pool.reset();
        if (cancel_query)
            context.cancel();
        else
            pool_for_finish->finishSource();
        pool_for_finish.reset();
        auto drained = std::async(std::launch::async, [&] { context.wait(); });
        EXPECT_EQ(drained.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
        EXPECT_FALSE(weak_pool.expired());
        sync.next();
        EXPECT_EQ(drained.wait_for(std::chrono::seconds(3)), std::future_status::ready);
        drained.get();
        EXPECT_TRUE(weak_pool.expired());
        EXPECT_NE(context.dumpActiveTasks().find("active_ref_count=0"), String::npos);
    }
}
#endif
} // namespace DB::tests

#endif
