// Copyright 2023 PingCAP, Ltd.
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

#include <Common/FailPoint.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Remote/RNReadTask.h>
#include <Storages/DeltaMerge/Remote/RNWorkers.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/StorageDisaggregated.h>
#include <TestUtils/TiFlashStorageTestBasic.h>

using namespace DB::DM::tests;
using namespace DB::DM::Remote;

namespace DB::FailPoints
{
extern const char pause_when_fetch_page[];
} // namespace DB::FailPoints

namespace DB::DM::Remote::tests
{

// Testing the pipeline and error handle of RNWorkers.

class MockRNWorkerFetchPages : public RNWorkerFetchPages
{
public:
    explicit MockRNWorkerFetchPages(const RNWorkerFetchPagesPtr & w)
        : RNWorkerFetchPages({w->source_queue, w->result_queue, w->concurrency})
    {}
    MockRNWorkerFetchPages(
        const RNWorkers::ChannelPtr & source_queue,
        const RNWorkers::ChannelPtr & result_queue,
        size_t concurrency)
        : RNWorkerFetchPages({source_queue, result_queue, concurrency})
    {}

    inline static std::atomic<bool> always_throw_exception{false};

protected:
    void doWorkImpl(const RNReadSegmentTaskPtr & task) override
    {
        RUNTIME_CHECK(!always_throw_exception, getName());
        LOG_TRACE(log, "Mock_{}: {}\n", getName(), fmt::ptr(task.get()));
    }
};

class MockRNWorkerPrepareStreams : public RNWorkerPrepareStreams
{
public:
    explicit MockRNWorkerPrepareStreams(const RNWorkerPrepareStreamsPtr & w)
        : RNWorkerPrepareStreams({w->source_queue, w->result_queue, w->concurrency})
    {}
    MockRNWorkerPrepareStreams(const RNWorkers::ChannelPtr & source_queue, size_t concurrency)
        : RNWorkerPrepareStreams({source_queue, nullptr, concurrency})
    {}

    inline static std::atomic<bool> always_throw_exception{false};

protected:
    void doWorkImpl(const RNReadSegmentTaskPtr & task) override
    {
        RUNTIME_CHECK(!always_throw_exception, getName());
        LOG_TRACE(log, "Mock_{}: {}\n", getName(), fmt::ptr(task.get()));
    }
};

std::pair<RNReadTaskPtr, std::set<UInt64>> mockRNReadTask(size_t n)
{
    auto param = std::make_shared<RNSegmentReadTaskParam>(nullptr, DM::ReadMode::Bitmap, nullptr, 0, nullptr, nullptr);
    param->log = DB::Logger::get();
    std::vector<RNReadSegmentTaskPtr> segment_read_tasks(n);
    std::set<UInt64> task_addrs;
    for (size_t i = 0; i < n; i++)
    {
        segment_read_tasks[i] = std::make_shared<RNReadSegmentTask>(RNReadSegmentMeta{}, param);
        task_addrs.emplace(reinterpret_cast<UInt64>(segment_read_tasks[i].get()));
    }
    return {RNReadTask::create(segment_read_tasks), task_addrs};
}

constexpr size_t test_max_task_count = 100;
constexpr size_t test_worker_concurrency = 32;

std::pair<RNWorkersPtr, std::set<UInt64>> mockRNWorkers(size_t task_count, bool enable_shared_rn_workers)
{
    auto context = DMTestEnv::getContext();
    context->getSettingsRef().set("dt_enable_shared_rn_workers", enable_shared_rn_workers ? "true" : "false");
    Int64 max_queue_size = context->getSettingsRef().dt_shared_max_queue_size;
    auto [read_task, task_addrs] = mockRNReadTask(task_count);
    auto rn_workers = RNWorkers::create(*context, read_task, 8);
    RUNTIME_CHECK(rn_workers->getReadyChannel() != nullptr);
    RUNTIME_CHECK(read_task->segment_read_tasks.front()->param->getPreparedQueueStatus() == MPMCQueueStatus::NORMAL);

    if (enable_shared_rn_workers)
    {
        static std::once_flag mock_flag;
        std::call_once(mock_flag, [max_queue_size]() {
            RNWorkers::shutdown();

            RNWorkers::shared_fetch_pages = std::make_shared<MockRNWorkerFetchPages>(
                std::make_shared<RNWorkers::Channel>(max_queue_size),
                std::make_shared<RNWorkers::Channel>(max_queue_size),
                test_worker_concurrency);

            RNWorkers::shared_prepare_streams = std::make_shared<MockRNWorkerPrepareStreams>(
                RNWorkers::shared_fetch_pages->result_queue,
                test_worker_concurrency);

            RNWorkers::shared_fetch_pages->start();
            RNWorkers::shared_prepare_streams->start();
        });
    }
    else
    {
        RNWorkers::ChannelPtr source_queue;
        rn_workers->worker_fetch_pages->source_queue.swap(source_queue);
        RNWorkers::ChannelPtr result_queue;
        rn_workers->worker_fetch_pages->result_queue.swap(result_queue);

        rn_workers->worker_fetch_pages = std::make_shared<MockRNWorkerFetchPages>(
            source_queue,
            result_queue,
            rn_workers->worker_fetch_pages->concurrency);

        rn_workers->worker_prepare_streams = std::make_shared<MockRNWorkerPrepareStreams>(
            result_queue,
            rn_workers->worker_prepare_streams->concurrency);
    }
    return {rn_workers, task_addrs};
}

void pipelineTest(size_t i, bool enable_shared_rn_workers, size_t task_count)
{
    auto [rn_workers, task_addrs] = mockRNWorkers(task_count, enable_shared_rn_workers);
    ASSERT_EQ(task_addrs.size(), task_count);
    rn_workers->startInBackground();
    std::set<UInt64> ready_task_addrs;
    while (true)
    {
        RNReadSegmentTaskPtr task;
        auto res = rn_workers->getReadyChannel()->pop(task);
        if (res == MPMCQueueResult::FINISHED)
        {
            ASSERT_EQ(ready_task_addrs.size(), task_count) << enable_shared_rn_workers;
            ASSERT_EQ(ready_task_addrs, task_addrs);
            LOG_DEBUG(
                DB::Logger::get(),
                "Thread#{} enable_shared_rn_workers={} task_count={}",
                i,
                enable_shared_rn_workers,
                task_count);
            break;
        }
        ASSERT_EQ(res, MPMCQueueResult::OK)
            << rn_workers->getReadyChannel()->getCancelReason() << " " << enable_shared_rn_workers;
        ready_task_addrs.emplace(reinterpret_cast<UInt64>(task.get()));
    }
}

void loopPipelineTest(size_t i, bool enable_shared_rn_workers, size_t max_task_count)
{
    for (size_t task_count = 1; task_count < max_task_count; task_count++)
    {
        pipelineTest(i, enable_shared_rn_workers, task_count);
    }
}

void concurrentPipelineTest(bool enable_shared_rn_workers, size_t concurrency, size_t max_task_count)
{
    std::vector<std::thread> threads;
    for (size_t i = 0; i < concurrency; i++)
    {
        threads.emplace_back(std::thread(loopPipelineTest, i, enable_shared_rn_workers, max_task_count));
    }

    for (auto & t : threads)
    {
        t.join();
    }
}

void pipelineExceptionTest(bool enable_shared_rn_workers, size_t task_count, const String & cancel_reason_keyword)
{
    auto [rn_workers, task_addrs] = mockRNWorkers(task_count, enable_shared_rn_workers);
    ASSERT_EQ(task_addrs.size(), task_count);
    rn_workers->startInBackground();
    RNReadSegmentTaskPtr task;
    auto res = rn_workers->getReadyChannel()->pop(task);
    ASSERT_EQ(res, MPMCQueueResult::CANCELLED);
    auto reason = rn_workers->getReadyChannel()->getCancelReason();
    ASSERT_NE(reason.find(cancel_reason_keyword), std::string::npos) << reason << ", " << cancel_reason_keyword;
}

TEST(RNWorkersTest, SharedRNWorkers)
try
{
    concurrentPipelineTest(true, 10, test_max_task_count);
}
CATCH

TEST(RNWorkersTest, NotSharedRNWorkers)
try
{
    concurrentPipelineTest(false, 10, test_max_task_count);
}
CATCH

TEST(RNWorkersTest, PipelineException)
try
{
    SCOPE_EXIT({
        MockRNWorkerFetchPages::always_throw_exception.store(false);
        MockRNWorkerPrepareStreams::always_throw_exception.store(false);
    });
    MockRNWorkerFetchPages::always_throw_exception.store(true);
    pipelineExceptionTest(true, 10, "getName() = FetchPages");

    MockRNWorkerPrepareStreams::always_throw_exception.store(true);
    pipelineExceptionTest(true, 10, "getName() = FetchPages");

    MockRNWorkerFetchPages::always_throw_exception.store(false);
    pipelineExceptionTest(true, 10, "getName() = PrepareStreams");
}
CATCH

TEST(RNWorkersTest, QueryAbort)
try
{
    constexpr Int64 task_count = 1000;
    auto [rn_workers, task_addrs] = mockRNWorkers(task_count, true);
    ASSERT_EQ(task_addrs.size(), task_count);

    fiu_init(0); // init failpoint
    DB::FailPointHelper::enableFailPoint(DB::FailPoints::pause_when_fetch_page);

    auto disable_fail_point_once = []() {
        static std::once_flag flag;
        std::call_once(flag, []() { DB::FailPointHelper::disableFailPoint(DB::FailPoints::pause_when_fetch_page); });
    };
    SCOPE_EXIT({ disable_fail_point_once(); });

    rn_workers->startInBackground();

    auto & fetch_pages = RNWorkers::shared_fetch_pages;
    auto & prepare_streams = RNWorkers::shared_prepare_streams;
    ASSERT_GE(fetch_pages->source_queue->size(), task_count - fetch_pages->concurrency);
    ASSERT_EQ(prepare_streams->source_queue->size(), 0);

    auto prepared_tasks = rn_workers->getReadyChannel();
    ASSERT_EQ(prepared_tasks->getStatus(), MPMCQueueStatus::NORMAL);
    ASSERT_EQ(prepared_tasks->size(), 0);

    rn_workers.reset(); // Query abort.

    ASSERT_EQ(MPMCQueueStatus::CANCELLED, prepared_tasks->getStatus())
        << magic_enum::enum_name(prepared_tasks->getStatus());
    disable_fail_point_once();

    while (fetch_pages->source_queue->size() > 0)
    {
        ASSERT_EQ(prepare_streams->source_queue->size(), 0);
        ASSERT_EQ(prepared_tasks->size(), 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(prepare_streams->source_queue->size(), 0);
    ASSERT_EQ(prepared_tasks->size(), 0);
}
CATCH

TEST(RNWorkersTest, QueueFull)
try
{
    auto context = DMTestEnv::getContext();
    auto max_queue_size = context->getSettingsRef().dt_shared_max_queue_size;
    auto task_count = max_queue_size + test_worker_concurrency + 1;
    auto [rn_workers, task_addrs] = mockRNWorkers(task_count, true);
    ASSERT_EQ(task_addrs.size(), task_count);

    fiu_init(0); // init failpoint
    DB::FailPointHelper::enableFailPoint(DB::FailPoints::pause_when_fetch_page);
    auto disable_fail_point_once = []() {
        static std::once_flag flag;
        std::call_once(flag, []() { DB::FailPointHelper::disableFailPoint(DB::FailPoints::pause_when_fetch_page); });
    };
    SCOPE_EXIT({ disable_fail_point_once(); });

    try
    {
        rn_workers->startInBackground();
        FAIL() << "Should not come here";
    }
    catch (const DB::Exception & e)
    {
        auto errmsg
            = fmt::format("Check res != MPMCQueueResult::FULL failed: Too many task in processing: {}", max_queue_size);
        ASSERT_EQ(e.message(), errmsg);
    }
}
CATCH

TEST(RNWorkersTest, RefCycle)
try
{
    std::weak_ptr<MPMCQueue<RNReadSegmentTaskPtr>> weak_prepared_tasks;
    {
        constexpr Int64 task_count = 1000;
        auto [rn_workers, task_addrs] = mockRNWorkers(task_count, true);
        ASSERT_EQ(task_addrs.size(), task_count);
        rn_workers->startInBackground();
        weak_prepared_tasks = rn_workers->getReadyChannel();
        while (rn_workers->getReadyChannel()->size() == 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_EQ(weak_prepared_tasks.lock().get(), rn_workers->getReadyChannel().get());
        rn_workers.reset(); // Query abort.
    }
    ASSERT_TRUE(weak_prepared_tasks.expired());
}
CATCH

} // namespace DB::DM::Remote::tests
