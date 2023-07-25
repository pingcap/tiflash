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

#include <Storages/DeltaMerge/Remote/RNWorkers.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/TiFlashStorageTestBasic.h>

using namespace DB::DM::tests;
using namespace DB::DM::Remote;

namespace DB::DM::Remote::tests
{

// Testing the pipeline and error handle of RNWorkers.

class MockRNWorkerFetchPages : public RNWorkerFetchPages
{
public:
    explicit MockRNWorkerFetchPages(const RNWorkerFetchPagesPtr & w)
        : RNWorkerFetchPages(w->source_queue, w->result_queue, w->concurrency)
    {}
    MockRNWorkerFetchPages(const RNWorkers::ChannelPtr & source_queue, const RNWorkers::ChannelPtr & result_queue, size_t concurrency)
        : RNWorkerFetchPages(source_queue, result_queue, concurrency)
    {}

    inline static std::atomic<bool> always_throw_exception{false};

protected:
    void doWorkImpl([[maybe_unused]] const RNReadSegmentTaskPtr & task) override
    {
        RUNTIME_CHECK(!always_throw_exception, getName());
        std::cout << fmt::format("{}: {}\n", getName(), fmt::ptr(task.get()));
    }
};

class MockRNWorkerPrepareStreams : public RNWorkerPrepareStreams
{
public:
    explicit MockRNWorkerPrepareStreams(const RNWorkerPrepareStreamsPtr & w)
        : RNWorkerPrepareStreams(w->source_queue, w->concurrency)
    {}
    MockRNWorkerPrepareStreams(const RNWorkers::ChannelPtr & source_queue, size_t concurrency)
        : RNWorkerPrepareStreams(source_queue, concurrency)
    {}

    inline static std::atomic<bool> always_throw_exception{false};

protected:
    void doWorkImpl([[maybe_unused]] const RNReadSegmentTaskPtr & task) override
    {
        RUNTIME_CHECK(!always_throw_exception, getName());
        std::cout << fmt::format("{}: {}\n", getName(), fmt::ptr(task.get()));
    }
};

std::pair<RNReadTaskPtr, std::set<UInt64>> mockRNReadTask(size_t n)
{
    auto param = std::make_shared<StorageDisaggregated::SegmentReadTaskParam>();
    param->prepared_tasks = std::make_shared<RNWorkers::Channel>(n);
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

std::pair<RNWorkersPtr, std::set<UInt64>> mockRNWorkers(size_t task_count, bool use_shared_rn_workers)
{
    auto context = DMTestEnv::getContext();
    context->getSettingsRef().set("dt_use_shared_rn_workers", use_shared_rn_workers ? "true" : "false");
    auto [read_task, task_addrs] = mockRNReadTask(task_count);
    auto rn_workers = RNWorkers::create(*context, read_task, 8);
    RUNTIME_CHECK(rn_workers->prepared_tasks != nullptr);
    RUNTIME_CHECK(read_task->segment_read_tasks.front()->param->prepared_tasks != nullptr);
    RUNTIME_CHECK(read_task->segment_read_tasks.front()->param->prepared_tasks.get() == rn_workers->prepared_tasks.get());

    if (use_shared_rn_workers)
    {
        static std::once_flag mock_flag;
        std::call_once(mock_flag, []() {
            RNWorkers::shared_worker_fetch_pages->source_queue->cancel(); // Stop all worker in the same pipeline.

            const Int64 max_queue_size = 16384;

            RNWorkers::shared_worker_fetch_pages = std::make_shared<MockRNWorkerFetchPages>(
                std::make_shared<RNWorkers::Channel>(max_queue_size),
                std::make_shared<RNWorkers::Channel>(max_queue_size),
                std::thread::hardware_concurrency());

            RNWorkers::shared_worker_prepare_streams = std::make_shared<MockRNWorkerPrepareStreams>(
                RNWorkers::shared_worker_fetch_pages->result_queue,
                std::thread::hardware_concurrency());

            RNWorkers::shared_worker_fetch_pages->startInBackground();
            RNWorkers::shared_worker_prepare_streams->start();
        });
    }
    else
    {
        rn_workers->worker_fetch_pages = std::make_shared<MockRNWorkerFetchPages>(rn_workers->worker_fetch_pages);
        rn_workers->worker_prepare_streams = std::make_shared<MockRNWorkerPrepareStreams>(rn_workers->worker_prepare_streams);
    }
    return {rn_workers, task_addrs};
}

void pipelineTest(size_t i, bool use_shared_rn_workers, size_t task_count)
{
    auto [rn_workers, task_addrs] = mockRNWorkers(task_count, use_shared_rn_workers);
    ASSERT_EQ(task_addrs.size(), task_count);
    rn_workers->startInBackground();
    std::set<UInt64> ready_task_addrs;
    while (true)
    {
        RNReadSegmentTaskPtr task;
        auto res = rn_workers->getReadyTask(task);
        if (res == MPMCQueueResult::FINISHED)
        {
            ASSERT_EQ(ready_task_addrs.size(), task_count) << use_shared_rn_workers;
            ASSERT_EQ(ready_task_addrs, task_addrs);
            LOG_DEBUG(DB::Logger::get(), "Thread#{} use_shared_rn_workers={} task_count={}", i, use_shared_rn_workers, task_count);
            break;
        }
        ASSERT_EQ(res, MPMCQueueResult::OK) << rn_workers->getCancelReason() << " " << use_shared_rn_workers;
        ready_task_addrs.emplace(reinterpret_cast<UInt64>(task.get()));
    }
}

void loopPipelineTest(size_t i, bool use_shared_rn_workers, size_t max_task_count)
{
    for (size_t task_count = 1; task_count < max_task_count; task_count++)
    {
        pipelineTest(i, use_shared_rn_workers, task_count);
    }
}

void concurrentPipelineTest(bool use_shared_rn_workers, size_t concurrency, size_t max_task_count)
{
    std::vector<std::thread> threads;
    for (size_t i = 0; i < concurrency; i++)
    {
        threads.emplace_back(std::thread(loopPipelineTest, i, use_shared_rn_workers, max_task_count));
    }

    for (auto & t : threads)
    {
        t.join();
    }
}

void pipelineExceptionTest(bool use_shared_rn_workers, size_t task_count, const String & cancel_reason_keyword)
{
    auto [rn_workers, task_addrs] = mockRNWorkers(task_count, use_shared_rn_workers);
    ASSERT_EQ(task_addrs.size(), task_count);
    rn_workers->startInBackground();
    RNReadSegmentTaskPtr task;
    auto res = rn_workers->getReadyTask(task);
    ASSERT_EQ(res, MPMCQueueResult::CANCELLED);
    auto reason = rn_workers->getCancelReason();
    ASSERT_NE(reason.find(cancel_reason_keyword), std::string::npos) << reason << ", " << cancel_reason_keyword;
}

constexpr size_t test_max_task_count = 100;

TEST(RNWorkersTest, SharedRNWorkers)
try
{
    concurrentPipelineTest(true, 10, test_max_task_count);
    // pipelineTest(1, true, 10);
}
CATCH

TEST(RNWorkersTest, NotSharedRNWorkers)
try
{
    concurrentPipelineTest(false, 10, test_max_task_count);
}
CATCH

TEST(RNWorkersTest, FetchPageException)
try
{
    MockRNWorkerFetchPages::always_throw_exception.store(true);
    pipelineExceptionTest(true, 10, "getName() = FetchPages");

    MockRNWorkerPrepareStreams::always_throw_exception.store(true);
    pipelineExceptionTest(true, 10, "getName() = FetchPages");

    MockRNWorkerFetchPages::always_throw_exception.store(false);
    pipelineExceptionTest(true, 10, "getName() = PrepareStreams");
}
CATCH

} // namespace DB::DM::Remote::tests
