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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Remote/RNReadTask.h>
#include <Storages/DeltaMerge/Remote/RNWorkers.h>

namespace DB::DM::Remote
{

std::pair<size_t, size_t> calculateConcurrency(const Context & context, size_t seg_count, size_t num_streams)
{
    const auto & settings = context.getSettingsRef();
    size_t fetch_pages_concurrency = num_streams * settings.dt_fetch_page_concurrency_scale;
    size_t prepare_streams_concurrency = num_streams * settings.dt_prepare_stream_concurrency_scale;
    RUNTIME_CHECK(
        fetch_pages_concurrency > 0 && prepare_streams_concurrency > 0,
        fetch_pages_concurrency,
        prepare_streams_concurrency);
    return {std::min(fetch_pages_concurrency, seg_count), std::min(prepare_streams_concurrency, seg_count)};
}

std::pair<size_t, size_t> calculateSharedConcurrency(const Context & context)
{
    auto logical_cores = context.getServerInfo().has_value() ? context.getServerInfo()->cpu_info.logical_cores
                                                             : std::thread::hardware_concurrency();
    const auto & settings = context.getSettingsRef();
    size_t fetch_pages_concurrency = logical_cores * settings.dt_fetch_page_concurrency_scale;
    size_t prepare_streams_concurrency = logical_cores * settings.dt_prepare_stream_concurrency_scale;
    RUNTIME_CHECK(
        fetch_pages_concurrency > 0 && prepare_streams_concurrency > 0,
        fetch_pages_concurrency,
        prepare_streams_concurrency);
    return {fetch_pages_concurrency, prepare_streams_concurrency};
}

std::pair<RNWorkerFetchPagesPtr, RNWorkerPrepareStreamsPtr> createPipelineWorkers(
    size_t fetch_pages_concurrency,
    size_t prepare_streams_concurrency,
    Int64 queue_size)
{
    auto fetch_pages = RNWorkerFetchPages::create({
        .source_queue = std::make_shared<MPMCQueue<RNReadSegmentTaskPtr>>(queue_size),
        .result_queue = std::make_shared<MPMCQueue<RNReadSegmentTaskPtr>>(queue_size),
        .concurrency = fetch_pages_concurrency,
    });

    auto prepare_streams = RNWorkerPrepareStreams::create({
        .source_queue = fetch_pages->result_queue,
        .result_queue = nullptr,
        .concurrency = prepare_streams_concurrency,
    });

    return {fetch_pages, prepare_streams};
}

RNWorkers::RNWorkers(const Context & context, const RNReadTaskPtr & read_task, size_t num_streams)
    : enable_shared_workers(context.getSettingsRef().dt_enable_shared_rn_workers)
    , pending_tasks(read_task)
    , log(read_task->segment_read_tasks.front()->param->log)
{
    prepared_tasks = pending_tasks->segment_read_tasks.front()->param->initPreparedTaskQueue(
        pending_tasks->segment_read_tasks.size());

    if (enable_shared_workers)
    {
        std::call_once(shared_init_flag, [&]() {
            auto [fetch_pages_concurrency, prepare_streams_concurrency] = calculateSharedConcurrency(context);
            LOG_INFO(
                log,
                "Shared RNWorkers concurrency: fetch_pages={} prepare_streams={}",
                fetch_pages_concurrency,
                prepare_streams_concurrency);
            std::tie(shared_fetch_pages, shared_prepare_streams) = createPipelineWorkers(
                fetch_pages_concurrency,
                prepare_streams_concurrency,
                context.getSettingsRef().dt_shared_max_queue_size);
            shared_fetch_pages->start();
            shared_prepare_streams->start();
        });
    }
    else
    {
        auto seg_count = read_task->segment_read_tasks.size();
        auto [fetch_pages_concurrency, prepare_streams_concurrency]
            = calculateConcurrency(context, seg_count, num_streams);
        LOG_DEBUG(
            log,
            "RNWorkers concurrency: fetch_pages={} prepare_streams={}",
            fetch_pages_concurrency,
            prepare_streams_concurrency);
        std::tie(worker_fetch_pages, worker_prepare_streams)
            = createPipelineWorkers(fetch_pages_concurrency, prepare_streams_concurrency, seg_count);
        // Wokers will be start in startInBackground.
    }
}

void RNWorkers::addTasks()
{
    auto start_channel = getStartChannel();
    // TODO: Can we push the task that all delta/stable data hit local cache first?
    for (auto const & seg_task : pending_tasks->segment_read_tasks)
    {
        // Can not block threads here, because addTasks is called in startInBackground in RNSegmentInputStream by threads of computing.
        // Blocking computing threads can affect consumption of prepared streams.
        auto res = start_channel->tryPush(seg_task);
        RUNTIME_CHECK_MSG(res != MPMCQueueResult::FULL, "Too many task in processing: {}", start_channel->size());
        RUNTIME_CHECK(res == MPMCQueueResult::OK, magic_enum::enum_name(res));
    }
    pending_tasks.reset(); // Avoid hold the life time of tasks.
    if (!enable_shared_workers)
    {
        start_channel->finish();
    }
}

void RNWorkers::startInBackground()
{
    std::call_once(start_flag, [&]() {
        addTasks();
        if (!enable_shared_workers)
        {
            worker_fetch_pages->start();
            worker_prepare_streams->start();
        }
    });
}

RNWorkers::ChannelPtr RNWorkers::getReadyChannel() const
{
    return prepared_tasks;
}

RNWorkers::ChannelPtr RNWorkers::getStartChannel() const
{
    return enable_shared_workers ? shared_fetch_pages->source_queue : worker_fetch_pages->source_queue;
}

RNWorkersPtr RNWorkers::create(const Context & context, const RNReadTaskPtr & read_task, size_t num_streams)
{
    RUNTIME_CHECK(num_streams > 0);
    RUNTIME_CHECK(!read_task->segment_read_tasks.empty());
    return std::make_shared<RNWorkers>(context, read_task, num_streams);
}

void RNWorkers::shutdown()
{
    if (shared_fetch_pages != nullptr)
    {
        LOG_INFO(DB::Logger::get(), "Shared RNWorkers is going to shutdown.");
        shared_fetch_pages->source_queue->cancelWith("shutdown");
        shared_fetch_pages.reset();
        shared_prepare_streams.reset();
        LOG_INFO(DB::Logger::get(), "Shared RNWorkers has been shutdown.");
    }
}
} // namespace DB::DM::Remote
