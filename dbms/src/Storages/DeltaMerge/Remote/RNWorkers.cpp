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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Remote/RNReadTask.h>
#include <Storages/DeltaMerge/Remote/RNWorkers.h>

namespace DB::DM::Remote
{

void RNWorkers::initSharedWorkers(const Context & context)
{
    Stopwatch sw;
    auto logical_cores = context.getServerInfo().has_value() ? context.getServerInfo()->cpu_info.logical_cores : std::thread::hardware_concurrency();
    const auto & settings = context.getSettingsRef();
    const size_t fetch_pages_concurrency = std::ceil(settings.dt_fetch_page_concurrency_scale * logical_cores);
    const size_t prepare_streams_concurrency = std::ceil(settings.dt_prepare_stream_concurrency_scale * logical_cores);
    const Int64 max_queue_size = settings.dt_shared_max_queue_size;
    LOG_INFO(
        DB::Logger::get(),
        "logical_cores={}, fetch_pages_concurrency={} prepare_streams_concurrency={} max_queue_size={}",
        logical_cores,
        fetch_pages_concurrency,
        prepare_streams_concurrency,
        max_queue_size);

    shared_worker_fetch_pages = RNWorkerFetchPages::create(
        std::make_shared<Channel>(max_queue_size),
        std::make_shared<Channel>(max_queue_size),
        fetch_pages_concurrency);

    shared_worker_prepare_streams = RNWorkerPrepareStreams::create(
        shared_worker_fetch_pages->result_queue,
        prepare_streams_concurrency);

    shared_worker_fetch_pages->start();
    shared_worker_prepare_streams->start();
    LOG_INFO(DB::Logger::get(), "initSharedWorkers cost {}ms", sw.elapsedMilliseconds());
}

RNWorkers::RNWorkers(const Context & context, const RNReadTaskPtr & read_task)
    : pending_read_task(read_task)
    , prepared_tasks(read_task->segment_read_tasks.front()->param->prepared_tasks)
    , task_count(read_task->segment_read_tasks.size())
{
    std::call_once(init_flag, initSharedWorkers, context);
}

RNWorkers::RNWorkers(const Context & context, const RNReadTaskPtr & read_task, size_t num_streams)
    : prepared_tasks(read_task->segment_read_tasks.front()->param->prepared_tasks)
    , task_count(read_task->segment_read_tasks.size())
{
    size_t n = read_task->segment_read_tasks.size();
    auto fetch_pages_concurrency = n;
    auto prepare_streams_concurrency = n;
    const auto & settings = context.getSettingsRef();
    if (settings.dt_fetch_page_concurrency_scale > 0.0)
    {
        fetch_pages_concurrency = std::min(std::ceil(num_streams * settings.dt_fetch_page_concurrency_scale), fetch_pages_concurrency);
    }
    if (settings.dt_prepare_stream_concurrency_scale > 0.0)
    {
        prepare_streams_concurrency = std::min(std::ceil(num_streams * settings.dt_prepare_stream_concurrency_scale), prepare_streams_concurrency);
    }

    worker_fetch_pages = RNWorkerFetchPages::create(
        std::make_shared<Channel>(n),
        std::make_shared<Channel>(n),
        fetch_pages_concurrency);

    worker_prepare_streams = RNWorkerPrepareStreams::create(
        worker_fetch_pages->result_queue,
        prepare_streams_concurrency);

    // TODO: Can we push the task that all delta/stable data hit local cache first?
    addTasks(read_task, worker_fetch_pages->source_queue);
    worker_fetch_pages->source_queue->finish();
}

void RNWorkers::startInBackground()
{
    if (worker_fetch_pages != nullptr && worker_prepare_streams != nullptr)
    {
        RUNTIME_CHECK(pending_read_task == nullptr);
        worker_fetch_pages->start();
        worker_prepare_streams->start();
    }
    else
    {
        std::call_once(add_tasks_flags, [&]() {
            RUNTIME_CHECK(pending_read_task != nullptr);
            addTasks(pending_read_task, shared_worker_fetch_pages->source_queue);
            pending_read_task.reset();
        });
    }
}

} // namespace DB::DM::Remote
