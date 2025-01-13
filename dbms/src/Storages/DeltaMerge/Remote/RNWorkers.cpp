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
#include <Storages/DeltaMerge/Remote/RNWorkers.h>
namespace DB::DM::Remote
{

RNWorkers::RNWorkers(
    const Context & context,
    SegmentReadTasks && read_tasks,
    const Options & options,
    size_t num_streams)
{
    RUNTIME_CHECK(num_streams > 0, num_streams);
    size_t n = read_tasks.size();
    if (n == 0)
    {
        empty_channel = std::make_shared<Channel>(0);
        empty_channel->finish();
        return;
    }

    auto fetch_pages_concurrency = n;
    auto prepare_streams_concurrency = n;
    const auto & settings = context.getSettingsRef();
    if (settings.dt_fetch_page_concurrency_scale > 0.0)
    {
        fetch_pages_concurrency
            = std::min(std::ceil(num_streams * settings.dt_fetch_page_concurrency_scale), fetch_pages_concurrency);
    }
    if (settings.dt_prepare_stream_concurrency_scale > 0.0)
    {
        prepare_streams_concurrency = std::min(
            std::ceil(num_streams * settings.dt_prepare_stream_concurrency_scale),
            prepare_streams_concurrency);
    }

    worker_fetch_pages = RNWorkerFetchPages::create({
        .source_queue = std::make_shared<Channel>(n),
        .result_queue = std::make_shared<Channel>(n),
        .log = options.log,
        .concurrency = fetch_pages_concurrency,
    });

    worker_prepare_streams = RNWorkerPrepareStreams::create({
        .source_queue = worker_fetch_pages->result_queue,
        .result_queue = std::make_shared<Channel>(n),
        .log = options.log,
        .concurrency = prepare_streams_concurrency,
        .columns_to_read = options.columns_to_read,
        .start_ts = options.start_ts,
        .push_down_executor = options.push_down_executor,
        .read_mode = options.read_mode,
    });

    // TODO: Can we push the task that all delta/stable data hit local cache first?
    for (auto const & seg_task : read_tasks)
    {
        auto push_result = worker_fetch_pages->source_queue->tryPush(seg_task);
        RUNTIME_CHECK(push_result == MPMCQueueResult::OK, magic_enum::enum_name(push_result));
    }
    worker_fetch_pages->source_queue->finish();
}

void RNWorkers::startInBackground()
{
    if (!empty_channel)
    {
        worker_fetch_pages->startInBackground();
        worker_prepare_streams->startInBackground();
    }
}

void RNWorkers::wait()
{
    if (!empty_channel)
    {
        worker_fetch_pages->wait();
        worker_prepare_streams->wait();
    }
}

RNWorkers::ChannelPtr RNWorkers::getReadyChannel() const
{
    if (empty_channel)
        return empty_channel;
    return worker_prepare_streams->result_queue;
}
} // namespace DB::DM::Remote
