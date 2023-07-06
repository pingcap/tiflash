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

#include <Storages/DeltaMerge/Remote/RNReadTask.h>
#include <Storages/DeltaMerge/Remote/RNWorkers.h>

namespace DB::DM::Remote
{

RNWorkers::RNWorkers(const Options & options)
{
    size_t n = options.read_task->segment_read_tasks.size();
    RUNTIME_CHECK(n > 0, n);

    worker_fetch_pages = RNWorkerFetchPages::create({
        .source_queue = std::make_shared<Channel>(n),
        .result_queue = std::make_shared<Channel>(n),
        .log = options.log,
        .concurrency = n,
        .cluster = options.cluster,
    });

    worker_prepare_streams = RNWorkerPrepareStreams::create({
        .source_queue = worker_fetch_pages->result_queue,
        .result_queue = std::make_shared<Channel>(n),
        .log = options.log,
        .concurrency = n,
        .columns_to_read = options.columns_to_read,
        .read_tso = options.read_tso,
        .push_down_filter = options.push_down_filter,
        .read_mode = options.read_mode,
    });

    // TODO: Can we push the task that all delta/stable data hit local cache first?
    for (auto const & seg_task : options.read_task->segment_read_tasks)
    {
        auto push_result = worker_fetch_pages->source_queue->tryPush(seg_task);
        RUNTIME_CHECK(push_result == MPMCQueueResult::OK, magic_enum::enum_name(push_result));
    }
    worker_fetch_pages->source_queue->finish();
}

void RNWorkers::startInBackground()
{
    worker_fetch_pages->startInBackground();
    worker_prepare_streams->startInBackground();
}

void RNWorkers::wait()
{
    worker_fetch_pages->wait();
    worker_prepare_streams->wait();
}

RNWorkers::ChannelPtr RNWorkers::getReadyChannel() const
{
    return worker_prepare_streams->result_queue;
}
} // namespace DB::DM::Remote
