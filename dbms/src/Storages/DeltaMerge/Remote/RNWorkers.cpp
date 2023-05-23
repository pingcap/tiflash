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

#include <Storages/DeltaMerge/Remote/RNWorkers.h>

namespace DB::DM::Remote
{

RNWorkers::RNWorkers(
    LoggerPtr log,
    const std::vector<RNReadSegmentTaskPtr> & unprocessed_seg_tasks,
    const ColumnDefinesPtr & columns_to_read_,
    UInt64 read_tso_,
    const PushDownFilterPtr & push_down_filter_,
    ReadMode read_mode_)
{
    size_t n = unprocessed_seg_tasks.size();
    worker_fetch_pages = std::make_shared<RNWorkerFetchPages>(
        /* source */ std::make_shared<Channel>(n),
        /* result */ std::make_shared<Channel>(n),
        log,
        /* concurrency */ n);

    worker_prepare_streams = std::make_shared<RNWorkerPrepareStreams>(
        /* source */ worker_fetch_pages->result_queue,
        /* result */ std::make_shared<Channel>(n),
        log,
        /* concurrency */ n,
        columns_to_read_,
        read_tso_,
        push_down_filter_,
        read_mode_);

    for (auto const & seg_task : unprocessed_seg_tasks)
    {
        auto push_result = worker_fetch_pages->source_queue->tryPush(seg_task);
        RUNTIME_CHECK(push_result == MPMCQueueResult::OK);
    }
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

RNWorkers::ChannelPtr RNWorkers::getPreparedChannel() const
{
    return worker_prepare_streams->result_queue;
}
} // namespace DB::DM::Remote
