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

#pragma once

#include <Common/MPMCQueue.h>
#include <Storages/DeltaMerge/Remote/RNWorkerFetchPages.h>
#include <Storages/DeltaMerge/Remote/RNWorkerPrepareStreams.h>
#include <Storages/DeltaMerge/Remote/RNWorkers_fwd.h>

#include <boost/noncopyable.hpp>
#include <memory>

namespace DB::DM::Remote
{

class RNWorkers : private boost::noncopyable
{
public:
    using Channel = MPMCQueue<SegmentReadTaskPtr>;
    using ChannelPtr = std::shared_ptr<Channel>;

public:
    /// Get the channel which outputs ready-for-read segment tasks.
    ChannelPtr getReadyChannel() const;

    void startInBackground();

    void wait();

    ~RNWorkers() { wait(); }

public:
    struct Options
    {
        const LoggerPtr log;
        const ColumnDefinesPtr & columns_to_read;
        const UInt64 start_ts;
        const PushDownExecutorPtr & push_down_executor;
        const ReadMode read_mode;
    };

    RNWorkers(const Context & context, SegmentReadTasks && read_tasks, const Options & options, size_t num_streams);

    static RNWorkersPtr create(
        const Context & context,
        SegmentReadTasks && read_tasks,
        const Options & options,
        size_t num_streams)
    {
        return std::make_shared<RNWorkers>(context, std::move(read_tasks), options, num_streams);
    }

private:
    ChannelPtr empty_channel;

    RNWorkerFetchPagesPtr worker_fetch_pages;
    RNWorkerPrepareStreamsPtr worker_prepare_streams;
};

} // namespace DB::DM::Remote
