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

#pragma once

#include <Common/MPMCQueue.h>
#include <Storages/DeltaMerge/Remote/RNWorkerFetchPages.h>
#include <Storages/DeltaMerge/Remote/RNWorkerPrepareStreams.h>
#include <Storages/DeltaMerge/Remote/RNWorkers_fwd.h>

#include <boost/noncopyable.hpp>
#include <memory>

namespace DB::DM::Remote
{

class RNWorkers
    : private boost::noncopyable
{
public:
    using Channel = MPMCQueue<RNReadSegmentTaskPtr>;
    using ChannelPtr = std::shared_ptr<Channel>;

public:
    /// Get the channel which outputs ready-for-read segment tasks.
    ChannelPtr getReadyChannel() const;

    void startInBackground();

    void wait();

    ~RNWorkers()
    {
        wait();
    }

public:
    struct Options
    {
        const LoggerPtr log;
        const RNReadTaskPtr & read_task;
        const ColumnDefinesPtr & columns_to_read;
        const UInt64 read_tso;
        const PushDownFilterPtr & push_down_filter;
        const ReadMode read_mode;
        const pingcap::kv::Cluster * cluster;
    };

    explicit RNWorkers(const Options & options);

    static RNWorkersPtr create(const Options & options)
    {
        return std::make_shared<RNWorkers>(options);
    }

private:
    RNWorkerFetchPagesPtr worker_fetch_pages;
    RNWorkerPrepareStreamsPtr worker_prepare_streams;
};

} // namespace DB::DM::Remote
