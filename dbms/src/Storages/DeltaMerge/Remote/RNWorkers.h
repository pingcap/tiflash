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

    explicit RNWorkers(
        LoggerPtr log,
        const std::vector<RNReadSegmentTaskPtr> & unprocessed_seg_tasks,
        const ColumnDefinesPtr & columns_to_read_,
        UInt64 read_tso_,
        const PushDownFilterPtr & push_down_filter_,
        ReadMode read_mode_);

    /// Get the channel which outputs ready-for-read segment tasks.
    ChannelPtr getPreparedChannel() const;

    void startInBackground();

    void wait();

    ~RNWorkers()
    {
        wait();
    }

private:
    RNWorkerFetchPagesPtr worker_fetch_pages;
    RNWorkerPrepareStreamsPtr worker_prepare_streams;
};

} // namespace DB::DM::Remote
