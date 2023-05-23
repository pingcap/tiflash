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

#include <Common/ThreadedWorker.h>
#include <Storages/DeltaMerge/Remote/RNReadTask_fwd.h>
#include <pingcap/kv/Cluster.h>

#include <boost/noncopyable.hpp>

namespace DB::DM::Remote
{

class RNWorkerFetchPages;
using RNWorkerFetchPagesPtr = std::shared_ptr<RNWorkerFetchPages>;

/// This worker fetch page data from Write Node, and then write page data into the local cache.
class RNWorkerFetchPages
    : private boost::noncopyable
    , public ThreadedWorker<RNReadSegmentTaskPtr, RNReadSegmentTaskPtr>
{
protected:
    RNReadSegmentTaskPtr doWork(const RNReadSegmentTaskPtr & task) override;

private:
    void doFetchPages(
        const RNReadSegmentTaskPtr & seg_task,
        std::shared_ptr<disaggregated::FetchDisaggPagesRequest> request);

private:
    const pingcap::kv::Cluster * cluster;

public:
    explicit RNWorkerFetchPages(
        std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> source_queue_,
        std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> result_queue_,
        LoggerPtr log_,
        size_t concurrency_,
        pingcap::kv::Cluster * cluster_)
        : ThreadedWorker<RNReadSegmentTaskPtr, RNReadSegmentTaskPtr>(
            source_queue_,
            result_queue_,
            log_,
            concurrency_)
        , cluster(cluster_)
    {}
};

} // namespace DB::DM::Remote
