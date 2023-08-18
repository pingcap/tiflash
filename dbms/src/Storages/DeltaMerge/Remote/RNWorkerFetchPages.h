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

    String getName() const noexcept override { return "FetchPages"; }

private:
    void doFetchPages(const RNReadSegmentTaskPtr & seg_task, const disaggregated::FetchDisaggPagesRequest & request);

private:
    const pingcap::kv::Cluster * cluster;

public:
    struct Options
    {
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & source_queue;
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & result_queue;
        const LoggerPtr & log;
        const size_t concurrency;
        const pingcap::kv::Cluster * cluster;
    };

    explicit RNWorkerFetchPages(const Options & options)
        : ThreadedWorker<RNReadSegmentTaskPtr, RNReadSegmentTaskPtr>(
            options.source_queue,
            options.result_queue,
            options.log,
            options.concurrency)
        , cluster(options.cluster)
    {}

    static RNWorkerFetchPagesPtr create(const Options & options)
    {
        return std::make_shared<RNWorkerFetchPages>(options);
    }

    ~RNWorkerFetchPages() override { wait(); }
};

} // namespace DB::DM::Remote
