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

#include <Storages/DeltaMerge/Remote/RNReadTask_fwd.h>
#include <Storages/DeltaMerge/Remote/StorageThreadWorker.h>
#include <kvproto/disaggregated.pb.h>

#include <boost/noncopyable.hpp>

namespace DB::DM::Remote
{

class RNWorkerFetchPages;
using RNWorkerFetchPagesPtr = std::shared_ptr<RNWorkerFetchPages>;

/// This worker fetch page data from Write Node, and then write page data into the local cache.
class RNWorkerFetchPages
    : public StorageThreadWorker<RNReadSegmentTaskPtr, RNReadSegmentTaskPtr>
    , private boost::noncopyable
{
public:
    static RNWorkerFetchPagesPtr create(
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & source_queue,
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & result_queue,
        const size_t concurrency)
    {
        return std::make_shared<RNWorkerFetchPages>(source_queue, result_queue, concurrency);
    }

    RNWorkerFetchPages(
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & source_queue,
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & result_queue,
        const size_t concurrency)
        : StorageThreadWorker<RNReadSegmentTaskPtr, RNReadSegmentTaskPtr>(
            "FetchPages",
            source_queue,
            result_queue,
            concurrency)
    {}

protected:
    RNReadSegmentTaskPtr doWork(const RNReadSegmentTaskPtr & task) noexcept override;

private:
    static void doFetchPages(
        const RNReadSegmentTaskPtr & seg_task,
        const disaggregated::FetchDisaggPagesRequest & request);

    virtual void doWorkImpl(const RNReadSegmentTaskPtr & seg_task);
};

} // namespace DB::DM::Remote
