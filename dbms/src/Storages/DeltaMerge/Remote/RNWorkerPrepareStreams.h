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

#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/Remote/RNReadTask.h>
#include <Storages/DeltaMerge/Remote/RNReadTask_fwd.h>
#include <Storages/DeltaMerge/Remote/StorageThreadWorker.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <pingcap/kv/Cluster.h>

#include <boost/noncopyable.hpp>

namespace DB::DM::Remote
{

class RNWorkerPrepareStreams;
using RNWorkerPrepareStreamsPtr = std::shared_ptr<RNWorkerPrepareStreams>;

class RNWorkerPrepareStreams : public StorageThreadWorker<RNReadSegmentTaskPtr, RNReadSegmentTaskPtr>
    , private boost::noncopyable
{
public:
    static RNWorkerPrepareStreamsPtr create(
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & source_queue_,
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & /*result_queue_*/,
        const size_t concurrency_)
    {
        return std::make_shared<RNWorkerPrepareStreams>(source_queue_, concurrency_);
    }

    RNWorkerPrepareStreams(
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & source_queue_,
        const size_t concurrency_)
        : StorageThreadWorker("PrepareStreams", source_queue_, nullptr, concurrency_)
    {}

protected:
    RNReadSegmentTaskPtr doWork(const RNReadSegmentTaskPtr & task) noexcept override;

    virtual void doWorkImpl(const RNReadSegmentTaskPtr & task);
};

} // namespace DB::DM::Remote
