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
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/Remote/RNReadTask_fwd.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <pingcap/kv/Cluster.h>

#include <boost/noncopyable.hpp>

namespace DB::DM::Remote
{

class RNWorkerPrepareStreams;
using RNWorkerPrepareStreamsPtr = std::shared_ptr<RNWorkerPrepareStreams>;

/// This worker prepare data streams for reading.
/// For example, when S3 files of the stable layer does not exist locally,
/// they will be downloaded.
class RNWorkerPrepareStreams
    : private boost::noncopyable
    , public ThreadedWorker<RNReadSegmentTaskPtr, RNReadSegmentTaskPtr>
{
protected:
    RNReadSegmentTaskPtr doWork(const RNReadSegmentTaskPtr & task) override;

    String getName() const noexcept override { return "PrepareStreams"; }

private:
    virtual void doWorkImpl(const RNReadSegmentTaskPtr & task);

public:
    static RNWorkerPrepareStreamsPtr create(
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & source_queue,
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & result_queue,
        const size_t concurrency)
    {
        return std::make_shared<RNWorkerPrepareStreams>(source_queue, result_queue, concurrency);
    }

    explicit RNWorkerPrepareStreams(
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & source_queue,
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & result_queue,
        const size_t concurrency)
        : ThreadedWorker<RNReadSegmentTaskPtr, RNReadSegmentTaskPtr>(
            source_queue,
            result_queue,
            DB::Logger::get(getName()),
            concurrency)
    {}

    ~RNWorkerPrepareStreams() override { wait(); }
};

} // namespace DB::DM::Remote
