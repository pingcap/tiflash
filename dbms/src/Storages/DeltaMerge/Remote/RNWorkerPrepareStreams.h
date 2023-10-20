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

public:
    const ColumnDefinesPtr columns_to_read;
    const UInt64 read_tso;
    const PushDownFilterPtr push_down_filter;
    const ReadMode read_mode;

public:
    struct Options
    {
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & source_queue;
        const std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> & result_queue;
        const LoggerPtr & log;
        const size_t concurrency;
        const ColumnDefinesPtr & columns_to_read;
        const UInt64 read_tso;
        const PushDownFilterPtr & push_down_filter;
        const ReadMode read_mode;
    };

    static RNWorkerPrepareStreamsPtr create(const Options & options)
    {
        return std::make_shared<RNWorkerPrepareStreams>(options);
    }

    explicit RNWorkerPrepareStreams(const Options & options)
        : ThreadedWorker<RNReadSegmentTaskPtr, RNReadSegmentTaskPtr>(
            options.source_queue,
            options.result_queue,
            options.log,
            options.concurrency)
        , columns_to_read(options.columns_to_read)
        , read_tso(options.read_tso)
        , push_down_filter(options.push_down_filter)
        , read_mode(options.read_mode)
    {}

    ~RNWorkerPrepareStreams() override { wait(); }

    bool initInputStream(const RNReadSegmentTaskPtr & task, bool enable_delta_index_error_fallback);

    // Only use in unit-test.
    RNReadSegmentTaskPtr testDoWork(const RNReadSegmentTaskPtr & task) { return doWork(task); }
};

} // namespace DB::DM::Remote
