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
#include <Storages/DeltaMerge/SegmentReadTask.h>

#include <boost/noncopyable.hpp>

namespace DB::DM::Remote
{

class RNWorkerFetchPages;
using RNWorkerFetchPagesPtr = std::shared_ptr<RNWorkerFetchPages>;

/// This worker fetch page data from Write Node, and then write page data into the local cache.
class RNWorkerFetchPages
    : private boost::noncopyable
    , public ThreadedWorker<SegmentReadTaskPtr, SegmentReadTaskPtr>
{
protected:
    SegmentReadTaskPtr doWork(const SegmentReadTaskPtr & task) override
    {
        task->fetchPages();
        return task;
    }

    String getName() const noexcept override { return "FetchPages"; }

public:
    struct Options
    {
        const std::shared_ptr<MPMCQueue<SegmentReadTaskPtr>> & source_queue;
        const std::shared_ptr<MPMCQueue<SegmentReadTaskPtr>> & result_queue;
        const LoggerPtr & log;
        const size_t concurrency;
    };

    explicit RNWorkerFetchPages(const Options & options)
        : ThreadedWorker<SegmentReadTaskPtr, SegmentReadTaskPtr>(
            options.source_queue,
            options.result_queue,
            options.log,
            options.concurrency)
    {}

    static RNWorkerFetchPagesPtr create(const Options & options)
    {
        return std::make_shared<RNWorkerFetchPages>(options);
    }

    ~RNWorkerFetchPages() override { wait(); }
};

} // namespace DB::DM::Remote
