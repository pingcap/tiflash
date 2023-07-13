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

namespace DB
{
class Context;
};

namespace DB::DM::Remote
{

// Create an RNWorker object for each request.
// The RNWorker object encapsulates the interfaces that submits segments to the thread pool and returns to each request.
class RNWorkers : private boost::noncopyable
{
public:
    using Channel = MPMCQueue<RNReadSegmentTaskPtr>;
    using ChannelPtr = std::shared_ptr<Channel>;

    // Get the channel which outputs ready-for-read segment tasks.
    ChannelPtr getReadyChannel() const;

    // Submit segment read tasks and start background threads if shared workers is disabled.
    void startInBackground();

    RNWorkers(const Context & context, const RNReadTaskPtr & read_task, size_t num_streams);

    ~RNWorkers() { prepared_tasks->cancel(); }

    static RNWorkersPtr create(const Context & context, const RNReadTaskPtr & read_task, size_t num_streams);
    static void shutdown();

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif
    ChannelPtr getStartChannel() const;
    void addTasks();

    bool enable_shared_workers;
    RNReadTaskPtr pending_tasks;
    ChannelPtr prepared_tasks;
    LoggerPtr log;

    std::once_flag start_flag; // Prevent `startInBackground` from being called repeatly.

    // Use when enable_shared_workers is false.
    RNWorkerFetchPagesPtr worker_fetch_pages;
    RNWorkerPrepareStreamsPtr worker_prepare_streams;

    // Use when enable_shared_workers is true.
    inline static std::once_flag shared_init_flag;
    inline static RNWorkerFetchPagesPtr shared_fetch_pages;
    inline static RNWorkerPrepareStreamsPtr shared_prepare_streams;
};

} // namespace DB::DM::Remote
