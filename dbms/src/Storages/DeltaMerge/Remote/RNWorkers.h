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
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Remote/RNWorkerDispatchSegmentReadTasks.h>
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
    void startInBackground();

    void wait();

    ~RNWorkers()
    {
        prepared_tasks->finish();
        wait();
    }

    MPMCQueueResult getReadyTask(RNReadSegmentTaskPtr & task)
    {
        auto res = prepared_tasks->pop(task);
        if (res == MPMCQueueResult::OK)
        {
            ready_task_count++;
            if (ready_task_count >= task_count)
            {
                prepared_tasks->finish();
            }
        }
        return res;
    }

    String getCancelReason() const
    {
        return prepared_tasks->getCancelReason();
    }

public:
    RNWorkers(const Context & context, const RNReadTaskPtr & read_task, size_t num_streams);
    RNWorkers(const Context & context, const RNReadTaskPtr & read_task);

    static RNWorkersPtr create(const Context & context, const RNReadTaskPtr & read_task, size_t num_streams)
    {
        RUNTIME_CHECK(!read_task->segment_read_tasks.empty());
        RUNTIME_CHECK(num_streams > 0);
        return context.getSettingsRef().dt_use_shared_rn_workers ? std::make_shared<RNWorkers>(context, read_task) : std::make_shared<RNWorkers>(context, read_task, num_streams);
    }

private:
    static void addTasks(const RNReadTaskPtr & read_task, const ChannelPtr & q)
    {
        for (auto const & seg_task : read_task->segment_read_tasks)
        {
            auto push_result = q->push(seg_task);
            RUNTIME_CHECK(push_result == MPMCQueueResult::OK, magic_enum::enum_name(push_result));
        }
    }

    static void initSharedWorkers(const Context & context);

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif
    RNWorkerFetchPagesPtr worker_fetch_pages;
    RNWorkerPrepareStreamsPtr worker_prepare_streams;
    RNReadTaskPtr pending_read_task;
    ChannelPtr prepared_tasks;
    size_t task_count;
    size_t ready_task_count = 0;

    inline static std::once_flag init_flag;
    inline static RNWorkerFetchPagesPtr shared_worker_fetch_pages;
    inline static RNWorkerPrepareStreamsPtr shared_worker_prepare_streams;
    inline static RNWorkerDispatchSegmentReadTasksPtr shared_worker_dispatch_tasks;
};

} // namespace DB::DM::Remote
