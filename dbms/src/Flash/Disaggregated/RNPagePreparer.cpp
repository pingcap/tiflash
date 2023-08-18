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

#include <Common/Exception.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Disaggregated/RNPagePreparer.h>
#include <Flash/Disaggregated/RNPageReceiver.h>
#include <IO/IOThreadPools.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Remote/RNRemoteReadTask.h>
#include <common/logger_useful.h>

#include <future>

namespace DB
{

RNPagePreparer::RNPagePreparer(
    DM::RNRemoteReadTaskPtr remote_read_tasks_,
    std::shared_ptr<RNPageReceiver> receiver_,
    const DM::ColumnDefinesPtr & columns_to_read,
    size_t max_streams_,
    const String & req_id,
    const String & executor_id,
    bool do_prepare_)
    : threads_num(max_streams_ * 2) // these threads involve disk IO, 2x scale for better CPU utilization
    , do_prepare(do_prepare_)
    , remote_read_tasks(std::move(remote_read_tasks_))
    , receiver(std::move(receiver_))
    , live_persisters(threads_num)
    , state(PageReceiverState::NORMAL)
    , total_rows(0)
    , total_pages(0)
    , exc_log(Logger::get(req_id, executor_id))
{
    assert(columns_to_read != nullptr);
    decoder_ptr = std::make_unique<CHBlockChunkCodec>(DM::toEmptyBlock(*columns_to_read));

    try
    {
        for (size_t index = 0; index < threads_num; ++index)
        {
            auto task = std::make_shared<std::packaged_task<void()>>([this, index] {
                try
                {
                    prepareLoop(index);
                }
                catch (...)
                {
                    auto ex = getCurrentExceptionMessage(true);
                    LOG_ERROR(Logger::get(), "PagePrepareThread#{} read loop meet exception and exited, ex={}", index, ex);
                }
            });
            persist_threads.emplace_back(task->get_future());

            RNPagePreparerPool::get().scheduleOrThrowOnError([task] { (*task)(); });
        }
    }
    catch (...)
    {
        tryLogCurrentException(exc_log, __PRETTY_FUNCTION__);
        throw;
    }
}

RNPagePreparer::~RNPagePreparer() noexcept
{
    for (auto & task : persist_threads)
    {
        try
        {
            task.get();
        }
        catch (...)
        {
            tryLogCurrentException(exc_log, __PRETTY_FUNCTION__);
        }
    }
}

void RNPagePreparer::prepareLoop(size_t idx)
{
    LoggerPtr log = exc_log->getChild(fmt::format("PagePrepareThread#{}", idx));


    bool meet_error = false;
    String local_err_msg;

    while (!meet_error)
    {
        try
        {
            // no more results
            if (!consumeOneResult(log))
                break;
        }
        catch (...)
        {
            meet_error = true;
            local_err_msg = getCurrentExceptionMessage(false);
        }
    }

    downloadDone(meet_error, local_err_msg, log);

    // try to do some preparation for speed up reading
    size_t num_prepared = 0;
    double seconds_cost = 0.0;
    if (do_prepare)
    {
        while (true)
        {
            auto seg_task = remote_read_tasks->nextTaskForPrepare();
            if (!seg_task)
                break;
            watch.restart();
            // do place index
            seg_task->prepare();
            seconds_cost = watch.elapsedSeconds();
            num_prepared += 1;
            LOG_DEBUG(log, "segment prepare done, segment_id={}", seg_task->segment_id);
            // update the state
            remote_read_tasks->updateTaskState(seg_task, DM::SegmentReadTaskState::DataReadyAndPrepared, false);
        }
    }

    remote_read_tasks->wakeAll();

    LOG_INFO(log, "Done preparation for {} segment tasks, cost={:.3f}s", num_prepared, seconds_cost);
}

void RNPagePreparer::downloadDone(bool meet_error, const String & local_err_msg, const LoggerPtr & log)
{
    Int32 copy_persister_num = -1;
    {
        std::unique_lock lock(mu);
        if (meet_error)
        {
            if (state == PageReceiverState::NORMAL)
                state = PageReceiverState::ERROR;
            if (err_msg.empty())
                err_msg = local_err_msg;
        }
        copy_persister_num = --live_persisters;
    }

    LOG_DEBUG(
        log,
        "persist end. meet error: {}{}, current alive persister: {}",
        meet_error,
        meet_error ? fmt::format(", err msg: {}", local_err_msg) : "",
        copy_persister_num);

    if (copy_persister_num < 0)
    {
        throw Exception("live_persisters should not be less than 0!");
    }
    else if (copy_persister_num == 0)
    {
        LOG_DEBUG(log, "All persist threads end in RNPageReceiver");
        String copy_persister_msg;
        {
            std::unique_lock lock(mu);
            copy_persister_msg = err_msg;
        }
        remote_read_tasks->allDataReceive(copy_persister_msg);
    }
}

bool RNPagePreparer::consumeOneResult(const LoggerPtr & log)
{
    auto result = receiver->nextResult(decoder_ptr);
    if (result.eof())
    {
        LOG_DEBUG(log, "fetch reader meets eof");
        return false;
    }
    if (!result.ok())
    {
        LOG_WARNING(log, "fetch reader meets error: {}", result.error_msg);
        throw Exception(result.error_msg);
    }

    const auto decode_detail = result.decode_detail;
    total_rows += decode_detail.rows;
    total_pages += decode_detail.pages;

    return true;
}

} // namespace DB
