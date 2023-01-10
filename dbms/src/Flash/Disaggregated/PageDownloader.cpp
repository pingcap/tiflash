#include <Common/Exception.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Disaggregated/PageDownloader.h>
#include <Flash/Disaggregated/PageReceiver.h>
#include <IO/IOThreadPool.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Remote/RemoteReadTask.h>
#include <common/logger_useful.h>

#include <future>

namespace DB
{

PageDownloader::PageDownloader(
    DM::RemoteReadTaskPtr remote_read_tasks_,
    std::shared_ptr<PageReceiver> receiver_,
    const DM::ColumnDefinesPtr & columns_to_read,
    size_t max_streams_,
    const String & req_id,
    const String & executor_id)
    : threads_num(max_streams_)
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
                persistLoop(index);
            });
            persist_threads.emplace_back(task->get_future());

            IOThreadPool::get().scheduleOrThrowOnError([task] { (*task)(); });
        }
    }
    catch (...)
    {
        tryLogCurrentException(exc_log, __PRETTY_FUNCTION__);
        throw;
    }
}

PageDownloader::~PageDownloader()
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

void PageDownloader::persistLoop(size_t idx)
{
    LoggerPtr log = exc_log->getChild(fmt::format("persist{}", idx));


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
    while (true)
    {
        auto seg_task = remote_read_tasks->nextTaskForPrepare();
        if (!seg_task)
            break;
        // do place index
        seg_task->prepare();
        num_prepared += 1;
        // update the state
        remote_read_tasks->updateTaskState(seg_task, DM::SegmentReadTaskState::DataReadyAndPrepared, false);
    }
    LOG_INFO(log, "Done preparation for {} segment tasks", num_prepared);
}

void PageDownloader::downloadDone(bool meet_error, const String & local_err_msg, const LoggerPtr & log)
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
        LOG_DEBUG(log, "All persist threads end in PageReceiver");
        String copy_persister_msg;
        {
            std::unique_lock lock(mu);
            copy_persister_msg = err_msg;
        }
        remote_read_tasks->allDataReceive(copy_persister_msg);

        GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_fetch_page).Observe(watch.elapsedSeconds());
    }
}

bool PageDownloader::consumeOneResult(const LoggerPtr & log)
{
    auto result = receiver->nextResult(0, decoder_ptr);
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
