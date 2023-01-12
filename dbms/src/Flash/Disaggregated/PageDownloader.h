#pragma once

#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Disaggregated/PageReceiver.h>

namespace DB
{

class PageDownloader
{
public:
    PageDownloader(
        DM::RemoteReadTaskPtr remote_read_tasks_,
        std::shared_ptr<PageReceiver> receiver_,
        const DM::ColumnDefinesPtr & columns_to_read,
        size_t max_streams_,
        const String & req_id,
        const String & executor_id,
        bool do_prepare_);

    ~PageDownloader();

private:
    void persistLoop(size_t idx);

    bool consumeOneResult(const LoggerPtr & log);

    void downloadDone(bool meet_error, const String & local_err_msg, const LoggerPtr & log);

private:
    const size_t threads_num;
    const bool do_prepare;
    DM::RemoteReadTaskPtr remote_read_tasks;
    std::shared_ptr<PageReceiver> receiver;
    std::vector<std::future<void>> persist_threads;

    std::unique_ptr<CHBlockChunkCodec> decoder_ptr;

    std::mutex mu;
    Int32 live_persisters;
    PageReceiverState state;
    String err_msg;

    std::atomic<size_t> total_rows;
    std::atomic<size_t> total_pages;

    Stopwatch watch;
    LoggerPtr exc_log;
};

} // namespace DB
