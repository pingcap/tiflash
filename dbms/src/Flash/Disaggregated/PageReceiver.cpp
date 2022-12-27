#include <Common/CPUAffinityManager.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Common/ThreadManager.h>
#include <Flash/Disaggregated/GRPCPageReceiverContext.h>
#include <Flash/Disaggregated/PageReceiver.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/File/dtpb/column_file.pb.h>
#include <Storages/DeltaMerge/Remote/RemoteReadTask.h>
#include <common/logger_useful.h>

#include <magic_enum.hpp>
#include <memory>
#include <mutex>

namespace DB
{
namespace details
{
String constructStatusString(ExchangeReceiverState state, const String & error_message)
{
    if (error_message.empty())
        return fmt::format("Receiver state: {}", magic_enum::enum_name(state));
    return fmt::format("Receiver state: {}, error message: {}", magic_enum::enum_name(state), error_message);
}
bool pushPacket(const DM::RemoteSegmentReadTaskPtr & seg_task,
                const String & req_info,
                const TrackedPageDataPacketPtr & tracked_packet,
                std::unique_ptr<MPMCQueue<PageReceivedMessagePtr>> & msg_channel,
                LoggerPtr & log)
{
    bool push_succeed = true;

    const mpp::Error * error_ptr = nullptr;
    auto & packet = *tracked_packet;
    if (packet.has_error())
        error_ptr = &packet.error();

    // assert(!msg_channels.empty());

    if (!(error_ptr == nullptr // no error
          && packet.chunks_size() == 0 && packet.pages_size() == 0 // empty
          ))
    {
        auto recv_msg = std::make_shared<PageReceivedMessage>(
            req_info,
            seg_task,
            tracked_packet,
            error_ptr);

        push_succeed = msg_channel->push(std::move(recv_msg)) == MPMCQueueResult::OK;
        if (push_succeed)
            seg_task->addPendingMsg();
    }

    LOG_TRACE(log, "push recv_msg to msg_channels(size: {}) succeed:{}", 1, push_succeed);
    return push_succeed;
}
} // namespace details

template <typename RPCContext>
PageReceiverBase<RPCContext>::PageReceiverBase(
    std::unique_ptr<RPCContext> rpc_context_,
    size_t source_num_,
    size_t max_streams_,
    const String & req_id,
    const String & executor_id)
    : rpc_context(std::move(rpc_context_))
    , source_num(source_num_)
    , max_buffer_size(std::max<size_t>(16, max_streams_ * 2))
    , persist_threads_num(max_streams_)
    , thread_manager(newThreadManager())
    , live_connections(source_num)
    , state(ExchangeReceiverState::NORMAL)
    , live_persisters(persist_threads_num)
    , collected(false)
    , thread_count(0)
    , exc_log(Logger::get(req_id, executor_id))
    , total_rows(0)
    , total_pages(0)
{
    try
    {
        // msg_channels.push_back(std::make_unique<MPMCQueue<PageReceivedMessagePtr>>(max_buffer_size));
        msg_channel = std::make_unique<MPMCQueue<PageReceivedMessagePtr>>(max_buffer_size);
        // setup fetch threads to fetch pages/blocks from write nodes
        setUpConnection();
        // setup persist threads to pop msg (pages/blocks) and write down
        setUpPersist();
    }
    catch (...)
    {
        try
        {
            cancel();
            thread_manager->wait();
        }
        catch (...)
        {
            tryLogCurrentException(exc_log, __PRETTY_FUNCTION__);
        }
        throw;
    }
}

template <typename RPCContext>
PageReceiverBase<RPCContext>::~PageReceivedMessage()
{
    try
    {
        close();
        thread_manager->wait();
    }
    catch (...)
    {
        tryLogCurrentException(exc_log, __PRETTY_FUNCTION__);
    }
}

template <typename RPCContext>
void PageReceiverBase<RPCContext>::cancel()
{
    if (setEndState(ExchangeReceiverState::CANCELED))
    {
        rpc_context->cancelMPPTaskOnTiFlashStorageNode(exc_log);
    }
    cancelAllMsgChannels();
}

template <typename RPCContext>
void PageReceiverBase<RPCContext>::close()
{
    setEndState(ExchangeReceiverState::CLOSED);
    finishAllMsgChannels();
}

template <typename RPCContext>
void PageReceiverBase<RPCContext>::setUpConnection()
{
    // TODO: support async
    for (size_t index = 0; index < source_num; ++index)
    {
        thread_manager->schedule(true, "Receiver", [this] {
            readLoop();
        });
        ++thread_count;
    }
}

template <typename RPCContext>
void PageReceiverBase<RPCContext>::setUpPersist()
{
    for (size_t index = 0; index < persist_threads_num; ++index)
    {
        thread_manager->schedule(true, "Persister", [this, index] {
            persistLoop(index);
        });
        ++thread_count;
    }
}

template <typename RPCContext>
void PageReceiverBase<RPCContext>::persistLoop(size_t idx)
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

    Int32 copy_persister_num = -1;
    {
        std::unique_lock lock(mu);
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
        rpc_context->finishAllReceivingTasks();
    }
}

template <typename RPCContext>
bool PageReceiverBase<RPCContext>::consumeOneResult(const LoggerPtr & log)
{
    // TODO: this does not make sense. maybe we need another class for
    // saving the pages/block
    DM::ColumnDefines columns_to_read;
    Block sample_block = DM::toEmptyBlock(columns_to_read);

    auto decoder_ptr = std::make_unique<CHBlockChunkCodec>(sample_block);

    auto result = nextResult(0, decoder_ptr);
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

template <typename RPCContext>
PageReceiverResult PageReceiverBase<RPCContext>::nextResult(
    size_t /*stream_id*/,
    std::unique_ptr<CHBlockChunkCodec> & decoder_ptr)
{
#if 0
    if (unlikely(stream_id >= msg_channels.size()))
    {
        LOG_ERROR(exc_log, "stream_id out of range, stream_id: {}, total_stream_count: {}", stream_id, msg_channels.size());
        return PageReceiverResult::newError("", "stream_id out of range");
    }
#endif

    // TODO: decode_ptr can not squash blocks, the blocks could comes
    // from different stores, tables, segments

    std::shared_ptr<PageReceivedMessage> recv_msg;
    // auto pop_res = msg_channels[stream_id]->pop(recv_msg);
    auto pop_res = msg_channel->pop(recv_msg);
    if (pop_res != MPMCQueueResult::OK)
    {
        std::unique_lock lock(mu);
        if (state != DB::ExchangeReceiverState::NORMAL)
        {
            return PageReceiverResult::newError(PageReceiverBase<RPCContext>::name, details::constructStatusString(state, err_msg));
        }

        /// live_connections == 0, msg_channel is finished, and state is NORMAL,
        /// that is the end.
        return PageReceiverResult::newEOF(PageReceiverBase<RPCContext>::name);
    }

    assert(recv_msg != nullptr);
    if (unlikely(recv_msg->error_ptr != nullptr))
        return PageReceiverResult::newError(recv_msg->req_info, recv_msg->error_ptr->msg());

    // Decode the pages or blocks into recv_msg->seg_task
    return toDecodeResult(recv_msg, decoder_ptr);
}

template <typename RPCContext>
PageReceiverResult PageReceiverBase<RPCContext>::toDecodeResult(
    const std::shared_ptr<PageReceivedMessage> & recv_msg,
    std::unique_ptr<CHBlockChunkCodec> & decoder_ptr)
{
    assert(recv_msg != nullptr);
    /// the data packets (now we ignore execution summary)
    auto result = PageReceiverResult::newOk(recv_msg->req_info);
    result.decode_detail = decodeChunks(recv_msg, decoder_ptr);
    auto has_pending_msg = recv_msg->seg_task->addConsumedMsg();
    LOG_DEBUG(exc_log,
              "seg: {} state: {} received: {} pending: {}",
              recv_msg->seg_task->segment_id,
              magic_enum::enum_name(recv_msg->seg_task->state),
              recv_msg->seg_task->num_msg_consumed,
              recv_msg->seg_task->num_msg_to_consume);
    if (recv_msg->seg_task->state == DM::SegmentReadTaskState::Receiving
        && !has_pending_msg)
    {
        rpc_context->finishTaskReceive(recv_msg->seg_task);
    }
    return result;
}

template <typename RPCContext>
PageDecodeDetail PageReceiverBase<RPCContext>::decodeChunks(
    const std::shared_ptr<PageReceivedMessage> & recv_msg,
    std::unique_ptr<CHBlockChunkCodec> & decoder_ptr)
{
    assert(recv_msg != nullptr);
    PageDecodeDetail detail;

    LOG_DEBUG(exc_log, "decoding msg with {} chunks, {} pages", recv_msg->chunks().size(), recv_msg->pages().size());

    if (recv_msg->empty())
        return detail;

    {
        // Record total packet size
        const auto & packet = *recv_msg->packet;
        detail.packet_bytes = packet.ByteSizeLong();
    }

    // Note: Currently in the 2nd response memtable data is not contained.
    for (const String & chunk : recv_msg->chunks())
    {
        auto block = decoder_ptr->decode(chunk);
        if (!block)
            continue;
        detail.rows += block.rows();
        if likely (block.rows() > 0)
        {
            recv_msg->seg_task->receiveMemTable(std::move(block));
        }
    }

    // TODO: Parse the chunks into pages and push to queue
    for (const String & page : recv_msg->pages())
    {
        dtpb::RemotePage remote_page;
        bool parsed = remote_page.ParseFromString(page); // TODO: handle error
        RUNTIME_CHECK_MSG(parsed, "Can not parse remote page");
        recv_msg->seg_task->receivePage(std::move(remote_page));
        detail.pages += 1;
    }

    return detail;
}

constexpr Int32 max_retry_times = 10;

template <typename RPCContext>
void PageReceiverBase<RPCContext>::readLoop()
{
    // TODO: metrics

    CPUAffinityManager::getInstance().bindSelfQueryThread();
    bool meet_error = false;
    String local_err_msg;

    // Keep poping segment fetch pages request to get the task ready
    while (!meet_error)
    {
        auto req = rpc_context->popRequest();
        if (!req.isValid())
            break;
        try
        {
            std::tie(meet_error, local_err_msg) = taskReadLoop(req);
        }
        catch (...)
        {
            meet_error = true;
            local_err_msg = getCurrentExceptionMessage(false);
        }
        rpc_context->finishTaskEstablish(req, meet_error);
    }
    connectionDone(meet_error, local_err_msg, exc_log);
}

template <typename RPCContext>
std::tuple<bool, String> PageReceiverBase<RPCContext>::taskReadLoop(const Request & req)
{
    auto status = RPCContext::getStatusOK();
    bool meet_error = false;
    String local_err_msg;

    String req_info = fmt::format("tunnel{}", req.identifier());
    LoggerPtr log = exc_log->getChild(req_info);
    for (int i = 0; i < max_retry_times; ++i)
    {
        auto reader = rpc_context->makeReader(req);
        bool has_data = false;
        // Keep reading packets and push the packet
        // into msg_channels
        for (;;)
        {
            LOG_TRACE(log, "begin next ");
            TrackedPageDataPacketPtr packet = std::make_shared<PageDataPacket>();
            if (bool success = reader->read(packet); !success)
                break;
            has_data = true;
            if (packet->has_error())
            {
                meet_error = true;
                local_err_msg = fmt::format("Read error message from page packet: {}", packet->error().msg());
                break;
            }

            if (!details::pushPacket(
                    req.seg_task,
                    req_info,
                    packet,
                    msg_channel,
                    log))
            {
                meet_error = true;
                local_err_msg = fmt::format("Push page packet failed. {}", getStatusString());
                break;
            }
            // LOG_DEBUG(log,
            //           "push packet into channel, chunks: {} pages: {}, msg_size: {}",
            //           packet->chunks_size(),
            //           packet->pages_size(),
            //           msg_channel->size());
        }
        // if meet error, such as decode packet fails, it will not retry.
        if (meet_error)
        {
            reader->cancel(local_err_msg);
            break;
        }
        status = reader->finish();
        if (status.ok())
        {
            // LOG_DEBUG(log, "finish read: {}", req.debugString());
            break;
        }
        else
        {
            bool retriable = !has_data && i + 1 < max_retry_times;
            LOG_WARNING(
                log,
                "FetchDisaggregatedPagesRequest meets rpc fail. Err code = {}, err msg = {}, retriable = {}",
                status.error_code(),
                status.error_message(),
                retriable);
            // if we have received some data, we should not retry.
            if (has_data)
                break;

            using namespace std::chrono_literals;
            std::this_thread::sleep_for(1s);
        }
    }
    if (!status.ok())
    {
        meet_error = true;
        local_err_msg = status.error_message();
    }
    return {meet_error, local_err_msg};
}

template <typename RPCContext>
bool PageReceiverBase<RPCContext>::setEndState(ExchangeReceiverState new_state)
{
    assert(new_state == ExchangeReceiverState::CANCELED || new_state == ExchangeReceiverState::CLOSED);
    std::unique_lock lock(mu);
    if (state == ExchangeReceiverState::CANCELED || state == ExchangeReceiverState::CLOSED)
    {
        return false;
    }
    state = new_state;
    return true;
}

template <typename RPCContext>
String PageReceiverBase<RPCContext>::getStatusString()
{
    std::unique_lock lock(mu);
    return details::constructStatusString(state, err_msg);
}

template <typename RPCContext>
void PageReceiverBase<RPCContext>::connectionDone(
    bool meet_error,
    const String & local_err_msg,
    const LoggerPtr & log)
{
    Int32 copy_live_conn = -1;
    {
        std::unique_lock lock(mu);
        if (meet_error)
        {
            if (state == ExchangeReceiverState::NORMAL)
                state = ExchangeReceiverState::ERROR;
            if (err_msg.empty())
                err_msg = local_err_msg;
        }
        copy_live_conn = --live_connections;
    }
    LOG_DEBUG(
        log,
        "connection end. meet error: {}{}, current alive connections: {}",
        meet_error,
        meet_error ? fmt::format(", err msg: {}", local_err_msg) : "",
        copy_live_conn);

    if (copy_live_conn == 0)
    {
        LOG_DEBUG(log, "All read threads end in PageReceiver");
    }
    else if (copy_live_conn < 0)
        throw Exception("live_connections should not be less than 0!");

    if (meet_error || copy_live_conn == 0)
        finishAllMsgChannels();
}

template <typename RPCContext>
void PageReceiverBase<RPCContext>::finishAllMsgChannels()
{
    // for (auto & msg_channel : msg_channels)
    msg_channel->finish();
}

template <typename RPCContext>
void PageReceiverBase<RPCContext>::cancelAllMsgChannels()
{
    // for (auto & msg_channel : msg_channels)
    msg_channel->cancel();
}

/// Explicit template instantiations
template class PageReceiverBase<GRPCPagesReceiverContext>;

} // namespace DB
