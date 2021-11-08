#include <Common/CPUAffinityManager.h>
#include <Common/ThreadFactory.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <fmt/core.h>

namespace DB
{
template <typename RPCContext>
ExchangeReceiverBase<RPCContext>::ExchangeReceiverBase(
    std::shared_ptr<RPCContext> rpc_context_,
    const ::tipb::ExchangeReceiver & exc,
    const ::mpp::TaskMeta & meta,
    size_t max_streams_,
    const std::shared_ptr<LogWithPrefix> & log_)
    : rpc_context(std::move(rpc_context_))
    , pb_exchange_receiver(exc)
    , source_num(pb_exchange_receiver.encoded_task_meta_size())
    , task_meta(meta)
    , max_streams(max_streams_)
    , max_buffer_size(max_streams_ * 2)
    , res_buffer(max_buffer_size)
    , live_connections(pb_exchange_receiver.encoded_task_meta_size())
    , state(ExchangeReceiverState::NORMAL)
    , log(getMPPTaskLog(log_, "ExchangeReceiver"))
{
    for (int i = 0; i < exc.field_types_size(); i++)
    {
        String name = "exchange_receiver_" + std::to_string(i);
        ColumnInfo info = TiDB::fieldTypeToColumnInfo(exc.field_types(i));
        schema.push_back(std::make_pair(name, info));
    }

    setUpConnection();
}

template <typename RPCContext>
ExchangeReceiverBase<RPCContext>::~ExchangeReceiverBase()
{
    {
        std::unique_lock<std::mutex> lk(mu);
        state = ExchangeReceiverState::CLOSED;
        cv.notify_all();
    }

    for (auto & worker : workers)
    {
        worker.join();
    }
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::cancel()
{
    std::unique_lock<std::mutex> lk(mu);
    state = ExchangeReceiverState::CANCELED;
    cv.notify_all();
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::setUpConnection()
{
    for (size_t index = 0; index < source_num; ++index)
    {
        auto t = ThreadFactory(true, "Receiver").newThread(&ExchangeReceiverBase<RPCContext>::readLoop, this, index);
        workers.push_back(std::move(t));
    }
}

static inline String getReceiverStateStr(const ExchangeReceiverState & s)
{
    switch (s)
    {
    case ExchangeReceiverState::NORMAL:
        return "NORMAL";
    case ExchangeReceiverState::ERROR:
        return "ERROR";
    case ExchangeReceiverState::CANCELED:
        return "CANCELED";
    case ExchangeReceiverState::CLOSED:
        return "CLOSED";
    default:
        return "UNKNOWN";
    }
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::readLoop(size_t source_index)
{
    CPUAffinityManager::getInstance().bindSelfQueryThread();
    bool meet_error = false;
    String local_err_msg;

    Int64 send_task_id = -1;
    Int64 recv_task_id = task_meta.task_id();

    try
    {
        auto req = rpc_context->makeRequest(source_index, pb_exchange_receiver, task_meta);
        send_task_id = req.send_task_id;
        String req_info = "tunnel" + std::to_string(send_task_id) + "+" + std::to_string(recv_task_id);
        LOG_DEBUG(log, "begin start and read : " << req.debugString());
        auto status = RPCContext::getStatusOK();
        for (int i = 0; i < 10; i++)
        {
            auto reader = rpc_context->makeReader(req);
            reader->initialize();
            std::shared_ptr<ReceivedMessage> recv_msg;
            bool has_data = false;
            for (;;)
            {
                LOG_TRACE(log, "begin next ");
                {
                    std::unique_lock<std::mutex> lock(mu);
                    cv.wait(lock, [&] { return res_buffer.hasEmpty() || state != ExchangeReceiverState::NORMAL; });
                    if (state == ExchangeReceiverState::NORMAL)
                    {
                        res_buffer.popEmpty(recv_msg);
                        cv.notify_all();
                    }
                    else
                    {
                        meet_error = true;
                        local_err_msg = "receiver's state is " + getReceiverStateStr(state) + ", exit from readLoop";
                        LOG_WARNING(log, local_err_msg);
                        break;
                    }
                }
                recv_msg->req_info = req_info;
                recv_msg->source_index = source_index;
                bool success = reader->read(recv_msg->packet.get());
                if (!success)
                    break;
                else
                    has_data = true;
                if (recv_msg->packet->has_error())
                {
                    throw Exception("Exchange receiver meet error : " + recv_msg->packet->error().msg());
                }
                {
                    std::unique_lock<std::mutex> lock(mu);
                    cv.wait(lock, [&] { return res_buffer.canPush() || state != ExchangeReceiverState::NORMAL; });
                    if (state == ExchangeReceiverState::NORMAL)
                    {
                        res_buffer.pushObject(recv_msg);
                        cv.notify_all();
                    }
                    else
                    {
                        meet_error = true;
                        local_err_msg = "receiver's state is " + getReceiverStateStr(state) + ", exit from readLoop";
                        LOG_WARNING(log, local_err_msg);
                        break;
                    }
                }
            }
            // if meet error, such as decode packect fails, it will not retry.
            if (meet_error)
            {
                break;
            }
            status = reader->finish();
            if (status.ok())
            {
                LOG_DEBUG(log, "finish read : " << req.debugString());
                break;
            }
            else
            {
                LOG_WARNING(
                    log,
                    "EstablishMPPConnectionRequest meets rpc fail. Err msg is: " << status.error_message() << " req info " << req_info);
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
    }
    catch (Exception & e)
    {
        meet_error = true;
        local_err_msg = e.message();
    }
    catch (std::exception & e)
    {
        meet_error = true;
        local_err_msg = e.what();
    }
    catch (...)
    {
        meet_error = true;
        local_err_msg = "fatal error";
    }
    Int32 copy_live_conn = -1;
    {
        std::unique_lock<std::mutex> lock(mu);
        live_connections--;
        if (meet_error && state == ExchangeReceiverState::NORMAL)
            state = ExchangeReceiverState::ERROR;
        if (meet_error && err_msg.empty())
            err_msg = local_err_msg;
        copy_live_conn = live_connections;
        cv.notify_all();
    }
    LOG_DEBUG(log, fmt::format("{} -> {} end! current alive connections: {}", send_task_id, recv_task_id, copy_live_conn));

    if (copy_live_conn == 0)
        LOG_DEBUG(log, fmt::format("All threads end in ExchangeReceiver"));
    else if (copy_live_conn < 0)
        throw Exception("live_connections should not be less than 0!");
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::returnEmptyMsg(std::shared_ptr<ReceivedMessage> & recv_msg)
{
    recv_msg->packet->Clear();
    std::unique_lock<std::mutex> lock(mu);
    cv.wait(lock, [&] { return res_buffer.canPushEmpty(); });
    res_buffer.pushEmpty(std::move(recv_msg));
    cv.notify_all();
}

template <typename RPCContext>
Int64 ExchangeReceiverBase<RPCContext>::decodeChunks(std::shared_ptr<ReceivedMessage> & recv_msg, std::queue<Block> & block_queue, const DataTypes & expected_types)
{
    assert(recv_msg != nullptr);
    Int64 rows = 0;

    int chunk_size = recv_msg->packet->chunks_size();
    if (chunk_size == 0)
        return rows;

    /// ExchangeReceiverBase should receive chunks of TypeCHBlock
    for (int i = 0; i < chunk_size; i++)
    {
        Block block = CHBlockChunkCodec().decode(recv_msg->packet->chunks(i), schema);
        rows += block.rows();
        if (unlikely(block.rows() == 0))
            continue;
        assertBlockSchema(expected_types, block, "ExchangeReceiver decodes chunks");
        block_queue.push(std::move(block));
    }
    return rows;
}

template <typename RPCContext>
ExchangeReceiverResult ExchangeReceiverBase<RPCContext>::nextResult(std::queue<Block> & block_queue, const DataTypes & expected_types)
{
    std::shared_ptr<ReceivedMessage> recv_msg;
    {
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock, [&] { return res_buffer.hasObjects() || live_connections == 0 || state != ExchangeReceiverState::NORMAL; });

        if (state != ExchangeReceiverState::NORMAL)
        {
            String msg;
            if (state == ExchangeReceiverState::CANCELED)
                msg = "query canceled";
            else if (state == ExchangeReceiverState::CLOSED)
                msg = "ExchangeReceiver closed";
            else if (!err_msg.empty())
                msg = err_msg;
            else
                msg = "Unknown error";
            return {nullptr, 0, "ExchangeReceiver", true, msg, false};
        }
        else if (res_buffer.hasObjects())
        {
            res_buffer.popObject(recv_msg);
            cv.notify_all();
        }
        else /// live_connections == 0, res_buffer is empty, and state is NORMAL, that is the end.
        {
            return {nullptr, 0, "ExchangeReceiver", false, "", true};
        }
    }
    assert(recv_msg != nullptr && recv_msg->packet != nullptr);
    ExchangeReceiverResult result;
    if (recv_msg->packet->has_error())
    {
        result = {nullptr, recv_msg->source_index, recv_msg->req_info, true, recv_msg->packet->error().msg(), false};
    }
    else
    {
        if (!recv_msg->packet->data().empty()) /// the data of the last packet is serialized from tipb::SelectResponse including execution summaries.
        {
            auto resp_ptr = std::make_shared<tipb::SelectResponse>();
            if (!resp_ptr->ParseFromString(recv_msg->packet->data()))
            {
                result = {nullptr, recv_msg->source_index, recv_msg->req_info, true, "decode error", false};
            }
            else
            {
                result = {resp_ptr, recv_msg->source_index, recv_msg->req_info, false, "", false};
                /// If mocking TiFlash as TiDB, here should decode chunks from resp_ptr.
                if (!resp_ptr->chunks().empty())
                {
                    assert(recv_msg->packet->chunks().empty());
                    result.rows = CoprocessorReader::decodeChunks(resp_ptr, block_queue, expected_types, schema);
                }
            }
        }
        else /// the non-last packets
        {
            result = {nullptr, recv_msg->source_index, recv_msg->req_info, false, "", false};
        }
        if (!result.meet_error && !recv_msg->packet->chunks().empty())
        {
            assert(result.rows == 0);
            result.rows = decodeChunks(recv_msg, block_queue, expected_types);
        }
    }
    returnEmptyMsg(recv_msg);
    return result;
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class ExchangeReceiverBase<GRPCReceiverContext>;

} // namespace DB
