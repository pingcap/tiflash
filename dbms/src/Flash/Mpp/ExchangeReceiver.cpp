#include <Flash/Mpp/ExchangeReceiver.h>

namespace pingcap
{
namespace kv
{

template <>
struct RpcTypeTraits<::mpp::EstablishMPPConnectionRequest>
{
    using RequestType = ::mpp::EstablishMPPConnectionRequest;
    using ResultType = ::mpp::MPPDataPacket;
    static std::unique_ptr<::grpc::ClientReader<::mpp::MPPDataPacket>> doRPCCall(
        grpc::ClientContext * context, std::shared_ptr<KvConnClient> client, const RequestType & req)
    {
        return client->stub->EstablishMPPConnection(context, req);
    }
    static std::unique_ptr<::grpc::ClientAsyncReader<::mpp::MPPDataPacket>> doAsyncRPCCall(grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client, const RequestType & req, grpc::CompletionQueue & cq, void * call)
    {
        return client->stub->AsyncEstablishMPPConnection(context, req, &cq, call);
    }
};

<<<<<<< HEAD
} // namespace kv
} // namespace pingcap
=======
template <typename RPCContext>
ExchangeReceiverBase<RPCContext>::ExchangeReceiverBase(
    std::shared_ptr<RPCContext> rpc_context_,
    size_t source_num_,
    size_t max_streams_,
    const String & req_id,
    const String & executor_id)
    : rpc_context(std::move(rpc_context_))
    , source_num(source_num_)
    , max_streams(max_streams_)
    , max_buffer_size(std::max<size_t>(batch_packet_count, std::max(source_num, max_streams_) * 2))
    , thread_manager(newThreadManager())
    , msg_channel(max_buffer_size)
    , live_connections(source_num)
    , state(ExchangeReceiverState::NORMAL)
    , exc_log(Logger::get("ExchangeReceiver", req_id, executor_id))
    , collected(false)
{
    try
    {
        rpc_context->fillSchema(schema);
        setUpConnection();
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
ExchangeReceiverBase<RPCContext>::~ExchangeReceiverBase()
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
void ExchangeReceiverBase<RPCContext>::cancel()
{
    setEndState(ExchangeReceiverState::CANCELED);
    msg_channel.finish();
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::close()
{
    setEndState(ExchangeReceiverState::CLOSED);
    msg_channel.finish();
}
>>>>>>> 4019600ea9 (fix some unsafe constructor and destructor (#4782))

namespace DB
{

void ExchangeReceiver::setUpConnection()
{
    for (int index = 0; index < pb_exchange_receiver.encoded_task_meta_size(); index++)
    {
        auto & meta = pb_exchange_receiver.encoded_task_meta(index);
        std::thread t(&ExchangeReceiver::ReadLoop, this, std::ref(meta), index);
        workers.push_back(std::move(t));
    }
}

void ExchangeReceiver::ReadLoop(const String & meta_raw, size_t source_index)
{
    bool meet_error = false;
    String local_err_msg;
    try
    {
        auto sender_task = new mpp::TaskMeta();
        sender_task->ParseFromString(meta_raw);
        auto req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
        req->set_allocated_receiver_meta(new mpp::TaskMeta(task_meta));
        req->set_allocated_sender_meta(sender_task);
        LOG_DEBUG(log, "begin start and read : " << req->DebugString());
        ::grpc::Status status = ::grpc::Status::OK;
        static const Int32 MAX_RETRY_TIMES = 10;
        for (int i = 0; i < MAX_RETRY_TIMES; i++)
        {
            pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call(req);
            grpc::ClientContext client_context;
            auto reader = cluster->rpc_client->sendStreamRequest(req->sender_meta().address(), &client_context, call);
            reader->WaitForInitialMetadata();
            mpp::MPPDataPacket packet;
            String req_info = "tunnel" + std::to_string(sender_task->task_id()) + "+" + std::to_string(task_meta.task_id());
            bool has_data = false;
            for (;;)
            {
                LOG_TRACE(log, "begin next ");
                bool success = reader->Read(&packet);
                if (!success)
                    break;
                else
                    has_data = true;
                if (packet.has_error())
                {
                    throw Exception("Exchange receiver meet error : " + packet.error().msg());
                }
                if (!decodePacket(packet, source_index, req_info))
                {
                    meet_error = true;
                    local_err_msg = "Decode packet meet error";
                    LOG_WARNING(log, "Decode packet meet error, exit from ReadLoop");
                    break;
                }
            }
            // if meet error, such as decode packect fails, it will not retry.
            if (meet_error)
            {
                break;
            }
            status = reader->Finish();
            if (status.ok())
            {
                LOG_DEBUG(log, "finish read : " << req->DebugString());
                break;
            }
            else
            {
                bool retriable = !has_data && i + 1 < MAX_RETRY_TIMES;
                LOG_WARNING(log,
                    "EstablishMPPConnectionRequest meets rpc fail for req " << req_info << ". Err code = " << status.error_code()
                                                                            << ", err msg = " << status.error_message()
                                                                            << ", retriable = " << retriable);
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
    std::lock_guard<std::mutex> lock(mu);
    live_connections--;
    if (meet_error && state == NORMAL)
        state = ERROR;
    if (meet_error && err_msg.empty())
        err_msg = local_err_msg;
    cv.notify_all();
    LOG_DEBUG(log, "read thread end!!! live connections: " << std::to_string(live_connections));
}

} // namespace DB
