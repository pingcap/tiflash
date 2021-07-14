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

} // namespace kv
} // namespace pingcap

namespace DB
{

void ExchangeReceiver::setUpConnection()
{
    for (int index = 0; index < pb_exchange_receiver.encoded_task_meta_size(); index++)
    {
        auto & meta = pb_exchange_receiver.encoded_task_meta(index);
        std::thread t(&ExchangeReceiver::ReadLoop, this, std::ref(meta), index);
        live_connections++;
        workers.push_back(std::move(t));
    }
}

void ExchangeReceiver::ReadLoop(const String & meta_raw, size_t source_index)
{
    bool meet_error = false;
    try
    {
        auto sender_task = new mpp::TaskMeta();
        sender_task->ParseFromString(meta_raw);
        auto req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
        req->set_allocated_receiver_meta(new mpp::TaskMeta(task_meta));
        req->set_allocated_sender_meta(sender_task);
        LOG_DEBUG(log, "begin start and read : " << req->DebugString());
        pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call(req);
        grpc::ClientContext client_context;
        auto reader = cluster->rpc_client->sendStreamRequest(req->sender_meta().address(), &client_context, call);
        reader->WaitForInitialMetadata();
        // Block until the next result is available in the completion queue "cq".
        mpp::MPPDataPacket packet;
        String req_info = "tunnel" + std::to_string(sender_task->task_id()) + "+" + std::to_string(task_meta.task_id());
        for (;;)
        {
            LOG_TRACE(log, "begin next ");
            bool success = reader->Read(&packet);
            if (!success)
                break;
            if (packet.has_error())
            {
                throw Exception("exchange receiver meet error : " + packet.error().msg());
            }
            if (!decodePacket(packet, source_index, req_info))
            {
                meet_error = true;
                LOG_WARNING(log, "Decode packet meet error, exit from ReadLoop");
                break;
            }
        }
        LOG_DEBUG(log, "finish read : " << req->DebugString());
    }
    catch (Exception & e)
    {
        meet_error = true;
        err = e;
    }
    catch (std::exception & e)
    {
        meet_error = true;
        err = Exception(e.what());
    }
    catch (...)
    {
        meet_error = true;
        err = Exception("fatal error");
    }
    std::lock_guard<std::mutex> lock(mu);
    live_connections--;
    if (meet_error && state == NORMAL)
        state = ERROR;
    cv.notify_all();
    LOG_DEBUG(log, "read thread end!!! live connections: " << std::to_string(live_connections));
}

bool ExchangeReceiver::decodePacket(const mpp::MPPDataPacket & p, size_t source_index, const String & req_info)
{
    bool ret = true;
    std::shared_ptr<tipb::SelectResponse> resp_ptr = std::make_shared<tipb::SelectResponse>();
    if (!resp_ptr->ParseFromString(p.data()))
    {
        resp_ptr = nullptr;
        ret = false;
    }
    std::unique_lock<std::mutex> lock(mu);
    cv.wait(lock, [&] { return result_buffer.size() < max_buffer_size || state != NORMAL; });
    if (state == NORMAL)
    {
        if (resp_ptr != nullptr)
            result_buffer.emplace(resp_ptr, source_index, req_info);
        else
            result_buffer.emplace(resp_ptr, source_index, req_info, true, "Error while decoding MPPDataPacket");
    }
    else
    {
        ret = false;
    }
    cv.notify_all();
    return ret;
}

ExchangeReceiverResult ExchangeReceiver::nextResult()
{
    std::unique_lock<std::mutex> lk(mu);
    cv.wait(lk, [&] { return !result_buffer.empty() || live_connections == 0 || state != NORMAL; });
    ExchangeReceiverResult result;
    if (state != NORMAL)
    {
        String msg;
        if (state == CANCELED)
            msg = "query canceled";
        else if (state == CLOSED)
            msg = "ExchangeReceiver closed";
        else
            msg = err.message();
        result = {nullptr, 0, "ExchangeReceiver", true, msg, false};
    }
    else if (result_buffer.empty())
    {
        result = {nullptr, 0, "ExchangeReceiver", false, "", true};
    }
    else
    {
        result = result_buffer.front();
        result_buffer.pop();
    }
    cv.notify_all();
    return result;
}

} // namespace DB
