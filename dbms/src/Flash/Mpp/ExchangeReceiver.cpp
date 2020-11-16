#include "ExchangeReceiver.h"

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
// Each call owns contexts to proceed asynchronous requests
class ExchangeReceiver::ExchangeCall
{
public:
    using RequestType = ::mpp::EstablishMPPConnectionRequest;
    using ResultType = ::mpp::MPPDataPacket;
    grpc::ClientContext client_context;
    std::shared_ptr<RequestType> req;
    mpp::MPPDataPacket packet;
    std::unique_ptr<::grpc::ClientAsyncReader<::mpp::MPPDataPacket>> reader;
    enum StateType
    {
        CONNECTED,
        TOREAD,
        DONE
    };
    StateType state_type;
    ExchangeCall(TMTContext & tmtContext, std::string meta_str, ::mpp::TaskMeta & task_meta, grpc::CompletionQueue & cq)
    {
        auto sender_task = new mpp::TaskMeta();
        sender_task->ParseFromString(meta_str);
        req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
        req->set_allocated_receiver_meta(new mpp::TaskMeta(task_meta));
        req->set_allocated_sender_meta(sender_task);
        pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call(req);
        reader = tmtContext.getCluster()->rpc_client->sendStreamRequestAsync(
            req->sender_meta().address(), &client_context, call, cq, (void *)this);
        state_type = CONNECTED;
    }
};

void ExchangeReceiver::sendAsyncReq()
{
    for (auto & meta : pb_exchange_receiver.encoded_task_meta())
    {
        live_connections++;
        auto ptr = std::make_shared<ExchangeCall>(context, meta, task_meta, grpc_com_queue);
        exchangeCalls.emplace_back(ptr);
        LOG_DEBUG(log, "begin to async read : " << ptr << " " << ptr->req->DebugString());
    }
}
void ExchangeReceiver::proceedAsyncReq()
{
    try
    {
        void * got_tag;
        bool ok = false;

        // Block until the next result is available in the completion queue "cq".
        while (live_connections > 0 && grpc_com_queue.Next(&got_tag, &ok))
        {
            ExchangeCall * call = static_cast<ExchangeCall *>(got_tag);
            if (!ok)
            {
                call->state_type = ExchangeCall::DONE;
            }
            switch (call->state_type)
            {
                case ExchangeCall::StateType::CONNECTED:
                {
                    call->reader->Read(&call->packet, (void *)call);
                    call->state_type = ExchangeCall::StateType::TOREAD;
                }
                break;
                case ExchangeCall::StateType::TOREAD:
                {
                    // the last read() asynchronously succeed!
                    if (call->packet.has_error()) // This is the only way that down stream pass an error.
                    {
                        throw TiFlashException("exchange receiver meet error : " + call->packet.error().msg(), Errors::MPP::Internal);
                    }
                    decodePacket(call->packet);
                    // issue a new read request
                    call->reader->Read(&call->packet, (void *)call);
                }
                break;
                case ExchangeCall::StateType::DONE:
                {
                    live_connections--;
                }
                break;
                default:
                {
                    throw TiFlashException("exchange receiver meet unknown msg", Errors::MPP::Internal);
                }
            }
        }
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
    live_connections = 0;
    cv.notify_all();
    LOG_DEBUG(log, "async thread end!!!");
}

} // namespace DB