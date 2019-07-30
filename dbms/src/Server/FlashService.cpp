#include <Server/FlashService.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>
#include <Core/Types.h>
#include <tipb/select.pb.h>
#include <Coprocessor/CoprocessorHandler.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/RegionException.h>

namespace DB
{
const Int64 REQ_TYPE_DAG = 103;
//const Int64 REQ_TYPE_ANALYZE = 104;
//const Int64 REQ_TYPE_CHECKSUM = 105;

FlashService::FlashService(const std::string & address_, IServer & server_)
    : server(server_),
      address(address_),
      log(&Logger::get("FlashService"))
{
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);

    // todo should set a reasonable value??
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);

    grpc_server = builder.BuildAndStart();

    LOG_INFO(log, "Flash service listening on [" << address << "]");
}

FlashService::~FlashService()
{
    // wait 5 seconds for pending rpcs to gracefully stop
    gpr_timespec deadline{5, 0, GPR_TIMESPAN};
    LOG_DEBUG(log, "Begin to shutting down grpc server");
    grpc_server->Shutdown(deadline);
    grpc_server->Wait();
}

String getClientMetaVar(grpc::ServerContext * grpc_context, String name, String default_val) {
    if(grpc_context->client_metadata().count(name) != 1) {
        return default_val;
    } else {
        return String(grpc_context->client_metadata().find(name)->second.data());
    }
}

::grpc::Status setClientInfo(grpc::ServerContext * grpc_context, Context & server_context) {
    auto client_meta = grpc_context->client_metadata();
    String query_id = getClientMetaVar(grpc_context, "query_id", "");
    server_context.setCurrentQueryId(query_id);
    ClientInfo & client_info = server_context.getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::GRPC;
    std::string peer = grpc_context->peer();
    Int64 pos = peer.find(':');
    if(pos == -1) {
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "invalid peer address");
    }
    std::string client_ip = peer.substr(pos+1);
    Poco::Net::SocketAddress client_address(client_ip);
    client_info.current_address = client_address;
    client_info.current_user = getClientMetaVar(grpc_context, "user", "");
    std::string records_per_chunk_str = getClientMetaVar(grpc_context, "records_per_chunk", "");
    if(!records_per_chunk_str.empty()) {
        server_context.setSetting("records_per_chunk", records_per_chunk_str);
    }
    std::string builder_version = getClientMetaVar(grpc_context, "builder_version", "v1");
    server_context.setSetting("coprocessor_plan_builder_version", builder_version);
    return ::grpc::Status::OK;
}

grpc::Status FlashService::Coprocessor(grpc::ServerContext * grpc_context, const coprocessor::Request * request,
        coprocessor::Response * response)
{
    LOG_DEBUG(log, "receive coprocessor request");
    LOG_DEBUG(log, request->DebugString());
    Context context = server.context();
    context.setGlobalContext(server.context());
    setClientInfo(grpc_context, context);
    if(request->tp() != REQ_TYPE_DAG) {
        LOG_ERROR(log, "Flash service Coprocessor other than dag request not implement yet");
        return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Only DAG request is supported");
    }
    try {
        CoprocessorContext cop_context(context, request->context(), *grpc_context);
        CoprocessorHandler coprocessorHandler(request, response, cop_context);
        if (coprocessorHandler.execute()) {
            LOG_DEBUG(log, "Flash service Coprocessor finished");
            return ::grpc::Status(::grpc::StatusCode::OK, "");
        } else {
            LOG_ERROR(log, "Flash service Coprocessor meet internal error");
            return ::grpc::Status(::grpc::StatusCode::INTERNAL, "");
        }
    } catch (LockException & e) {
        //todo set lock error info
        LOG_ERROR(log, "meet lock exception");
        // clear the data to avoid sending partial data
        response->set_data("");
    } catch (RegionException & e) {
        // todo set region error info
        LOG_ERROR(log, "meet region exception");
        response->set_data("");
    } catch (Exception & e) {
        // todo return exception message
        LOG_ERROR(log, "meet unknown exception, errmsg: " + e.message());
        response->set_data("");
    } catch (...) {
        LOG_ERROR(log, "meet unknown exception");
        response->set_data("");
    }
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, "");
}

} // namespace DB
