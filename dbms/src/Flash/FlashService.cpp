#include <Flash/FlashService.h>

#include <Core/Types.h>
#include <Flash/Coprocessor/CoprocessorHandler.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/RegionException.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

FlashService::FlashService(const std::string & address_, IServer & server_)
    : server(server_), address(address_), log(&Logger::get("FlashService"))
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

grpc::Status FlashService::Coprocessor(
    grpc::ServerContext * grpc_context, const coprocessor::Request * request, coprocessor::Response * response)
{
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling coprocessor request: " << request->DebugString());

    auto [context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        return status;
    }

    try
    {
        CoprocessorContext cop_context(context, request->context(), *grpc_context);
        CoprocessorHandler cop_handler(cop_context, request, response);

        cop_handler.execute();

        LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handle coprocessor request done");
        return ::grpc::Status(::grpc::StatusCode::OK, "");
    }
    catch (const LockException & e)
    {
        // TODO: handle lock error properly.
        LOG_ERROR(log, __PRETTY_FUNCTION__ << ": LockException: " << e.displayText());
        response->set_data("");
        return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, e.message());
    }
    catch (const RegionException & e)
    {
        // TODO: handle region error properly.
        LOG_ERROR(log, __PRETTY_FUNCTION__ << ": RegionException: " << e.displayText());
        response->set_data("");
        return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, e.message());
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, __PRETTY_FUNCTION__ << ": Exception: " << e.displayText());
        response->set_data("");

        if (e.code() == ErrorCodes::NOT_IMPLEMENTED)
            return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, e.message());

        // TODO: Map other DB error codes to grpc codes.

        return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, __PRETTY_FUNCTION__ << ": Exception: " << e.what());
        response->set_data("");
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
    }
}

String getClientMetaVarWithDefault(grpc::ServerContext * grpc_context, const String & name, const String & default_val)
{
    if (grpc_context->client_metadata().count(name) != 1)
        return default_val;
    else
        return String(grpc_context->client_metadata().find(name)->second.data());
}

std::tuple<Context, ::grpc::Status> FlashService::createDBContext(grpc::ServerContext * grpc_context)
{
    /// Create DB context.
    Context context = server.context();
    context.setGlobalContext(server.context());

    /// Set a bunch of client information.
    auto client_meta = grpc_context->client_metadata();
    String query_id = getClientMetaVarWithDefault(grpc_context, "query_id", "");
    context.setCurrentQueryId(query_id);
    ClientInfo & client_info = context.getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::GRPC;
    std::string peer = grpc_context->peer();
    Int64 pos = peer.find(':');
    if (pos == -1)
    {
        return std::make_tuple(context, ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid peer address: " + peer));
    }
    std::string client_ip = peer.substr(pos + 1);
    Poco::Net::SocketAddress client_address(client_ip);
    client_info.current_address = client_address;
    client_info.current_user = getClientMetaVarWithDefault(grpc_context, "user", "");

    /// Set DAG parameters.
    std::string dag_records_per_chunk_str = getClientMetaVarWithDefault(grpc_context, "dag_records_per_chunk", "");
    if (!dag_records_per_chunk_str.empty())
    {
        context.setSetting("dag_records_per_chunk", dag_records_per_chunk_str);
    }
    std::string planner = getClientMetaVarWithDefault(grpc_context, "dag_planner", "sql");
    context.setSetting("dag_planner", planner);

    return std::make_tuple(context, ::grpc::Status::OK);
}

} // namespace DB
