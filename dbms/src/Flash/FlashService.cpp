#include <Flash/FlashService.h>

#include <Core/Types.h>
#include <Flash/BatchCommandsHandler.h>
#include <Flash/CoprocessorHandler.h>
#include <Raft/RaftService.h>
#include <Server/IServer.h>
#include <grpcpp/server_builder.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

FlashService::FlashService(IServer & server_) : server(server_), log(&Logger::get("FlashService")) {}

grpc::Status FlashService::Coprocessor(
    grpc::ServerContext * grpc_context, const coprocessor::Request * request, coprocessor::Response * response)
{
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling coprocessor request: " << request->DebugString());

    auto [context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        return status;
    }

    CoprocessorContext cop_context(context, request->context(), *grpc_context);
    CoprocessorHandler cop_handler(cop_context, request, response);

    auto ret = cop_handler.execute();

    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handle coprocessor request done: " << ret.error_code() << ", " << ret.error_message());
    return ret;
}

grpc::Status FlashService::BatchCommands(
    grpc::ServerContext * grpc_context, grpc::ServerReaderWriter<::tikvpb::BatchCommandsResponse, tikvpb::BatchCommandsRequest> * stream)
{
    auto [context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        return status;
    }

    tikvpb::BatchCommandsRequest request;
    while (stream->Read(&request))
    {
        LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling batch commands: " << request.DebugString());

        tikvpb::BatchCommandsResponse response;
        BatchCommandsContext batch_commands_context(
            context, [this](const grpc::ServerContext * grpc_server_context) { return createDBContext(grpc_server_context); },
            *grpc_context);
        BatchCommandsHandler batch_commands_handler(batch_commands_context, request, response);
        auto ret = batch_commands_handler.execute();
        if (!ret.ok())
        {
            LOG_DEBUG(
                log, __PRETTY_FUNCTION__ << ": Handle batch commands request done: " << ret.error_code() << ", " << ret.error_message());
            return ret;
        }

        if (!stream->Write(response))
        {
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Write response failed for unknown reason.");
            return grpc::Status(grpc::StatusCode::UNKNOWN, "Write response failed for unknown reason.");
        }

        LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handle batch commands request done: " << ret.error_code() << ", " << ret.error_message());
    }

    return grpc::Status::OK;
}

String getClientMetaVarWithDefault(const grpc::ServerContext * grpc_context, const String & name, const String & default_val)
{
    if (auto it = grpc_context->client_metadata().find(name); it != grpc_context->client_metadata().end())
        return it->second.data();
    return default_val;
}

std::tuple<Context, grpc::Status> FlashService::createDBContext(const grpc::ServerContext * grpc_context) const
{
    /// Create DB context.
    Context context = server.context();
    context.setGlobalContext(server.context());

    /// Set a bunch of client information.
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
    std::string planner = getClientMetaVarWithDefault(grpc_context, "dag_planner", "optree");
    context.setSetting("dag_planner", planner);
    std::string expr_field_type_check = getClientMetaVarWithDefault(grpc_context, "dag_expr_field_type_strict_check", "1");
    context.setSetting("dag_expr_field_type_strict_check", expr_field_type_check);

    return std::make_tuple(context, grpc::Status::OK);
}

} // namespace DB
