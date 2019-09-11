#include <Flash/BatchCommandsHandler.h>
#include <Flash/CoprocessorHandler.h>
#include <common/ThreadPool.h>

namespace DB
{

BatchCommandsHandler::BatchCommandsHandler(BatchCommandsContext & batch_commands_context_, const tikvpb::BatchCommandsRequest & request_,
    tikvpb::BatchCommandsResponse & response_)
    : batch_commands_context(batch_commands_context_), request(request_), response(response_), log(&Logger::get("BatchCommandsHandler"))
{}

grpc::Status BatchCommandsHandler::execute()
{
    ThreadPool thread_pool(SettingMaxThreads().getAutoValue() * 0.8);

    auto command_handler_func
        = [](BatchCommandsContext::DBContextCreationFunc db_context_creation_func, grpc::ServerContext * grpc_server_context,
              const tikvpb::BatchCommandsRequest::Request & req, tikvpb::BatchCommandsResponse::Response & resp, grpc::Status & ret) {
              if (!req.has_coprocessor())
              {
                  ret = grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
                  return;
              }

              const auto & cop_req = req.coprocessor();
              auto cop_resp = resp.mutable_coprocessor();

              auto [context, status] = db_context_creation_func(grpc_server_context);
              if (!status.ok())
              {
                  ret = status;
                  return;
              }

              CoprocessorContext cop_context(context, cop_req.context(), *grpc_server_context);
              CoprocessorHandler cop_handler(cop_context, &cop_req, cop_resp);

              ret = cop_handler.execute();
          };

    std::vector<grpc::Status> rets;

    size_t i = 0;
    for (const auto & req : request.requests())
    {
        auto resp = response.add_responses();
        response.add_request_ids(request.request_ids(i++));
        rets.emplace_back(grpc::Status::OK);
        thread_pool.schedule([&]() {
            command_handler_func(
                batch_commands_context.db_context_creation_func, &batch_commands_context.grpc_server_context, req, *resp, rets.back());
        });
    }

    thread_pool.wait();

    // Iterate all return values of each individual commands, returns the first non-OK one if any.
    for (const auto & ret : rets)
    {
        if (!ret.ok())
            return ret;
    }

    return grpc::Status::OK;
}

} // namespace DB
