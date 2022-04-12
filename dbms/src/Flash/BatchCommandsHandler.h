// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Interpreters/Context.h>
#include <common/ThreadPool.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/server_context.h>
#include <kvproto/tikvpb.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
struct BatchCommandsContext
{
    /// Context for this batch commands.
    Context & db_context;

    /// Context creation function for each individual command - they should be handled isolated,
    /// given that context is being used to pass arguments regarding queries.
    using DBContextCreationFunc = std::function<std::tuple<ContextPtr, grpc::Status>(const grpc::ServerContext *)>;
    DBContextCreationFunc db_context_creation_func;

    const grpc::ServerContext & grpc_server_context;

    BatchCommandsContext(
        Context & db_context_,
        DBContextCreationFunc && db_context_creation_func_,
        grpc::ServerContext & grpc_server_context_);
};

class BatchCommandsHandler
{
public:
    BatchCommandsHandler(BatchCommandsContext & batch_commands_context_, const tikvpb::BatchCommandsRequest & request_, tikvpb::BatchCommandsResponse & response_);

    ~BatchCommandsHandler() = default;

    grpc::Status execute();

protected:
    ThreadPool::Job handleCommandJob(
        const tikvpb::BatchCommandsRequest::Request & req,
        tikvpb::BatchCommandsResponse::Response & resp,
        grpc::Status & ret) const;

protected:
    const BatchCommandsContext & batch_commands_context;
    const tikvpb::BatchCommandsRequest & request;
    tikvpb::BatchCommandsResponse & response;

    Poco::Logger * log;
};

} // namespace DB
