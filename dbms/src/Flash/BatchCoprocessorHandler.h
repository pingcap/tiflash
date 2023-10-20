// Copyright 2023 PingCAP, Inc.
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

#include <Flash/CoprocessorHandler.h>
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/impl/codegen/sync_stream.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

namespace DB
{
class BatchCoprocessorHandler
{
public:
    BatchCoprocessorHandler(
        CoprocessorContext & cop_context_,
        const coprocessor::BatchRequest * cop_request_,
        ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_,
        const String & identifier);

    grpc::Status execute();

protected:
    grpc::Status recordError(grpc::StatusCode err_code, const String & err_msg);

protected:
    CoprocessorContext & cop_context;

    const coprocessor::BatchRequest * cop_request;
    grpc::ServerWriter<coprocessor::BatchResponse> * writer;

    const String resource_group_name;
    const LoggerPtr log;
};

using BatchCopHandlerPtr = std::shared_ptr<BatchCoprocessorHandler>;

} // namespace DB
