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

#include <DataStreams/BlockIO.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/server_context.h>
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
struct DecodedTiKVKey;
using DecodedTiKVKeyPtr = std::shared_ptr<DecodedTiKVKey>;

struct CoprocessorContext
{
    Context & db_context;
    const kvrpcpb::Context & kv_context;
    const grpc::ServerContext & grpc_server_context;

    CoprocessorContext(Context & db_context_, const kvrpcpb::Context & kv_context_, const grpc::ServerContext & grpc_server_context_);
};

/// Coprocessor request handler, deals with:
/// 1. DAG request: WIP;
/// 2. Analyze request: NOT IMPLEMENTED;
/// 3. Checksum request: NOT IMPLEMENTED;
class CoprocessorHandler
{
public:
    CoprocessorHandler(CoprocessorContext & cop_context_, const coprocessor::Request * cop_request_, coprocessor::Response * response_);

    virtual ~CoprocessorHandler() = default;

    virtual grpc::Status execute();

    static std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> GenCopKeyRange(
        const ::google::protobuf::RepeatedPtrField<::coprocessor::KeyRange> & ranges);

protected:
    virtual grpc::Status recordError(grpc::StatusCode err_code, const String & err_msg);

protected:
    enum
    {
        COP_REQ_TYPE_DAG = 103,
        COP_REQ_TYPE_ANALYZE = 104,
        COP_REQ_TYPE_CHECKSUM = 105,
    };

    CoprocessorContext & cop_context;
    const coprocessor::Request * cop_request;
    coprocessor::Response * cop_response;

    Poco::Logger * log;
};

} // namespace DB
