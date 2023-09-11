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

#include <Common/Logger.h>
#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/RegionInfo.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/Types.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/impl/codegen/sync_stream.h>
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include <vector>

namespace DB
{
class Context;

/// An abstraction of driver running DAG request.
/// Now is a naive native executor. Might get evolved to drive MPP-like computation.
template <DAGRequestKind Kind>
class DAGDriver
{
public:
    DAGDriver(
        Context & context_,
        UInt64 start_ts,
        UInt64 schema_ver,
        tipb::SelectResponse * cop_response_,
        bool internal_ = false);

    DAGDriver(
        Context & context_,
        UInt64 start_ts,
        UInt64 schema_ver,
        grpc::ServerWriter<::coprocessor::Response> * cop_writer_,
        bool internal_ = false);

    DAGDriver(
        Context & context_,
        UInt64 start_ts,
        UInt64 schema_ver,
        grpc::ServerWriter<coprocessor::BatchResponse> * batch_cop_writer_,
        bool internal_ = false);

    void execute();

private:
    void recordError(Int32 err_code, const String & err_msg);

    const tipb::DAGRequest & dagRequest() const;

    Context & context;

    tipb::SelectResponse * cop_response = nullptr;
    grpc::ServerWriter<coprocessor::Response> * cop_writer = nullptr;
    grpc::ServerWriter<coprocessor::BatchResponse> * batch_cop_writer = nullptr;

    bool internal;

    LoggerPtr log;
};
} // namespace DB
