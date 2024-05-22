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

#include <Common/Exception.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/WaitResult.h>
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
#include <mutex>

namespace mpp
{
class MPPDataPacket;
} // namespace mpp

namespace DB
{

struct CopStreamWriter
{
    grpc::ServerWriter<coprocessor::Response> * writer;
    std::mutex write_mutex;

    explicit CopStreamWriter(grpc::ServerWriter<coprocessor::Response> * writer_)
        : writer(writer_)
    {}
    void write(tipb::SelectResponse & response)
    {
        coprocessor::Response resp;
        if (!response.SerializeToString(resp.mutable_data()))
            throw Exception(
                "[StreamWriter]Fail to serialize response, response size: " + std::to_string(response.ByteSizeLong()));

        GET_METRIC(tiflash_coprocessor_response_bytes, type_cop_stream).Increment(resp.ByteSizeLong());

        std::lock_guard lk(write_mutex);
        if (!writer->Write(resp))
            throw Exception("Failed to write resp");
    }
    static bool isWritable() { throw Exception("Unsupport async write"); }
    static WaitResult waitForWritable() { throw Exception("Unsupport async write"); }
};

struct BatchCopStreamWriter
{
    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer;
    std::mutex write_mutex;

    explicit BatchCopStreamWriter(::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_)
        : writer(writer_)
    {}
    void write(tipb::SelectResponse & response)
    {
        ::coprocessor::BatchResponse resp;
        if (!response.SerializeToString(resp.mutable_data()))
            throw Exception(
                "[StreamWriter]Fail to serialize response, response size: " + std::to_string(response.ByteSizeLong()));

        GET_METRIC(tiflash_coprocessor_response_bytes, type_batch_cop).Increment(resp.ByteSizeLong());

        std::lock_guard lk(write_mutex);
        if (!writer->Write(resp))
            throw Exception("Failed to write resp");
    }
    static bool isWritable() { throw Exception("Unsupport async write"); }
    static WaitResult waitForWritable() { throw Exception("Unsupport async write"); }
};

using CopStreamWriterPtr = std::shared_ptr<CopStreamWriter>;
using BatchCopStreamWriterPtr = std::shared_ptr<BatchCopStreamWriter>;
} // namespace DB
