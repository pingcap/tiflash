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
struct StreamWriter
{
    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer;
    std::mutex write_mutex;

    explicit StreamWriter(::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_)
        : writer(writer_)
    {}
    void write(mpp::MPPDataPacket &)
    {
        throw Exception("StreamWriter::write(mpp::MPPDataPacket &) do not support writing MPPDataPacket!");
    }
    void write(mpp::MPPDataPacket &, [[maybe_unused]] uint16_t)
    {
        throw Exception("StreamWriter::write(mpp::MPPDataPacket &, [[maybe_unused]] uint16_t) do not support writing MPPDataPacket!");
    }
    void write(tipb::SelectResponse & response, [[maybe_unused]] uint16_t id = 0)
    {
        ::coprocessor::BatchResponse resp;
        if (!response.SerializeToString(resp.mutable_data()))
            throw Exception("Fail to serialize response, response size: " + std::to_string(response.ByteSizeLong()));
        std::lock_guard lk(write_mutex);
        if (!writer->Write(resp))
            throw Exception("Failed to write resp");
    }
    // a helper function
    uint16_t getPartitionNum() { return 0; }
};

using StreamWriterPtr = std::shared_ptr<StreamWriter>;
} // namespace DB
