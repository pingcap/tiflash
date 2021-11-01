#pragma once

#include <Common/Exception.h>
#include <grpcpp/impl/codegen/sync_stream.h>
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>

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
        std::lock_guard<std::mutex> lk(write_mutex);
        if (!writer->Write(resp))
            throw Exception("Failed to write resp");
    }
    // a helper function
    uint16_t getPartitionNum() { return 0; }
};

using StreamWriterPtr = std::shared_ptr<StreamWriter>;
} // namespace DB
