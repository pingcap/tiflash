#pragma once
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <kvproto/tikvpb.grpc.pb.h>

#pragma GCC diagnostic pop

namespace DB
{
// Writer used by MPPTunnel, to wrap both async & sync RPC writer.
class PacketWriter
{
public:
    virtual ~PacketWriter() = default;

    // Write a packet, if this api is called, it means the rpc is ready for writing
    // For sync writer, it is always ready for writing.
    // For async writer, it is judged and called by "TryWrite".
    // If fail to write, it will return false.
    virtual bool Write(const mpp::MPPDataPacket & packet) = 0;

    // Check if the rpc is ready for writing. If true, it will write a packet.
    // Because for async writer,
    // the caller can't know if the rpc session is ready for writing.
    // If it is not ready, caller can't write a packet.
    virtual bool TryWrite() { return false; }


    // Finish rpc with a status. Used in async mode in most cases. In sync mode, it will return Status directly.
    virtual void WriteDone(const ::grpc::Status & /*status*/) {}
};
}; // namespace DB
