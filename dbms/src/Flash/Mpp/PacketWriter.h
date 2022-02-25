#pragma once
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <kvproto/tikvpb.grpc.pb.h>

#pragma GCC diagnostic pop

class PacketWriter
{
public:
    virtual ~PacketWriter(){};

    // Write a packet, if this api is called, it means the rpc is ready for writing
    // For sync writer, it is always ready for writing.
    // For async writer, it is judged and called by "TryWrite".
    virtual bool Write(const mpp::MPPDataPacket & packet) = 0;

    // Check if the rpc is ready for writing. If true, it will write a packet.
    // Because for async writer,
    // the caller can't know if the rpc session is ready for writing.
    // If it is not ready, caller can't write a packet.
    virtual bool TryWrite() { return false; }

    virtual void WriteDone(const ::grpc::Status & /*status*/) {}
};
