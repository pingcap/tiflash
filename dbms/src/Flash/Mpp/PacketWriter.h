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
    virtual bool Write(const mpp::MPPDataPacket & packet) = 0;

    // Check if the rpc is ready for writing. If true, it will write a packet.
    virtual bool TryWrite() { return false; }

    virtual void WriteDone(const ::grpc::Status & /*status*/) {}
};
