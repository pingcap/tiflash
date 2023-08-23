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
// PacketWriter is a common interface of sync gRPC writer.
// It is used as the template parameter of `MPPTunnel`.
class PacketWriter
{
public:
    virtual ~PacketWriter() = default;

    // Write a packet and return false if any error occurs.
    virtual bool write(const mpp::MPPDataPacket & packet) = 0;
};

class SyncPacketWriter : public PacketWriter
{
public:
    explicit SyncPacketWriter(grpc::ServerWriter<mpp::MPPDataPacket> * writer)
        : writer(writer)
    {}

    bool write(const mpp::MPPDataPacket & packet) override { return writer->Write(packet); }

private:
    ::grpc::ServerWriter<::mpp::MPPDataPacket> * writer;
};

}; // namespace DB
