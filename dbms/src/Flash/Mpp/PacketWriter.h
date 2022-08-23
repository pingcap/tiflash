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
// PacketWriter is a common interface of both sync and async gRPC writer.
// It is used as the template parameter of `MPPTunnel`.
//
// TODO: In async grpc writer, the `write` function should only be called
// through the grpc thread. Also, the `attachAsyncTunnelSender` and `grpcCall`
// functions are used only for async grpc writer. We should find a more suitable
// abstraction.
class AsyncTunnelSender;
class PacketWriter
{
public:
    virtual ~PacketWriter() = default;

    // Write a packet and return false if any error occurs.
    // Note: in async mode the end of `Write` doesn't mean the `packet` is actually written done.
    virtual bool write(const mpp::MPPDataPacket & packet) = 0;

    // Finish rpc with a status. Needed by async writer. For sync writer it is useless but not harmful.
    virtual void writeDone(const ::grpc::Status & /*status*/) {}

    // Attach async sender to async writer so that async writer can use it to get/transfer DataPacket and set consumer finish msg directly
    virtual void attachAsyncTunnelSender(const std::shared_ptr<AsyncTunnelSender> &) {}

    // Get the pointer of `grpc_call`.
    virtual grpc_call * grpcCall() { return nullptr; }
};
}; // namespace DB
