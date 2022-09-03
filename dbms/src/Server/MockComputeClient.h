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

#include <Common/Exception.h>
#include <coprocessor.pb.h>
#include <fmt/core.h>
#include <grpc++/grpc++.h>
#include <kvproto/tikvpb.grpc.pb.h>
using grpc::Channel;
using grpc::Status;

namespace DB
{
/// Send RPC Requests to FlashService
/// TODO: Support more methods that FlashService serve.
/// TODO: Support more config of RPC client.
class MockComputeClient
{
public:
    explicit MockComputeClient(std::shared_ptr<Channel> channel)
        : stub(tikvpb::Tikv::NewStub(channel))
    {}

    void runDispatchMPPTask(std::shared_ptr<mpp::DispatchTaskRequest> request)
    {
        mpp::DispatchTaskResponse response;
        grpc::ClientContext context;
        Status status = stub->DispatchMPPTask(&context, *request, &response);
        if (!status.ok())
        {
            throw Exception(fmt::format("Meet error while dispatch mpp task, error code = {}, message = {}", status.error_code(), status.error_message()));
        }
    }

private:
    std::unique_ptr<tikvpb::Tikv::Stub> stub{};
};
} // namespace DB