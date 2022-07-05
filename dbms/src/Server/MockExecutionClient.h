/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <Flash/CoprocessorHandler.h>
#include <Flash/FlashService.h>
#include <coprocessor.pb.h>
#include <kvproto/tikvpb.grpc.pb.h>

#include <string>
#include "grpcpp/impl/codegen/client_context.h"
#include "grpcpp/impl/codegen/status.h"

using grpc::Status;
using grpc_impl::Channel;
namespace DB
{
class MockExecutionClient
{
public:
    explicit MockExecutionClient(std::shared_ptr<Channel> channel)
        : stub(tikvpb::Tikv::NewStub(channel))
    {}


    std::string runCoprocessor(coprocessor::Request request,
        coprocessor::Response response)
    {
        
        grpc::ClientContext context;
        request.set_tp(103);
        Status status = stub->Coprocessor(&context, request, &response);
        if (status.ok())
        {
            return response.DebugString();
        }
        else
        {
            std::cout << status.error_code() << ": " << status.error_message()
                      << std::endl;
            return "RPC failed";
        }
    }

    std::string runDispatchMPPTask()
    {
        const ::mpp::DispatchTaskRequest request;
            ::mpp::DispatchTaskResponse response;
        grpc::ClientContext context;
        Status status = stub->DispatchMPPTask(&context, request, &response);
        if (status.ok())
        {
            return response.DebugString();
        }
        else
        {
            std::cout << status.error_code() << ": " << status.error_message()
                      << std::endl;
            return "RPC failed";
        }
    }

private:
    std::unique_ptr<tikvpb::Tikv::Stub> stub{};
};
} // namespace DB