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
#include <Common/Logger.h>
#include <Common/grpcpp.h>

#include <magic_enum.hpp>

namespace DB
{

class GRPCKickTag;
using GRPCKickFunc = std::function<grpc_call_error(GRPCKickTag *)>;

/// In grpc cpp framework, the tag that is pushed into grpc completion
/// queue must be inherited from `CompletionQueueTag`.
/// The grpc cpp framework provides a tool named `Alarm` can be used to push a tag into
/// completion queue thus the next write/read can be done in grpc threads. But `Alarm` must need
/// a timeout and it uses a timer to trigger the notification, which is wasteful if we want
/// to trigger it immediately. So we can say `kick` function is a immediately-triggered `Alarm`.
class GRPCKickTag : public grpc::internal::CompletionQueueTag
{
public:
    GRPCKickTag()
        : call(nullptr)
        , status(true)
    {}

    virtual void execute(bool ok) = 0;

    bool FinalizeResult(void ** tag_, bool * status_) override
    {
        *tag_ = this;
        *status_ = status;
        return true;
    }

    void kick(const GRPCKickFunc & test_kick_func = nullptr)
    {
        grpc_call_error error;
        if unlikely (test_kick_func)
        {
            error = test_kick_func(this);
        }
        else
        {
            // If a call to `grpc_call_start_batch` with an empty batch returns
            // `GRPC_CALL_OK`, the tag is pushed into the completion queue immediately.
            // This behavior is well-defined. See https://github.com/grpc/grpc/issues/16357.
            error = grpc_call_start_batch(
                call,
                nullptr,
                0,
                static_cast<grpc::internal::CompletionQueueTag *>(this),
                nullptr);
        }
        // If an error occur, there must be something wrong about shutdown process.
        RUNTIME_ASSERT(
            error == grpc_call_error::GRPC_CALL_OK,
            "grpc_call_start_batch returns {} != GRPC_CALL_OK, memory of tag may leak",
            magic_enum::enum_name(error));
    }

    grpc_call * getCall() const { return call; }

    void setCall(grpc_call * call_) { call = call_; }

    void setStatus(bool status_) { status = status_; }

    bool getStatus() const { return status; }

    GRPCKickTag * asGRPCKickTag() { return this; }

private:
    grpc_call * call;
    bool status;
};

/// For test usage only.
class DummyGRPCKickTag : public GRPCKickTag
{
    void execute(bool) override {}
};

} // namespace DB
