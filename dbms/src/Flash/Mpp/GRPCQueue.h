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

#include <Common/grpcpp.h>

#include <magic_enum.hpp>

namespace DB
{
/// In grpc cpp framework, the tag that is pushed into grpc completion
/// queue must be inherited from `CompletionQueueTag`.
class KickTag : public grpc::internal::CompletionQueueTag
{
public:
    explicit KickTag(std::function<void *()> a)
        : action(std::move(a))
    {}

    bool FinalizeResult(void ** tag_, bool * /*status*/) override
    {
        *tag_ = action();
        return true;
    }

private:
    /// `action` is called before the `tag` is popped from completion queue
    /// in `FinalizeResult`.
    std::function<void *()> action;
};

using GRPCKickFunc = std::function<grpc_call_error(KickTag *)>;

class GRPCQueue
{
public:
    GRPCQueue(grpc_call * call, const LoggerPtr & log_)
        : log(log_)
        , kick_tag([this]() { return kickTagAction(); })
    {
        RUNTIME_ASSERT(call != nullptr, log, "call is null");
        // If a call to `grpc_call_start_batch` with an empty batch returns
        // `GRPC_CALL_OK`, the tag is pushed into the completion queue immediately.
        // This behavior is well-defined. See https://github.com/grpc/grpc/issues/16357.
        kick_func = [call](void * t) {
            return grpc_call_start_batch(call, nullptr, 0, t, nullptr);
        };
    }

    // For gtest usage.
    explicit GRPCQueue(GRPCKickFunc func)
        : log(Logger::get())
        , kick_func(func)
        , kick_tag([this]() { return kickTagAction(); })
    {}

protected:
    void * kickTagAction()
    {
        std::unique_lock lock(mu);

        RUNTIME_ASSERT(status == Status::QUEUING, log, "status {} is not queuing", magic_enum::enum_name(status));
        status = Status::NONE;

        return std::exchange(tag, nullptr);
    }

    /// Wake up its completion queue.
    void kickCompletionQueue()
    {
        {
            std::unique_lock lock(mu);
            if (status != Status::WAITING)
                return;

            RUNTIME_ASSERT(tag != nullptr, log, "status is waiting but tag is nullptr");
            status = Status::QUEUING;
        }

        grpc_call_error error = kick_func(&kick_tag);
        // If an error occur, there must be something wrong about shutdown process.
        RUNTIME_ASSERT(error == grpc_call_error::GRPC_CALL_OK, log, "grpc_call_start_batch returns {} != GRPC_CALL_OK, memory of tag may leak", error);
    }

    enum class Status
    {
        /// No tag.
        NONE,
        /// Waiting for kicking.
        WAITING,
        /// Queuing in the grpc completion queue.
        QUEUING,
    };

    const LoggerPtr log;

    /// The mutex is used to synchronize the concurrent calls between push/finish and pop.
    /// It protects `status` and `tag`.
    /// The concurrency problem we want to prevent here is LOST NOTIFICATION which is
    /// similar to `condition_variable`.
    /// Note that this mutex is necessary. It's useless to just change the `tag` to atomic.
    ///
    /// Imagine this case in GRPCSendQueue:
    /// Thread 1: want to pop the data from queue but find no data there.
    /// Thread 2: push/finish the data in queue.
    /// Thread 2: do not kick the completion queue because tag is nullptr.
    /// Thread 1: set the tag.
    ///
    /// If there is no more data, this connection will get stuck forever.
    std::mutex mu;
    Status status = Status::NONE;
    void * tag = nullptr;
    GRPCKickFunc kick_func;
    KickTag kick_tag;
};
} // namespace DB
