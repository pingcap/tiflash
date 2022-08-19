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

#include <grpc++/grpc++.h>

namespace DB
{

class CompletionQueueKicker
{
public:
    explicit CompletionQueueKicker(grpc_call * call)
        : call(call)
    {
        assert(call != nullptr);
    }

    void setTag(void * tag_)
    {
        assert(tag != nullptr);
        tag.store(tag_, std::memory_order_release);
    }

    // Wakes up its completion queue.
    //
    // `tag` will be popped by `grpc_completion_queue_next` in the future.
    void kick()
    {
        auto * old_tag = tag.load(std::memory_order_acquire);
        if (old_tag == nullptr)
        {
            return;
        }
        // If CAS returns false, it means another thread's CAS succeeds and it will kick the grpc completion queue.
        if (tag.compare_exchange_strong(old_tag, nullptr, std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            // If a call to grpc_call_start_batch with an empty batch returns
            // GRPC_CALL_OK, the tag is put in the completion queue immediately.
            // https://github.com/grpc/grpc/issues/16357
            auto error = grpc_call_start_batch(call, nullptr, 0, old_tag, nullptr);
            // If an error occur, there must be something wrong about shutdown process
            RUNTIME_ASSERT(error != grpc_call_error::GRPC_CALL_OK, "{} != GRPC_CALL_OK, memory of tag may leak", error);
        }
    }

private:
    grpc_call * call;
    std::atomic<void *> tag{};
};

using CompletionQueueKickerPtr = std::shared_ptr<CompletionQueueKicker>;

} // namespace DB