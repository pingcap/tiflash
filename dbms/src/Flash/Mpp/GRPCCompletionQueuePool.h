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

#include <Common/ThreadFactory.h>
#include <Common/UnaryCallback.h>
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpc++/grpc++.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include <atomic>

namespace DB
{
class GRPCCompletionQueuePool
{
public:
    static std::unique_ptr<GRPCCompletionQueuePool> global_instance;

    explicit GRPCCompletionQueuePool(size_t count);
    ~GRPCCompletionQueuePool();

    ::grpc::CompletionQueue & pickQueue();

private:
    void thread(size_t index);

    std::atomic<size_t> next = 0;
    std::vector<::grpc::CompletionQueue> queues;
    std::vector<std::thread> workers;
};
} // namespace DB
