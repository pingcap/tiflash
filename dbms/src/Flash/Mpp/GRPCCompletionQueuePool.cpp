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

#include <Common/GRPCQueue.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>

namespace DB
{
std::unique_ptr<GRPCCompletionQueuePool> GRPCCompletionQueuePool::global_instance;

GRPCCompletionQueuePool::GRPCCompletionQueuePool(size_t count)
    : queues(count)
{
    for (size_t i = 0; i < count; ++i)
        workers.emplace_back(ThreadFactory::newThread(false, "GRPCComp", &GRPCCompletionQueuePool::thread, this, i));
}

GRPCCompletionQueuePool::~GRPCCompletionQueuePool()
{
    for (auto & queue : queues)
        queue.Shutdown();

    for (auto & t : workers)
        t.join();
}

::grpc::CompletionQueue & GRPCCompletionQueuePool::pickQueue()
{
    return queues[next.fetch_add(1, std::memory_order_acq_rel) % queues.size()];
}

void GRPCCompletionQueuePool::thread(size_t index)
{
    GET_METRIC(tiflash_thread_count, type_threads_of_client_cq_pool).Increment();
    SCOPE_EXIT({
        if (!is_shutdown)
        {
            GET_METRIC(tiflash_thread_count, type_threads_of_client_cq_pool).Decrement();
        }
    });

    auto & q = queues[index];
    while (true)
    {
        void * got_tag = nullptr;
        bool ok = false;
        if (!q.Next(&got_tag, &ok))
        {
            break;
        }
        reinterpret_cast<GRPCKickTag *>(got_tag)->execute(ok);
    }
}

} // namespace DB
