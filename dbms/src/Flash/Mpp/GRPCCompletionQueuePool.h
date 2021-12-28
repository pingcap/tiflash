#pragma once

#include <Common/ThreadFactory.h>
#include <Common/UnaryCallback.h>
#include <grpc++/grpc++.h>

#include <atomic>
#include <boost/fiber/all.hpp>

namespace DB
{
class GRPCCompletionQueuePool
{
public:
    static GRPCCompletionQueuePool * Instance()
    {
        static auto concurrency = std::max(std::thread::hardware_concurrency(), 2u) - 1u;
        static GRPCCompletionQueuePool pool(concurrency);
        return &pool;
    }

    ::grpc::CompletionQueue & pickQueue()
    {
        return queues[next.fetch_add(1, std::memory_order_acq_rel) % queues.size()];
    }

    using Callback = UnaryCallback<bool>;

private:
    explicit GRPCCompletionQueuePool(size_t count)
        : queues(count)
    {
        for (size_t i = 0; i < count; ++i)
            workers.emplace_back(ThreadFactory::newThread("GRPCComp", &GRPCCompletionQueuePool::thread, this, i));
    }

    void thread(size_t index)
    {
        auto & q = queues[index];
        while (true)
        {
            void * got_tag = nullptr;
            bool ok = false;
            if (!q.Next(&got_tag, &ok))
            {
                break;
            }
            reinterpret_cast<Callback *>(got_tag)->execute(ok);
        }
    }

    std::atomic<size_t> next = 0;
    std::vector<::grpc::CompletionQueue> queues;
    std::vector<std::thread> workers;
};
} // namespace DB
