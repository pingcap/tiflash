#pragma once

#include <Common/ThreadFactory.h>
#include <common/logger_useful.h>
#include <boost/fiber/all.hpp>
#include <grpc++/grpc++.h>
#include <atomic>

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

    struct Callback
    {
        virtual void run(bool ok) = 0;
        virtual ~Callback() = default;
    };
private:
    explicit GRPCCompletionQueuePool(size_t count)
        : queues(count)
        , log(&Poco::Logger::get("GRPCCompletionQueuePool"))
    {
        LOG_DEBUG(log, "Construct count = " << count);
        for (size_t i = 0; i < count; ++i)
            workers.emplace_back(ThreadFactory(true, "GRPCComp").newThread(&GRPCCompletionQueuePool::thread, this, i));
    }

    void thread(size_t index)
    {
        LOG_DEBUG(log, "Thread start index = " << index);
        auto & q = queues[index];
        while (true)
        {
            void* got_tag = nullptr;
            bool ok = false;
            if (!q.Next(&got_tag, &ok)) {
                LOG_DEBUG(log, "Thread end index = " << index);
                break;
            }
            reinterpret_cast<Callback *>(got_tag)->run(ok);
        }
    }

    std::atomic<size_t> next = 0;
    std::vector<::grpc::CompletionQueue> queues;
    std::vector<std::thread> workers;
    Poco::Logger * log;
};
} // namespace

