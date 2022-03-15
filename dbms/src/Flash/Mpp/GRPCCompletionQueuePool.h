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
