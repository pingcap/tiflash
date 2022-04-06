#pragma once

#include <Common/FiberRWLock.h>
#include <boost/fiber/all.hpp> 
#include <mutex>
#include <shared_mutex>
#include <condition_variable>

namespace DB
{
struct FiberTraits
{
#ifdef TIFLASH_USE_FIBER
    using Mutex = boost::fibers::mutex;
    using ConditionVariable = boost::fibers::condition_variable;
    using SharedMutex = FiberRWLock;
    template <typename T>
    using PackagedTask = boost::fibers::packaged_task<T>;
    template <typename T>
    using Promise = boost::fibers::promise<T>;
    template <typename T>
    using SharedFuture = boost::fibers::shared_future<T>;
#else
    using Mutex = std::mutex;
    using ConditionVariable = std::condition_variable;
    using SharedMutex = std::shared_mutex;
    template <typename T>
    using PackagedTask = std::packaged_task<T>;
    template <typename T>
    using Promise = std::promise<T>;
    template <typename T>
    using SharedFuture = std::shared_future<T>;
#endif
};

} // namespace DB
