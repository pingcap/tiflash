#pragma once

#include <Common/FiberRWLock.h>
#include <boost/fiber/all.hpp> 
#include <mutex>
#include <condition_variable>

namespace DB
{
struct FiberTraits
{
#ifdef TIFLASH_USE_FIBER
    using Mutex = boost::fibers::mutex;
    using ConditionVariable = boost::fibers::condition_variable;
    using SharedMutex = FiberRWLock;
#else
    using Mutex = std::mutex;
    using ConditionVariable = std::condition_variable;
    using SharedMutex = std::shared_lock;
#endif
};

} // namespace DB
