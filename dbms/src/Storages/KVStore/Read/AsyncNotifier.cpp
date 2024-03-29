// Copyright 2024 PingCAP, Inc.
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

#include <Storages/KVStore/Read/ReadIndexWorkerImpl.h>

namespace DB
{
AsyncNotifier::Status AsyncWaker::Notifier::blockedWaitUtil(const SteadyClock::time_point & time_point)
{
    // if flag from false to false, wait for notification.
    // if flag from true to false, do nothing.
    auto res = AsyncNotifier::Status::Normal;
    if (!is_awake->exchange(false, std::memory_order_acq_rel))
    {
        {
            auto lock = genUniqueLock();
            if (!is_awake->load(std::memory_order_acquire))
            {
                if (condVar().wait_until(lock, time_point) == std::cv_status::timeout)
                    res = AsyncNotifier::Status::Timeout;
            }
        }
        is_awake->store(false, std::memory_order_release);
    }
    return res;
}

void AsyncWaker::Notifier::wake() NO_THREAD_SAFETY_ANALYSIS
{
    // if flag from false -> true, then wake up.
    // if flag from true -> true, do nothing.
    if (is_awake->load(std::memory_order_acquire))
        return;
    if (!is_awake->exchange(true, std::memory_order_acq_rel))
    {
        // wake up notifier
        auto _ = genLockGuard();
        condVar().notify_one();
    }
}

void AsyncWaker::wake(RawVoidPtr notifier_)
{
    auto & notifier = *reinterpret_cast<AsyncNotifier *>(notifier_);
    notifier.wake();
}

AsyncWaker::AsyncWaker(const TiFlashRaftProxyHelper & helper_)
    : AsyncWaker(helper_, new AsyncWaker::Notifier{})
{}

AsyncWaker::AsyncWaker(const TiFlashRaftProxyHelper & helper_, AsyncNotifier * notifier_)
    : inner(helper_.makeAsyncWaker(AsyncWaker::wake, GenRawCppPtr(notifier_, RawCppPtrTypeImpl::WakerNotifier)))
    , notifier(*notifier_)
{}

AsyncNotifier::Status AsyncWaker::waitUtil(SteadyClock::time_point time_point)
{
    return notifier.blockedWaitUtil(time_point);
}

RawVoidPtr AsyncWaker::getRaw() const
{
    return inner.ptr;
}
} // namespace DB