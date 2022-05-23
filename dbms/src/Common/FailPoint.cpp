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

#include <Common/FailPoint.h>

#include <boost/core/noncopyable.hpp>
#include <condition_variable>
#include <mutex>

namespace DB
{
std::unordered_map<String, std::shared_ptr<FailPointChannel>> FailPointHelper::fail_point_wait_channels;

#ifdef FIU_ENABLE
class FailPointChannel : private boost::noncopyable
{
public:
    // wake up all waiting threads when destroy
    ~FailPointChannel() { notifyAll(); }

    void wait()
    {
        std::unique_lock lock(m);
        cv.wait(lock);
    }

    void notifyAll()
    {
        std::unique_lock lock(m);
        cv.notify_all();
    }

private:
    std::mutex m;
    std::condition_variable cv;
};

void FailPointHelper::enableFailPoint(const String & fail_point_name)
{
#define SUB_M(NAME, flags)                                                                                  \
    if (fail_point_name == FailPoints::NAME)                                                                \
    {                                                                                                       \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/ \
        fiu_enable(FailPoints::NAME, 1, nullptr, flags);                                                    \
        return;                                                                                             \
    }

#define M(NAME) SUB_M(NAME, FIU_ONETIME)
    __APPLY_FOR_FAILPOINTS_ONCE(M)
#undef M
#define M(NAME) SUB_M(NAME, 0)
    __APPLY_FOR_FAILPOINTS(M)
#undef M
#undef SUB_M

#define SUB_M(NAME, flags)                                                                                  \
    if (fail_point_name == FailPoints::NAME)                                                                \
    {                                                                                                       \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/ \
        fiu_enable(FailPoints::NAME, 1, nullptr, flags);                                                    \
        fail_point_wait_channels.try_emplace(FailPoints::NAME, std::make_shared<FailPointChannel>());       \
        return;                                                                                             \
    }

#define M(NAME) SUB_M(NAME, FIU_ONETIME)
    __APPLY_FOR_FAILPOINTS_ONCE_WITH_CHANNEL(M)
#undef M

#define M(NAME) SUB_M(NAME, 0)
    __APPLY_FOR_FAILPOINTS_WITH_CHANNEL(M)
#undef M
#undef SUB_M

    throw Exception("Cannot find fail point " + fail_point_name, ErrorCodes::FAIL_POINT_ERROR);
}

void FailPointHelper::disableFailPoint(const String & fail_point_name)
{
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter != fail_point_wait_channels.end())
    {
        /// can not rely on deconstruction to do the notify_all things, because
        /// if someone wait on this, the deconstruct will never be called.
        iter->second->notifyAll();
        fail_point_wait_channels.erase(iter);
    }
    fiu_disable(fail_point_name.c_str());
}

void FailPointHelper::wait(const String & fail_point_name)
{
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter == fail_point_wait_channels.end())
        throw Exception("Can not find channel for fail point " + fail_point_name);
    else
    {
        auto ptr = iter->second;
        ptr->wait();
    }
}
#else
class FailPointChannel
{
};

void FailPointHelper::enableFailPoint(const String &) {}

void FailPointHelper::disableFailPoint(const String &) {}

void FailPointHelper::wait(const String &) {}
#endif

} // namespace DB
