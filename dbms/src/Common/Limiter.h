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

#pragma once

#include <Common/Exception.h>
#include <common/types.h>

#include <condition_variable>
#include <ext/scope_guard.h>
#include <mutex>

namespace DB
{
template <typename T>
class Limiter
{
public:
    explicit Limiter(UInt64 limit_)
        : limit(limit_)
    {
        RUNTIME_ASSERT(limit > 0);
    }

    ~Limiter()
    {
        std::unique_lock lock(mu);
        cv.wait(lock, [&]() { return active_count == 0; });
    }

    template <typename Duration>
    T executeFor(std::function<T()> exec_func, const Duration & max_wait_time, std::function<T()> timeout_func)
    {
        // If max_wait_time is 0, it means the wait time is infinite.
        if (max_wait_time <= max_wait_time.zero())
            return execute(exec_func);

        bool is_timeout = false;
        {
            std::unique_lock lock(mu);
            if (cv.wait_for(lock, max_wait_time, [&]() { return active_count < limit; }))
            {
                ++active_count;
            }
            else
            {
                is_timeout = true;
            }
        }

        if (is_timeout)
        {
            return timeout_func();
        }
        else
        {
            SCOPE_EXIT({
                {
                    std::lock_guard lock(mu);
                    --active_count;
                }
                cv.notify_one();
            });
            return exec_func();
        }
    }

    T execute(std::function<T()> func)
    {
        {
            std::unique_lock lock(mu);
            cv.wait(lock, [&]() { return active_count < limit; });
            ++active_count;
        }
        SCOPE_EXIT({
            {
                std::lock_guard lock(mu);
                --active_count;
            }
            cv.notify_one();
        });
        return func();
    }

    UInt64 getLimit() const { return limit; }

    UInt64 getActiveCount()
    {
        std::lock_guard lock(mu);
        return active_count;
    }

private:
    const UInt64 limit;
    std::mutex mu;
    std::condition_variable cv;
    UInt64 active_count = 0;
};

} // namespace DB
