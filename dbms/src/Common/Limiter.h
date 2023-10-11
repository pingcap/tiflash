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

    T execute(std::function<T()> func)
    {
        {
            std::unique_lock lock(mu);
            cv.wait(lock, [&]() { return active_count < limit; });
            ++active_count;
        }
        auto ret = func();
        {
            std::lock_guard lock(mu);
            --active_count;
        }
        cv.notify_one();
        return ret;
    }

    UInt64 getLimit() const { return limit; }

private:
    const UInt64 limit;
    std::mutex mu;
    std::condition_variable cv;
    UInt64 active_count = 0;
};

} // namespace DB
