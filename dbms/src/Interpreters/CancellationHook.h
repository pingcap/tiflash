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

#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <vector>

namespace DB
{
using CancellationHook = std::function<bool()>;

// Shared by a request's Context copies, including while its executor is being
// constructed. Callbacks must not throw and must not retain the request itself.
class CancellationSource
{
public:
    bool isCancelled() const { return cancelled.load(std::memory_order_acquire); }

    void addCallback(std::function<void()> callback)
    {
        {
            std::lock_guard lock(mutex);
            if (!isCancelled())
            {
                callbacks.push_back(std::move(callback));
                return;
            }
        }
        callback();
    }

    void cancel()
    {
        std::vector<std::function<void()>> pending;
        {
            std::lock_guard lock(mutex);
            if (cancelled.exchange(true, std::memory_order_acq_rel))
                return;
            pending.swap(callbacks);
        }
        // Never run callbacks under the registry lock.
        for (auto & callback : pending)
            callback();
    }

private:
    std::atomic<bool> cancelled{false};
    std::mutex mutex;
    std::vector<std::function<void()>> callbacks;
};
} // namespace DB
