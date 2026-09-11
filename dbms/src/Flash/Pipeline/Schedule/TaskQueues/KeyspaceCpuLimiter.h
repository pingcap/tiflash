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

#include <Common/TiFlashMetrics.h>
#include <Storages/KVStore/Types.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{
class Task;

/// Limits scheduler work for one keyspace.
///
/// The limiter is shared by the CPU and IO pools. Besides the concurrent-task
/// guard, it optionally maintains a CPU-time token bucket per keyspace. The
/// bucket is charged with thread CPU time, so blocking IO does not consume it
/// while CPU spent by columnar IO tasks does. Work outside these scheduler
/// pools is outside of its scope.
class KeyspaceCpuLimiter
{
public:
    explicit KeyspaceCpuLimiter(size_t max_active_tasks_, UInt64 cpu_quota_per_second_ns_ = 0)
        : max_active_tasks(max_active_tasks_)
        , cpu_quota_per_second_ns(cpu_quota_per_second_ns_)
        // Allow one second of quota to be used as a burst while preserving the
        // configured long-term rate.
        , cpu_quota_burst_ns(cpu_quota_per_second_ns_ == 0 ? 0 : std::max<UInt64>(1, cpu_quota_per_second_ns_))
    {}

    bool tryAcquire(KeyspaceID keyspace_id)
    {
        if (!isEnabled())
            return true;

        std::lock_guard lock(mu);
        auto & quota = getCPUQuotaWithoutLock(keyspace_id);
        if (cpu_quota_per_second_ns != 0)
        {
            refillCPUQuotaWithoutLock(quota);
            if (quota.tokens_ns <= 0)
            {
                quota.metrics->cpu_quota_throttled_total->Increment();
                updateMetricsWithoutLock(keyspace_id, quota);
                return false;
            }
        }

        auto & active = active_tasks[keyspace_id];
        if (max_active_tasks != 0 && active >= max_active_tasks)
        {
            quota.metrics->active_tasks_throttled_total->Increment();
            updateMetricsWithoutLock(keyspace_id, quota);
            return false;
        }
        ++active;
        updateMetricsWithoutLock(keyspace_id, quota);
        return true;
    }

    /// Associate a reservation made by tryAcquire with the task that owns it.
    /// The association makes release and CPU charging owner-aware, and avoids
    /// releasing a slot for a cancelled task that never acquired one.
    void bindOwner(KeyspaceID keyspace_id, const Task * task)
    {
        if (!isEnabled())
            return;

        std::lock_guard lock(mu);
        active_owners.emplace(task, keyspace_id);
    }

    bool isEnabled() const { return max_active_tasks != 0 || cpu_quota_per_second_ns != 0; }

    /// Charge CPU consumed since the previous execute() call. Returning true
    /// asks the task thread to yield before it starts another execution round.
    bool consumeCPUTime(const Task * task, UInt64 cpu_time_ns)
    {
        if (cpu_quota_per_second_ns == 0 || cpu_time_ns == 0)
            return false;

        std::lock_guard lock(mu);
        const auto owner_iter = active_owners.find(task);
        if (owner_iter == active_owners.end())
            return false;

        auto & quota = cpu_quotas[owner_iter->second];
        refillCPUQuotaWithoutLock(quota);
        quota.tokens_ns -= static_cast<double>(cpu_time_ns);
        quota.metrics->cpu_seconds_total->Increment(static_cast<double>(cpu_time_ns) / 1'000'000'000.0);
        updateMetricsWithoutLock(owner_iter->second, quota);
        return quota.tokens_ns <= 0;
    }

    /// Token refill is time based, so a queue with only throttled work must
    /// periodically retry even when no task is submitted or completed.
    std::chrono::milliseconds getRefillWaitDuration() const
    {
        return cpu_quota_per_second_ns == 0 ? std::chrono::milliseconds::max() : std::chrono::milliseconds(1);
    }

    void release(const Task * task)
    {
        if (!isEnabled())
            return;

        std::lock_guard lock(mu);
        auto owner_iter = active_owners.find(task);
        if (owner_iter == active_owners.end())
            return;

        const auto keyspace_id = owner_iter->second;
        active_owners.erase(owner_iter);
        releaseWithoutLock(keyspace_id);
        {
            auto & quota = getCPUQuotaWithoutLock(keyspace_id);
            if (cpu_quota_per_second_ns != 0)
                refillCPUQuotaWithoutLock(quota);
            updateMetricsWithoutLock(keyspace_id, quota);
        }
        ++change_id;
        cv.notify_all();
    }

    // Kept for direct limiter tests and callers that do not have a task owner.
    void release(KeyspaceID keyspace_id)
    {
        if (!isEnabled())
            return;

        std::lock_guard lock(mu);
        releaseWithoutLock(keyspace_id);
        updateMetricsWithoutLock(keyspace_id, getCPUQuotaWithoutLock(keyspace_id));
        ++change_id;
        cv.notify_all();
    }

    /// Return a generation that changes whenever a waiter may make progress.
    UInt64 getChangeId() const
    {
        std::lock_guard lock(mu);
        return change_id;
    }

    /// Wait for a release or a queue submission notification. The generation
    /// avoids a lost wakeup between checking the queues and starting to wait.
    void waitForChange(UInt64 previous_change_id)
    {
        if (!isEnabled())
            return;

        std::unique_lock lock(mu);
        cv.wait(lock, [&] { return change_id != previous_change_id; });
    }

    bool waitForChange(UInt64 previous_change_id, std::chrono::milliseconds timeout)
    {
        if (!isEnabled())
            return true;

        std::unique_lock lock(mu);
        return cv.wait_for(lock, timeout, [&] { return change_id != previous_change_id; });
    }

    void notifyAll()
    {
        if (!isEnabled())
            return;

        std::lock_guard lock(mu);
        ++change_id;
        cv.notify_all();
    }

private:
    void releaseWithoutLock(KeyspaceID keyspace_id)
    {
        auto iter = active_tasks.find(keyspace_id);
        if (iter == active_tasks.end())
            return;
        if (iter->second > 1)
            --iter->second;
        else
            active_tasks.erase(iter);
    }

private:
    const size_t max_active_tasks;
    const UInt64 cpu_quota_per_second_ns;
    const UInt64 cpu_quota_burst_ns;
    mutable std::mutex mu;
    std::condition_variable cv;
    UInt64 change_id = 0;
    std::unordered_map<KeyspaceID, size_t> active_tasks;
    std::unordered_map<const Task *, KeyspaceID> active_owners;

    struct CPUQuota
    {
        double tokens_ns = 0;
        std::chrono::steady_clock::time_point last_refill;
        TiFlashMetrics::KeyspaceCpuLimiterMetrics * metrics = nullptr;
    };

    CPUQuota & getCPUQuotaWithoutLock(KeyspaceID keyspace_id)
    {
        auto [iter, inserted] = cpu_quotas.try_emplace(keyspace_id);
        auto & quota = iter->second;
        if (inserted)
        {
            quota.tokens_ns = static_cast<double>(cpu_quota_burst_ns);
            quota.last_refill = std::chrono::steady_clock::now();
            quota.metrics = &TiFlashMetrics::instance().getKeyspaceCpuLimiterMetrics(keyspace_id);
        }
        return quota;
    }

    void refillCPUQuotaWithoutLock(CPUQuota & quota)
    {
        const auto now = std::chrono::steady_clock::now();
        const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - quota.last_refill).count();
        if (elapsed_ns <= 0)
            return;

        quota.tokens_ns = std::min(
            static_cast<double>(cpu_quota_burst_ns),
            quota.tokens_ns
                + static_cast<double>(elapsed_ns) * static_cast<double>(cpu_quota_per_second_ns)
                    / static_cast<double>(std::chrono::seconds(1).count() * 1'000'000'000ULL));
        quota.last_refill = now;
    }

    void updateMetricsWithoutLock(KeyspaceID keyspace_id, CPUQuota & quota)
    {
        const auto active_iter = active_tasks.find(keyspace_id);
        const auto active = active_iter == active_tasks.end() ? 0 : active_iter->second;
        quota.metrics->active_tasks->Set(active);
        quota.metrics->max_active_tasks->Set(max_active_tasks);
        quota.metrics->cpu_tokens_seconds->Set(quota.tokens_ns / 1'000'000'000.0);
        quota.metrics->cpu_quota_seconds_per_second->Set(cpu_quota_per_second_ns / 1'000'000'000.0);
        quota.metrics->throttled->Set(
            (cpu_quota_per_second_ns != 0 && quota.tokens_ns <= 0)
                    || (max_active_tasks != 0 && active >= max_active_tasks)
                ? 1
                : 0);
    }

    std::unordered_map<KeyspaceID, CPUQuota> cpu_quotas;
};

using KeyspaceCpuLimiterPtr = std::shared_ptr<KeyspaceCpuLimiter>;
} // namespace DB
