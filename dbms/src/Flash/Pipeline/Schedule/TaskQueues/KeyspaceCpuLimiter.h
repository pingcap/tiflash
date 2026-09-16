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
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
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
    {
        if (isEnabled())
            cleanup_thread = std::thread([this] {
                std::unique_lock lock(mu);
                while (!cleanup_cv.wait_for(lock, std::chrono::minutes(1), [this] { return stopping; }))
                    cleanupIdleQuotasWithoutLock(std::chrono::steady_clock::now());
            });
    }

    ~KeyspaceCpuLimiter()
    {
        {
            std::lock_guard lock(mu);
            stopping = true;
        }
        cleanup_cv.notify_all();
        if (cleanup_thread.joinable())
            cleanup_thread.join();
        for (const auto & entry : cpu_quotas)
            TiFlashMetrics::instance().releaseKeyspaceCpuLimiterMetrics(entry.first);
    }

    size_t cleanupIdleQuotas(std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now())
    {
        std::lock_guard lock(mu);
        return cleanupIdleQuotasWithoutLock(now);
    }

    bool tryAcquire(KeyspaceID keyspace_id)
    {
        if (!isEnabled())
            return true;

        std::lock_guard lock(mu);
        auto & quota = getCPUQuotaWithoutLock(keyspace_id);
        quota.last_activity = std::chrono::steady_clock::now();
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

    bool isCPUQuotaEnabled() const { return cpu_quota_per_second_ns != 0; }

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
        quota.last_activity = std::chrono::steady_clock::now();
        refillCPUQuotaWithoutLock(quota);
        quota.tokens_ns -= static_cast<double>(cpu_time_ns);
        quota.metrics->cpu_seconds_total->Increment(static_cast<double>(cpu_time_ns) / 1'000'000'000.0);
        updateMetricsWithoutLock(owner_iter->second, quota);
        return quota.tokens_ns <= 0;
    }

    /// How long a waiter should sleep before it re-checks a CPU-quota rejection.
    ///
    /// Token refill is time based, so a queue with only throttled work must
    /// retry even when no task is submitted or completed. Instead of polling at
    /// a fixed interval, wait until the earliest moment a depleted bucket turns
    /// positive again, so a waiter is not woken before it can make progress.
    /// Waiters still wake on `change_id` updates, so a release or a submission
    /// is not delayed. Returns `milliseconds::max()` when CPU quota is disabled
    /// or nothing is depleted, which the waiters treat as an unbounded wait.
    std::chrono::milliseconds getRefillWaitDuration() const
    {
        if (cpu_quota_per_second_ns == 0)
            return std::chrono::milliseconds::max();

        const auto nanoseconds_per_second = static_cast<double>(std::chrono::seconds(1).count() * 1'000'000'000ULL);
        const double quota_ns_per_ns = static_cast<double>(cpu_quota_per_second_ns) / nanoseconds_per_second;
        std::lock_guard lock(mu);
        const auto now = std::chrono::steady_clock::now();
        double min_wait_ms = static_cast<double>(max_refill_wait.count());
        bool has_depleted_quota = false;
        for (const auto & quota_entry : cpu_quotas)
        {
            const auto & quota = quota_entry.second;
            // Tokens refill linearly, so the bucket turns positive at
            // `last_refill + -tokens / rate`. Recompute the balance instead of
            // mutating the bucket: a keyspace that has been idle long enough to
            // pay back its deficit is not runnable work, and letting it report a
            // zero wait would keep the poll interval short forever.
            const auto elapsed_ns = static_cast<double>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(now - quota.last_refill).count());
            const double remaining_tokens_ns = quota.tokens_ns + elapsed_ns * quota_ns_per_ns;
            if (remaining_tokens_ns > 0)
                continue;

            has_depleted_quota = true;
            min_wait_ms = std::min(min_wait_ms, -remaining_tokens_ns / quota_ns_per_ns / 1'000'000.0);
        }
        if (!has_depleted_quota)
            return std::chrono::milliseconds::max();

        // Keep a small floor so a waiter with no other wakeup still re-checks.
        const double clamped_wait_ms
            = std::min(std::max(min_wait_ms, 1.0), static_cast<double>(max_refill_wait.count()));
        return std::chrono::milliseconds(static_cast<std::chrono::milliseconds::rep>(std::ceil(clamped_wait_ms)));
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
            quota.last_activity = std::chrono::steady_clock::now();
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
        if (active_tasks.find(keyspace_id) == active_tasks.end())
            return;
        releaseWithoutLock(keyspace_id);
        auto & quota = getCPUQuotaWithoutLock(keyspace_id);
        quota.last_activity = std::chrono::steady_clock::now();
        updateMetricsWithoutLock(keyspace_id, quota);
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
        if (timeout == std::chrono::milliseconds::max())
        {
            cv.wait(lock, [&] { return change_id != previous_change_id; });
            return true;
        }
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
    /// Upper bound on a refill wait, so a waiter still re-checks occasionally.
    static constexpr std::chrono::milliseconds max_refill_wait{1000};
    mutable std::mutex mu;
    std::condition_variable cv;
    UInt64 change_id = 0;
    std::unordered_map<KeyspaceID, size_t> active_tasks;
    std::unordered_map<const Task *, KeyspaceID> active_owners;

    struct CPUQuota
    {
        double tokens_ns = 0;
        std::chrono::steady_clock::time_point last_refill;
        std::chrono::steady_clock::time_point last_activity;
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
            quota.last_activity = quota.last_refill;
            quota.metrics = &TiFlashMetrics::instance().acquireKeyspaceCpuLimiterMetrics(keyspace_id);
        }
        return quota;
    }

    void refillCPUQuotaWithoutLock(
        CPUQuota & quota,
        std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now())
    {
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
    std::condition_variable cleanup_cv;
    bool stopping = false;
    std::thread cleanup_thread;

    size_t cleanupIdleQuotasWithoutLock(std::chrono::steady_clock::time_point now)
    {
        size_t removed = 0;
        for (auto iter = cpu_quotas.begin(); iter != cpu_quotas.end();)
        {
            auto & quota = iter->second;
            if (active_tasks.find(iter->first) != active_tasks.end()
                || now - quota.last_activity < std::chrono::minutes(60))
            {
                ++iter;
                continue;
            }
            refillCPUQuotaWithoutLock(quota, now);
            // Recreating a bucket must neither forgive CPU debt nor grant extra tokens.
            if (quota.tokens_ns < static_cast<double>(cpu_quota_burst_ns))
            {
                ++iter;
                continue;
            }
            TiFlashMetrics::instance().releaseKeyspaceCpuLimiterMetrics(iter->first);
            iter = cpu_quotas.erase(iter);
            ++removed;
        }
        return removed;
    }
};

using KeyspaceCpuLimiterPtr = std::shared_ptr<KeyspaceCpuLimiter>;
} // namespace DB
