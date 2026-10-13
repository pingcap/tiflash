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
#include <atomic>
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
        std::lock_guard lock(mu);
        for (const auto & entry : cpu_quotas)
            TiFlashMetrics::instance().releaseKeyspaceCpuLimiterMetrics(entry.first);
    }

    size_t cleanupIdleQuotas(std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now())
    {
        std::lock_guard lock(mu);
        return cleanupIdleQuotasWithoutLock(now);
    }

    /// When `cpu_quota_rejected` is provided, it is set to true if this call is
    /// rejected by the CPU quota. It is intentionally not reset for successful
    /// acquires or active-task-limit rejections, so callers can accumulate the
    /// reason across a candidate scan.
    bool tryAcquire(KeyspaceID keyspace_id, bool * cpu_quota_rejected = nullptr)
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
                if (cpu_quota_rejected)
                    *cpu_quota_rejected = true;
                // Remember that a waiter may be parked on this keyspace so the
                // next release that makes it admittable wakes it.
                quota.waiter_blocked = true;
                quota.metrics->cpu_quota_throttled_total->Increment();
                updateMetricsWithoutLock(keyspace_id, quota);
                return false;
            }
        }

        auto & active = active_tasks[keyspace_id];
        if (max_active_tasks != 0 && active >= max_active_tasks)
        {
            quota.waiter_blocked = true;
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
    /// is not delayed. Returns `milliseconds::max()` only when CPU quota is
    /// disabled or nothing is depleted and no CPU quota rejection was observed.
    /// A rejection keeps the wait bounded even if natural refill is observed
    /// before this method runs, since refill does not update `change_id`.
    std::chrono::milliseconds getRefillWaitDuration(bool cpu_quota_rejected = false) const
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
        {
            // A quota rejection may have happened just before this method
            // reacquired the limiter lock. Natural refill does not update
            // change_id, so keep a bounded retry in that case.
            return cpu_quota_rejected ? std::chrono::milliseconds(1) : std::chrono::milliseconds::max();
        }

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
        auto & quota = getCPUQuotaWithoutLock(keyspace_id);
        quota.last_activity = std::chrono::steady_clock::now();
        if (cpu_quota_per_second_ns != 0)
            refillCPUQuotaWithoutLock(quota);
        // Only wake rejected waiters when an active slot becomes available.
        // They must re-check CPU quota even if it is still depleted.
        const bool woken = shouldWakeWaiter(keyspace_id, quota);
        updateMetricsWithoutLock(keyspace_id, quota);
        if (woken)
            wakeWaiters();
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
        const bool woken = shouldWakeWaiter(keyspace_id, quota);
        updateMetricsWithoutLock(keyspace_id, quota);
        if (woken)
            wakeWaiters();
    }

    /// Return a generation that changes whenever a waiter may make progress.
    /// Lock free so the waiter path does not contend on `mu` on every loop.
    UInt64 getChangeId() const { return change_id.load(std::memory_order_acquire); }

    /// Bump the generation and wake waiters.
    ///
    /// Called when a task becomes available (submit/cancel/finish) and by
    /// `release()` when a keyspace can admit a waiter again. The generation is
    /// bumped under `waiter_mu` so it cannot slip between a waiter re-checking
    /// its predicate and blocking; `getChangeId()` stays lock free because the
    /// generation is atomic.
    void wakeWaiters()
    {
        if (!isEnabled())
            return;
        bool has_waiters = false;
        {
            std::lock_guard lock(waiter_mu);
            change_id.fetch_add(1, std::memory_order_acq_rel);
            has_waiters = waiter_count != 0;
        }
        // Skipping the broadcast when nobody is parked turns the common
        // submit/release path (which runs once per task round) into a cheap
        // lock-and-return instead of a futex wake.
        if (has_waiters)
            cv.notify_all();
    }

    /// Wait for a release or a queue submission notification. The generation
    /// avoids a lost wakeup between checking the queues and starting to wait.
    void waitForChange(UInt64 previous_change_id)
    {
        waitForChange(previous_change_id, std::chrono::milliseconds::max());
    }

    bool waitForChange(UInt64 previous_change_id, std::chrono::milliseconds timeout)
    {
        if (!isEnabled())
            return true;

        std::unique_lock lock(waiter_mu);
        ++waiter_count;
        bool changed = true;
        if (timeout == std::chrono::milliseconds::max())
        {
            cv.wait(lock, [&] { return change_id.load(std::memory_order_acquire) != previous_change_id; });
        }
        else
        {
            changed = cv.wait_for(lock, timeout, [&] {
                return change_id.load(std::memory_order_acquire) != previous_change_id;
            });
        }
        --waiter_count;
        return changed;
    }

private:
    /// Per-keyspace token bucket state. Declared before the methods that take
    /// it as a parameter, since a member function's parameter list is not part
    /// of the complete-class context.
    struct CPUQuota
    {
        double tokens_ns = 0;
        std::chrono::steady_clock::time_point last_refill;
        std::chrono::steady_clock::time_point last_activity;
        TiFlashMetrics::KeyspaceCpuLimiterMetrics * metrics = nullptr;
        /// Set when a waiter is rejected for this keyspace, cleared when a
        /// release wakes it. Gates the release-driven broadcast.
        bool waiter_blocked = false;
    };

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

    /// Whether a just-released keyspace has an active slot for a waiter that
    /// was previously rejected.
    ///
    /// A broadcast is only useful when some thread actually observed this
    /// keyspace as non-admittable (it set `waiter_blocked` on rejection) and
    /// the release made an active slot available. Waking on every release would
    /// flood all pool threads even when the keyspace was never throttled, which
    /// is the common case. The flag is cleared here so a single rejection does
    /// not turn every later release into a broadcast. Must be called with `mu`
    /// held.
    bool shouldWakeWaiter(KeyspaceID keyspace_id, CPUQuota & quota)
    {
        if (!quota.waiter_blocked)
            return false;
        const bool pool_admittable = max_active_tasks == 0 || getActiveWithoutLock(keyspace_id) < max_active_tasks;
        if (!pool_admittable)
            return false;
        // A waiter rejected by the active-task limit while CPU tokens were
        // positive may have chosen an unbounded wait. Wake it even if the
        // completing task depleted the bucket so it can arm a refill timeout.
        quota.waiter_blocked = false;
        return true;
    }

    size_t getActiveWithoutLock(KeyspaceID keyspace_id) const
    {
        const auto iter = active_tasks.find(keyspace_id);
        return iter == active_tasks.end() ? 0 : iter->second;
    }

private:
    const size_t max_active_tasks;
    const UInt64 cpu_quota_per_second_ns;
    const UInt64 cpu_quota_burst_ns;
    /// Upper bound on a refill wait, so a waiter still re-checks occasionally.
    static constexpr std::chrono::milliseconds max_refill_wait{1000};
    mutable std::mutex mu;
    /// Separate lock for the waiter condition variable so broadcasts and
    /// getChangeId() do not contend with the accounting in `mu`.
    mutable std::mutex waiter_mu;
    std::condition_variable cv;
    std::atomic<UInt64> change_id{0};
    /// Number of threads currently parked on `cv`, guarded by `waiter_mu`.
    /// Lets `wakeWaiters()` skip the broadcast when there is nobody to wake.
    size_t waiter_count = 0;
    std::unordered_map<KeyspaceID, size_t> active_tasks;
    std::unordered_map<const Task *, KeyspaceID> active_owners;

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
            quota.metrics->max_active_tasks->Set(max_active_tasks);
            quota.metrics->cpu_quota_seconds_per_second->Set(cpu_quota_per_second_ns / 1'000'000'000.0);
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
        // `max_active_tasks` and `cpu_quota_seconds_per_second` are constant for
        // the lifetime of the limiter, so they are published once when the quota
        // is created instead of on every hot-path update.
        const auto active = getActiveWithoutLock(keyspace_id);
        quota.metrics->active_tasks->Set(active);
        quota.metrics->cpu_tokens_seconds->Set(quota.tokens_ns / 1'000'000'000.0);
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
