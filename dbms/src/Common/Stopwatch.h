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

#include <common/defines.h>
#include <common/types.h>
#include <time.h>

#include <atomic>

#ifdef __APPLE__
#include <common/apple_rt.h>
#endif

constexpr UInt64 SECOND_TO_NANO = 1000000000ULL;
constexpr UInt64 MILLISECOND_TO_NANO = 1000000UL;
constexpr UInt64 MICROSECOND_TO_NANO = 1000UL;

inline UInt64 clock_gettime_ns(clockid_t clock_type = CLOCK_MONOTONIC)
{
    struct timespec ts
    {
    };
    clock_gettime(clock_type, &ts);
    return static_cast<UInt64>(ts.tv_sec * SECOND_TO_NANO + ts.tv_nsec);
}

/// Sometimes monotonic clock may not be monotonic (due to bug in kernel?).
/// It may cause some operations to fail with "Timeout exceeded: elapsed 18446744073.709553 seconds".
/// Takes previously returned value and returns it again if time stepped back for some reason.
inline UInt64 clock_gettime_ns_adjusted(UInt64 prev_time, clockid_t clock_type = CLOCK_MONOTONIC)
{
    UInt64 current_time = clock_gettime_ns(clock_type);
    if (likely(prev_time <= current_time))
        return current_time;

    /// Something probably went completely wrong if time stepped back for more than 1 second.
    assert(prev_time - current_time <= SECOND_TO_NANO);
    return prev_time;
}

/** Differs from Poco::Stopwatch only by using 'clock_gettime' instead of 'gettimeofday',
  *  returns nanoseconds instead of microseconds, and also by other minor differencies.
  */
class Stopwatch
{
public:
    /** CLOCK_MONOTONIC works relatively efficient (~15 million calls/sec) and doesn't lead to syscall.
      * Pass CLOCK_MONOTONIC_COARSE, if you need better performance with acceptable cost of several milliseconds of inaccuracy.
      */
    explicit Stopwatch(clockid_t clock_type_ = CLOCK_MONOTONIC)
        : clock_type(clock_type_)
    {
        start();
    }

    void start()
    {
        start_ns = nanosecondsWithBound(start_ns);
        last_ns = start_ns;
        is_running = true;
    }

    void stop()
    {
        stop_ns = nanosecondsWithBound(start_ns);
        is_running = false;
    }

    void reset()
    {
        start_ns = 0;
        stop_ns = 0;
        last_ns = 0;
        is_running = false;
    }

    void restart() { start(); }

    UInt64 elapsed() const { return is_running ? nanosecondsWithBound(start_ns) - start_ns : stop_ns - start_ns; }
    UInt64 elapsedMilliseconds() const { return elapsed() / MILLISECOND_TO_NANO; }
    double elapsedSeconds() const { return static_cast<double>(elapsed()) / SECOND_TO_NANO; }

    UInt64 elapsedFromLastTime()
    {
        const auto now_ns = nanosecondsWithBound(last_ns);
        if (is_running)
        {
            auto rc = now_ns - last_ns;
            last_ns = now_ns;
            return rc;
        }
        else
        {
            return stop_ns - last_ns;
        }
    }

    UInt64 elapsedMillisecondsFromLastTime() { return elapsedFromLastTime() / MILLISECOND_TO_NANO; }
    double elapsedSecondsFromLastTime() { return static_cast<double>(elapsedFromLastTime()) / SECOND_TO_NANO; }

private:
    UInt64 start_ns = 0;
    UInt64 stop_ns = 0;
    UInt64 last_ns = 0;
    clockid_t clock_type;
    bool is_running = false;

    // Get current nano seconds, ensuring the return value is not
    // less than `lower_bound`.
    UInt64 nanosecondsWithBound(UInt64 lower_bound) const { return clock_gettime_ns_adjusted(lower_bound, clock_type); }
};


class AtomicStopwatch
{
public:
    explicit AtomicStopwatch(clockid_t clock_type_ = CLOCK_MONOTONIC)
        : start_ns(0)
        , clock_type(clock_type_)
    {
        restart();
    }

    void restart() { start_ns = nanoseconds(0); }
    UInt64 elapsed() const
    {
        UInt64 current_start_ns = start_ns;
        return nanoseconds(current_start_ns) - start_ns;
    }
    UInt64 elapsedMilliseconds() const { return elapsed() / 1000000UL; }
    double elapsedSeconds() const { return static_cast<double>(elapsed()) / 1000000000ULL; }

    /** If specified amount of time has passed, then restarts timer and returns true.
      * Otherwise returns false.
      * This is done atomically.
      */
    bool compareAndRestart(double seconds)
    {
        UInt64 threshold = seconds * 1000000000ULL;
        UInt64 current_start_ns = start_ns;
        UInt64 current_ns = nanoseconds(current_start_ns);

        while (true)
        {
            if (current_ns < current_start_ns + threshold)
                return false;

            if (start_ns.compare_exchange_weak(current_start_ns, current_ns))
                return true;
        }
    }

    struct Lock
    {
        AtomicStopwatch * parent = nullptr;

        Lock() = default;

        explicit operator bool() const { return parent != nullptr; }

        explicit Lock(AtomicStopwatch * parent)
            : parent(parent)
        {}

        Lock(Lock &&) = default;

        ~Lock()
        {
            if (parent)
                parent->restart();
        }
    };

    /** If specified amount of time has passed and timer is not locked right now, then returns Lock object,
      *  which locks timer and, on destruction, restarts timer and releases the lock.
      * Otherwise returns object, that is implicitly casting to false.
      * This is done atomically.
      *
      * Usage:
      * if (auto lock = timer.compareAndRestartDeferred(1))
      *        /// do some work, that must be done in one thread and not more frequently than each second.
      */
    Lock compareAndRestartDeferred(double seconds)
    {
        UInt64 threshold = seconds * 1000000000ULL;
        UInt64 current_start_ns = start_ns;
        UInt64 current_ns = nanoseconds(current_start_ns);

        while (true)
        {
            if ((current_start_ns & 0x8000000000000000ULL))
                return {};

            if (current_ns < current_start_ns + threshold)
                return {};

            if (start_ns.compare_exchange_weak(current_start_ns, current_ns | 0x8000000000000000ULL))
                return Lock(this);
        }
    }

private:
    std::atomic<UInt64> start_ns;
    std::atomic<bool> lock{false};
    clockid_t clock_type;

    /// Most significant bit is a lock. When it is set, compareAndRestartDeferred method will return false.
    UInt64 nanoseconds(UInt64 prev_time) const
    {
        return clock_gettime_ns_adjusted(prev_time, clock_type) & 0x7FFFFFFFFFFFFFFFULL;
    }
};
