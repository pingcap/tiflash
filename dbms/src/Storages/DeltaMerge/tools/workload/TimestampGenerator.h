#pragma once

#include <Common/Stopwatch.h>

#include <atomic>

namespace DB::DM::tests
{
class TimestampGenerator
{
public:
    TimestampGenerator()
        : t(StopWatchDetail::nanoseconds(CLOCK_MONOTONIC))
    {}

    std::vector<uint64_t> get(int count)
    {
        uint64_t start = t.fetch_add(count, std::memory_order_relaxed);
        std::vector<uint64_t> v(count);
        for (int i = 0; i < count; i++)
        {
            v[i] = start + i;
        }
        return v;
    }

    uint64_t get() { return t.fetch_add(1, std::memory_order_relaxed); }

private:
    std::atomic<uint64_t> t;
};
} // namespace DB::DM::tests