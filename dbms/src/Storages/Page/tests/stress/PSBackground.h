#pragma once
#include <Common/Stopwatch.h>
#include <PSStressEnv.h>
#include <Poco/Timer.h>

class PSMetricsDumper
{
public:
    PSMetricsDumper(size_t status_interval_)
        : status_interval(status_interval_)
    {
        timer_status.setStartInterval(1000);
        timer_status.setPeriodicInterval(status_interval * 1000);
    };

    void onTime(Poco::Timer & timer);

    String toString()
    {
        return fmt::format(
            "Memory lastest used : {} , avg used : {} , top used {}.",
            lastest_memory,
            loop_times == 0 ? 0 : (memory_summary / loop_times),
            memory_biggest);
    }

    void start();

    UInt32 getMemoryPeak()
    {
        return memory_biggest;
    }

private:
    size_t status_interval = 0;
    UInt32 loop_times = 0;
    UInt32 memory_summary = 0;
    UInt32 memory_biggest = 0;
    UInt32 lastest_memory = 0;

    Poco::Timer timer_status;
};
using PSMetricsDumperPtr = std::shared_ptr<PSMetricsDumper>;

class PSGc
{
    PSPtr ps;

public:
    PSGc(const PSPtr & ps_)
        : ps(ps_)
    {
        assert(ps != nullptr);
        gc_timer.setStartInterval(1000);
        gc_timer.setPeriodicInterval(30 * 1000);
    }

    void doGcOnce();

    void onTime(Poco::Timer & /* t */) { doGcOnce(); }

    void start();

    UInt64 getElapsedMilliseconds()
    {
        return gc_stop_watch.elapsedMilliseconds();
    }

private:
    Poco::Timer gc_timer;
    Stopwatch gc_stop_watch;
};
using PSGcPtr = std::shared_ptr<PSGc>;

class PSScanner
{
    PSPtr ps;

public:
    PSScanner(const PSPtr & ps_)
        : ps(ps_)
    {
        assert(ps != nullptr);

        scanner_timer.setStartInterval(1000);
        scanner_timer.setPeriodicInterval(30 * 1000);
    }

    void onTime(Poco::Timer & timer);

    void start();

private:
    Poco::Timer scanner_timer;
};
using PSScannerPtr = std::shared_ptr<PSScanner>;

class StressTimeout
{
public:
    StressTimeout(size_t timeout_s)
    {
        StressEnvStatus::getInstance().setStat(STATUS_LOOP);
        LOG_INFO(StressEnv::logger, fmt::format("Timeout: {}s", timeout_s));
        timeout_timer.setStartInterval(timeout_s * 1000);
    };

    void onTime(Poco::Timer & timer);
    void start();

private:
    Poco::Timer timeout_timer;
};
using StressTimeoutPtr = std::shared_ptr<StressTimeout>;