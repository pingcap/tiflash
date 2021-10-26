#pragma once
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <PSStressEnv.h>
#include <Poco/Timer.h>

class PSMetricsDumper
{
public:
    explicit PSMetricsDumper(size_t status_interval_)
        : status_interval(status_interval_)
    {
#define REGISTER_METRICS(metric, desc) \
    metrics.insert(metrics.end(), std::pair<CurrentMetrics::Metric, MetricInfo>(metric, {desc}));
        REGISTER_METRICS(CurrentMetrics::MemoryTracking, "Memory");
        REGISTER_METRICS(CurrentMetrics::PSMVCCSnapshotsList, "SnapshotsList");

#undef REGISTER_METRICS
        timer_status.setStartInterval(1000);
        timer_status.setPeriodicInterval(status_interval * 1000);
    }

    void onTime(Poco::Timer & timer);

    String toString() const
    {
        String str;
        for (const auto & metric : metrics)
        {
            if (likely(metric.second.loop_times != 0))
            {
                str += (metric.second.toString() + "\n");
            }
        }
        return str;
    }

    void start();

    UInt32 getMemoryPeak() const
    {
        auto info = metrics.find(CurrentMetrics::MemoryTracking);
        if (info != metrics.end())
        {
            return info->second.biggest;
        }
        throw DB::Exception("No register MemoryTracking", DB::ErrorCodes::LOGICAL_ERROR);
    }

private:
    struct MetricInfo
    {
        String name;
        UInt32 loop_times = 0;
        UInt32 lastest = 0;
        UInt32 biggest = 0;
        UInt32 summary = 0;

        String toString() const
        {
            return fmt::format(
                "{} lastest used: {}, avg used: {}, top used: {}.",
                name,
                lastest,
                loop_times == 0 ? 0 : (summary / loop_times),
                biggest);
        }
    };
    size_t status_interval = 0;
    std::map<CurrentMetrics::Metric, MetricInfo> metrics;

    Poco::Timer timer_status;
};
using PSMetricsDumperPtr = std::shared_ptr<PSMetricsDumper>;

class PSGc
{
    PSPtr ps;

public:
    explicit PSGc(const PSPtr & ps_)
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
    explicit PSScanner(const PSPtr & ps_)
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
    explicit StressTimeout(size_t timeout_s)
    {
        StressEnvStatus::getInstance().setStat(STATUS_LOOP);
        LOG_INFO(StressEnv::logger, fmt::format("Timeout: {}s", timeout_s));
        timeout_timer.setStartInterval(timeout_s * 1000);
    }

    void onTime(Poco::Timer & timer);
    void start();

private:
    Poco::Timer timeout_timer;
};
using StressTimeoutPtr = std::shared_ptr<StressTimeout>;
