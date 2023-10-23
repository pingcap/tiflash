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
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Poco/Timer.h>
#include <Storages/Page/workload/PSStressEnv.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Object.h>
#pragma GCC diagnostic pop

namespace CurrentMetrics
{
extern const Metric PSMVCCSnapshotsList;
}

namespace DB::PS::tests
{
class PSMetricsDumper
{
public:
    explicit PSMetricsDumper(size_t status_interval_, const LoggerPtr & log)
        : status_interval(status_interval_)
        , logger(log)
    {
#define REGISTER_METRICS(metric, desc) \
    metrics.insert(metrics.end(), std::pair<CurrentMetrics::Metric, MetricInfo>(metric, {desc}));
        REGISTER_METRICS(CurrentMetrics::MemoryTracking, "memory");
        REGISTER_METRICS(CurrentMetrics::PSMVCCSnapshotsList, "snapshots");

#undef REGISTER_METRICS
        timer_status.setStartInterval(1000);
        timer_status.setPeriodicInterval(status_interval * 1000);
    }

    void onTime(Poco::Timer & timer);

    void addJSONSummaryTo(Poco::JSON::Object::Ptr & root) const;

    void start();

    void stop() { timer_status.stop(); }

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
        UInt32 latest = 0;
        UInt32 biggest = 0;
        UInt32 summary = 0;

        String toString() const
        {
            return fmt::format(
                "{} lastest used: {}, avg used: {}, top used: {}.",
                name,
                latest,
                loop_times == 0 ? 0 : (summary / loop_times),
                biggest);
        }
    };
    size_t status_interval = 0;
    std::map<CurrentMetrics::Metric, MetricInfo> metrics;

    Poco::Timer timer_status;

    LoggerPtr logger;
};
using PSMetricsDumperPtr = std::shared_ptr<PSMetricsDumper>;

class PSGc
{
    PSPtr ps;

public:
    explicit PSGc(const PSPtr & ps_, uint64_t interval)
        : ps(ps_)
    {
        assert(ps != nullptr);
        gc_timer.setStartInterval(1000);
        gc_timer.setPeriodicInterval(interval * 1000);
    }

    void doGcOnce();

    void onTime(Poco::Timer & /* t */) { doGcOnce(); }

    void start();

    void stop() { gc_timer.stop(); }

    UInt64 getElapsedMilliseconds() { return gc_stop_watch.elapsedMilliseconds(); }

private:
    Poco::Timer gc_timer;
    Stopwatch gc_stop_watch;
};
using PSGcPtr = std::shared_ptr<PSGc>;

class PSSnapStatGetter
{
    PSPtr ps;

public:
    PSSnapStatGetter(const PSPtr & ps_, const LoggerPtr & log)
        : ps(ps_)
        , logger(log)
    {
        assert(ps != nullptr);

        scanner_timer.setStartInterval(1000);
        scanner_timer.setPeriodicInterval(30 * 1000);
    }

    void onTime(Poco::Timer & timer);

    void start();

    void stop() { scanner_timer.stop(); }

private:
    Poco::Timer scanner_timer;
    LoggerPtr logger;
};
using PSSnapStatGetterPtr = std::shared_ptr<PSSnapStatGetter>;

class StressTimeout
{
public:
    StressTimeout(size_t timeout_s, const LoggerPtr & log)
        : logger(log)
    {
        StressEnvStatus::getInstance().setStat(STATUS_LOOP);
        LOG_INFO(logger, "Timeout: {}s", timeout_s);
        timeout_timer.setStartInterval(timeout_s * 1000);
    }

    void onTime(Poco::Timer & timer);
    void start();
    void stop() { timeout_timer.stop(); }

private:
    Poco::Timer timeout_timer;
    LoggerPtr logger;
};
using StressTimeoutPtr = std::shared_ptr<StressTimeout>;
} // namespace DB::PS::tests
