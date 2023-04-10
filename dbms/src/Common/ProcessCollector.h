#pragma once

#include <prometheus/counter.h>
#include <prometheus/registry.h>
#include <prometheus/metric_family.h>
#include <ProcessMetrics/ProcessMetrics.h>

#include <mutex>

namespace DB
{

class ProcessCollector : public prometheus::Collectable
{
public:
    static const size_t INFO_SIZE = 4;
    static constexpr auto CPU_METRIC_NAME = "tiflash_process_cpu_seconds_total";
    static constexpr auto CPU_METRIC_HELP = "Total user and system CPU time spent in seconds.";
    static constexpr auto VSIZE_METRIC_NAME = "tiflash_process_virtual_memory_bytes";
    static constexpr auto VSIZE_METRIC_HELP = "Virtual memory size in bytes.";
    static constexpr auto RSS_METRIC_NAME = "tiflash_process_resident_memory_bytes";
    static constexpr auto RSS_METRIC_HELP = "Resident memory size in bytes.";
    static constexpr auto START_TIME_METRIC_NAME = "tiflash_process_start_time_seconds";
    static constexpr auto START_TIME_METRIC_HELP = "Start time of the process since unix epoch in seconds.";

    ProcessCollector();

    std::vector<prometheus::MetricFamily> Collect() const override;
private:
    mutable std::mutex mu;
    mutable prometheus::Gauge cpu_total;
    mutable prometheus::Gauge vsize;
    mutable prometheus::Gauge rss;
    prometheus::Gauge start_time;
};
} // namespace DB
