// Copyright 2023 PingCAP, Ltd.
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

#include <ProcessMetrics/ProcessMetrics.h>
#include <prometheus/counter.h>
#include <prometheus/metric_family.h>
#include <prometheus/registry.h>

namespace DB
{

// Why not use async_metrics for cpu/mem metric:
// 1. ProcessCollector will collect cpu/mem metric when ProcessCollector::Collect() is called, so it's synchronous.
//    Just like the original tiflash-proxy logic.
// 2. Current implentation of async_metrics interval is 15s, it's too large. And this interval also affect pushgateway interval.
//    So better not to mix cpu/mem metrics with async_metrics.
// The difference between ProcessCollector and prometheus::Registry:
// 1. ProcessCollector will **update** Gauge then collect. prometheus::Registry only collect Gauge.
class ProcessCollector : public prometheus::Collectable
{
public:
    static constexpr auto CPU_METRIC_NAME = "tiflash_proxy_process_cpu_seconds_total";
    static constexpr auto CPU_METRIC_HELP = "Total user and system CPU time spent in seconds.";
    static constexpr auto VSIZE_METRIC_NAME = "tiflash_proxy_process_virtual_memory_bytes";
    static constexpr auto VSIZE_METRIC_HELP = "Virtual memory size in bytes.";
    static constexpr auto RSS_METRIC_NAME = "tiflash_proxy_process_resident_memory_bytes";
    static constexpr auto RSS_METRIC_HELP = "Resident memory size in bytes.";
    static constexpr auto START_TIME_METRIC_NAME = "tiflash_proxy_process_start_time_seconds";
    static constexpr auto START_TIME_METRIC_HELP = "Start time of the process since unix epoch in seconds.";

    ProcessCollector();

    std::vector<prometheus::MetricFamily> Collect() const override;

private:
    mutable prometheus::Gauge cpu_total;
    mutable prometheus::Gauge vsize;
    mutable prometheus::Gauge rss;
    prometheus::Gauge start_time;
};
} // namespace DB
