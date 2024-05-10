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

#include <Common/ProcessCollector.h>
#include <ProcessMetrics/ProcessMetrics.h>

namespace DB
{

std::vector<prometheus::MetricFamily> ProcessCollector::Collect() const
{
    auto new_info = get_process_metrics();

    std::vector<prometheus::MetricFamily> familes;

    // The following metrics shadow TiFlash proxy metrics, so that we ensure these metrics are available
    // in disaggregated mode, where TiFlash proxy may not start at all.
    // Note that, even in non-disaggregated mode, duplicates are fine when being collected by Prometheus,
    // because TiFlash proxy and TiFlash have different metrics endpoints. However we will see multiple
    // endpoints in the Grafana, because both TiFlash proxy and TiFlash uses the same metric name.
    // To avoid duplicates in Grafana, we will only include proxy metrics when proxy is not enabled.
    if (include_proxy_metrics)
    {
        familes.emplace_back(prometheus::MetricFamily{
            "tiflash_proxy_process_cpu_seconds_total",
            "Total user and system CPU time spent in seconds.",
            prometheus::MetricType::Gauge,
            {
                prometheus::ClientMetric{.gauge = {static_cast<double>(new_info.cpu_total)}},
            }});

        familes.emplace_back(prometheus::MetricFamily{
            "tiflash_proxy_process_virtual_memory_bytes",
            "Virtual memory size in bytes.",
            prometheus::MetricType::Gauge,
            {
                prometheus::ClientMetric{.gauge = {static_cast<double>(new_info.vsize)}},
            }});
        familes.emplace_back(prometheus::MetricFamily{
            "tiflash_proxy_process_resident_memory_bytes",
            "Resident memory size in bytes.",
            prometheus::MetricType::Gauge,
            {
                prometheus::ClientMetric{.gauge = {static_cast<double>(new_info.rss)}},
            }});
        familes.emplace_back(prometheus::MetricFamily{
            "tiflash_proxy_process_start_time_seconds",
            "Start time of the process since unix epoch in seconds.",
            prometheus::MetricType::Gauge,
            std::vector<prometheus::ClientMetric>{
                prometheus::ClientMetric{.gauge = {static_cast<double>(new_info.start_time)}},
            }});
    }

    // The following metrics are TiFlash specific process metrics.
    familes.emplace_back(prometheus::MetricFamily{
        "tiflash_process_rss_by_type_bytes",
        "Resident memory size by type in bytes.",
        prometheus::MetricType::Gauge,
        {
            prometheus::ClientMetric{
                .label = {{"type", "anon"}},
                .gauge = {static_cast<double>(new_info.rss_anon)},
            },
            prometheus::ClientMetric{
                .label = {{"type", "file"}},
                .gauge = {static_cast<double>(new_info.rss_file)},
            },
            prometheus::ClientMetric{
                .label = {{"type", "shared"}},
                .gauge = {static_cast<double>(new_info.rss_shared)},
            },
        }});

    return familes;
}

} // namespace DB
