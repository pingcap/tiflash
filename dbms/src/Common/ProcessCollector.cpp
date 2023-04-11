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

#include <Common/ProcessCollector.h>
#include <ProcessMetrics/ProcessMetrics.h>

namespace DB
{

ProcessCollector::ProcessCollector()
{
    auto info = get_process_metrics();
    start_time.Set(info.start_time);
}

std::vector<prometheus::MetricFamily> ProcessCollector::Collect() const
{
    auto new_info = get_process_metrics();

    // Gauge is thread safe, no need to lock.
    auto past_cpu_total = cpu_total.Value();
    cpu_total.Increment(new_info.cpu_total - past_cpu_total);
    vsize.Set(new_info.vsize);
    rss.Set(new_info.rss);

    std::vector<prometheus::MetricFamily> familes;
    familes.reserve(4);
    familes.emplace_back(prometheus::MetricFamily{CPU_METRIC_NAME, CPU_METRIC_HELP, prometheus::MetricType::Gauge, std::vector<prometheus::ClientMetric>{cpu_total.Collect()}});
    familes.emplace_back(prometheus::MetricFamily{VSIZE_METRIC_NAME, VSIZE_METRIC_HELP, prometheus::MetricType::Gauge, std::vector<prometheus::ClientMetric>{vsize.Collect()}});
    familes.emplace_back(prometheus::MetricFamily{RSS_METRIC_NAME, RSS_METRIC_HELP, prometheus::MetricType::Gauge, std::vector<prometheus::ClientMetric>{rss.Collect()}});
    familes.emplace_back(prometheus::MetricFamily{START_TIME_METRIC_NAME, START_TIME_METRIC_HELP, prometheus::MetricType::Gauge, std::vector<prometheus::ClientMetric>{start_time.Collect()}});
    return familes;
}

} // namespace DB
