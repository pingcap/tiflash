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

#include <common/types.h>
#include <prometheus/client_metric.h>
#include <prometheus/metric_type.h>

#include <unordered_set>

namespace DB::tests
{
struct TiFlashMetricsHelper
{
public:
    // "name-label" -> histogram
    struct HistogramId
    {
        String name;
        String type;
    };
    struct HistogramIdHash
    {
        size_t operator()(const HistogramId & id) const noexcept
        {
            return std::hash<String>()(id.name) ^ std::hash<String>()(id.type);
        }
    };
    struct HistogramIdEqual
    {
        size_t operator()(const HistogramId & lhs, const HistogramId & rhs) const noexcept
        {
            return lhs.name == rhs.name && lhs.type == rhs.type;
        }
    };
    using HistogramMap
        = std::unordered_map<HistogramId, prometheus::ClientMetric::Histogram, HistogramIdHash, HistogramIdEqual>;
    static HistogramMap collectHistorgrams(const std::unordered_set<String> & names);

    struct HistStats
    {
        double p99ms = 0.0;
        double p999ms = 0.0;
        double avgms = 0.0;
    };

    static HistStats histogramStats(const prometheus::ClientMetric::Histogram & hist);

    static double histogramQuantile(const prometheus::ClientMetric::Histogram & hist, double q);
    static double histogramAvg(const prometheus::ClientMetric::Histogram & hist);
};

} // namespace DB::tests
