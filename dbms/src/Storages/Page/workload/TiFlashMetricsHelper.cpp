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

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/Page/workload/TiFlashMetricsHelper.h>

#include <magic_enum.hpp>
#include <unordered_set>

namespace DB::tests
{
TiFlashMetricsHelper::HistogramMap TiFlashMetricsHelper::collectHistorgrams(const std::unordered_set<String> & names)
{
    HistogramMap histograms;

    auto & tiflash_metrics = TiFlashMetrics::instance();
    auto collectable = tiflash_metrics.registry;
    auto families = collectable->Collect();
    for (const auto & fam : families)
    {
        if (!names.contains(fam.name))
            continue;
        for (const auto & m : fam.metric)
        {
            FmtBuffer fmt_buf;
            fmt_buf.joinStr(
                m.label.begin(),
                m.label.end(),
                [](const prometheus::ClientMetric::Label & lbl, FmtBuffer & fmt_buf) {
                    fmt_buf.fmtAppend("<{},{}>", lbl.name, lbl.value);
                },
                ",");
            auto str_labels = fmt_buf.toString();

            RUNTIME_CHECK_MSG(
                fam.type == prometheus::MetricType::Histogram && m.label.size() == 1,
                "name={} type={} labels={}",
                fam.name,
                magic_enum::enum_name(fam.type),
                str_labels);
            histograms[HistogramId{.name = fam.name, .type = m.label[0].value}] = m.histogram;
        }
    }

    return histograms;
}

TiFlashMetricsHelper::HistStats TiFlashMetricsHelper::histogramStats(const prometheus::ClientMetric::Histogram & hist)
{
    HistStats stats;
    stats.p99ms = 1000. * histogramQuantile(hist, 0.99);
    stats.p999ms = 1000. * histogramQuantile(hist, 0.999);
    stats.avgms = 1000. * histogramAvg(hist);
    return stats;
}

double TiFlashMetricsHelper::histogramQuantile(const prometheus::ClientMetric::Histogram & hist, double q)
{
    // Base on https://github.com/prometheus/prometheus/blob/756202aa4fc09c4fdc756a8c5b1976d709cd3939/promql/quantile.go#L154-L177
    assert(q >= 0.0);
    assert(q <= 1.0);

    if (hist.sample_count == 0)
        return -1.0;

    double rank = q * hist.sample_count;
    double lower_bound = 0.0;
    UInt64 prev_cumulative_count = 0.0;
    for (const auto & bucket : hist.bucket)
    {
        RUNTIME_CHECK(bucket.upper_bound > 0.0, bucket.upper_bound);
        if (bucket.cumulative_count >= rank)
        {
            auto count = bucket.cumulative_count - prev_cumulative_count;
            auto rank_in_bucket = rank - prev_cumulative_count;
            // linear interpolation
            return lower_bound + (bucket.upper_bound - lower_bound) * (rank_in_bucket / count);
        }
        prev_cumulative_count = bucket.cumulative_count;
        lower_bound = bucket.upper_bound;
    }
    // rank >= all count, return the last bucket's upper_bound
    return lower_bound;
}

double TiFlashMetricsHelper::histogramAvg(const prometheus::ClientMetric::Histogram & hist)
{
    if (hist.sample_count == 0)
        return 0.0;

    double sum = 0.0;
    std::optional<double> lower_bound_value;
    double bucket_lower_bound = 0.0;
    UInt64 prev_cumulative_count = 0.0;
    for (const auto & bucket : hist.bucket)
    {
        auto bucket_count = bucket.cumulative_count - prev_cumulative_count;
        if (bucket_count > 0)
        {
            if (!lower_bound_value.has_value())
                lower_bound_value = bucket.upper_bound;
            sum += (bucket_lower_bound + bucket.upper_bound) / 2 * bucket_count;
        }

        prev_cumulative_count = bucket.cumulative_count;
        bucket_lower_bound = bucket.upper_bound;
    }

    // no values among all buckets
    if (!lower_bound_value.has_value())
        return 0.0;
    // lower limited by the value we observed at least once
    return std::max(sum / hist.sample_count, *lower_bound_value);
}

} // namespace DB::tests
