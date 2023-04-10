#include <Common/ProcessCollector.h>
#include <ProcessMetrics/ProcessMetrics.h>
#include <common/logger_useful.h>
#include <Common/Logger.h>

namespace DB
{

ProcessCollector::ProcessCollector()
{
    LoggerPtr log(Logger::get(""));
    LOG_INFO(log, "gjt debug in ProcessCollector::ProcessCollector()");
    auto info = get_process_metrics();
    start_time.Set(info.start_time);
}

std::vector<prometheus::MetricFamily> ProcessCollector::Collect() const
{
    auto new_info = get_process_metrics();

    {
        // todo: need lock?
        std::lock_guard<std::mutex> lock(mu);
        auto past_cpu_total = cpu_total.Value();
        cpu_total.Increment(new_info.cpu_total - past_cpu_total);
        LoggerPtr log(Logger::get(""));
        LOG_INFO(log, "gjt debug in ProcessCollector::Collect() {} {}", std::to_string(past_cpu_total), std::to_string(cpu_total.Value()));
        vsize.Set(new_info.vsize);
        rss.Set(new_info.rss);
    }

    std::vector<prometheus::MetricFamily> familes;
    familes.reserve(4);
    familes.emplace_back(prometheus::MetricFamily{CPU_METRIC_NAME, CPU_METRIC_HELP, prometheus::MetricType::Gauge, std::vector<prometheus::ClientMetric>{cpu_total.Collect()}});
    familes.emplace_back(prometheus::MetricFamily{VSIZE_METRIC_NAME, VSIZE_METRIC_HELP, prometheus::MetricType::Gauge, std::vector<prometheus::ClientMetric>{vsize.Collect()}});
    familes.emplace_back(prometheus::MetricFamily{RSS_METRIC_NAME, RSS_METRIC_HELP, prometheus::MetricType::Gauge, std::vector<prometheus::ClientMetric>{rss.Collect()}});
    familes.emplace_back(prometheus::MetricFamily{START_TIME_METRIC_NAME, START_TIME_METRIC_HELP, prometheus::MetricType::Gauge, std::vector<prometheus::ClientMetric>{start_time.Collect()}});
    return familes;
}

} // namespace DB
