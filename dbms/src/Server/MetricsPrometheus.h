#pragma once

#include <Common/ProfileEvents.h>
#include <Interpreters/Context.h>
#include <Poco/Util/Timer.h>
#include <Server/MetricsDefine.h>
#include <daemon/GraphiteWriter.h>
#include <prometheus/exposer.h>
#include <prometheus/gateway.h>
#include <prometheus/gauge.h>
#include <prometheus/registry.h>

#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>

namespace DB
{

class AsynchronousMetrics;
class Context;


/// Automatically sends
/// - difference of ProfileEvents;
/// - values of CurrentMetrics;
/// - values of AsynchronousMetrics;
/// to Prometheus
class MetricsPrometheus
{
public:
    MetricsPrometheus(Context & context_, const AsynchronousMetrics & async_metrics_);
    ~MetricsPrometheus();

private:
    std::shared_ptr<prometheus::Registry> registry = std::make_shared<prometheus::Registry>();

public:
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(name, help, type, n, ...) \
    MetricFamily<prometheus::type, n> name = MetricFamily<prometheus::type, n>(*registry, #name, #help, ##__VA_ARGS__);
    APPLY_FOR_METRICS(M)
#undef M

private:
    static constexpr auto profile_events_path_prefix = "tiflash_system_profile_events_";
    static constexpr auto current_metrics_path_prefix = "tiflash_system_metrics_";
    static constexpr auto asynchronous_metrics_path_prefix = "tiflash_system_asynchronous_metrics_";

    static constexpr auto status_metrics_interval = "status.metrics_interval";
    static constexpr auto status_metrics_addr = "status.metrics_addr";
    static constexpr auto status_metrics_port = "status.metrics_port";

    void run();
    void convertMetrics(const GraphiteWriter::KeyValueVector<ssize_t> & key_vals);

    Poco::Util::Timer timer;

    Context & context;
    const AsynchronousMetrics & async_metrics;
    Logger * log;

    int metrics_interval;
    std::shared_ptr<prometheus::Gateway> gateway;
    std::shared_ptr<prometheus::Exposer> exposer;
    std::map<std::string, prometheus::Gauge &> gauge_map;
};

} // namespace DB
