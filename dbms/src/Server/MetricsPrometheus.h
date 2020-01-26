#pragma once

#include <Common/ProfileEvents.h>
#include <Interpreters/Context.h>
#include <daemon/GraphiteWriter.h>
#include <prometheus/exposer.h>
#include <prometheus/gateway.h>
#include <prometheus/gauge.h>
#include <prometheus/registry.h>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <vector>


namespace DB
{

class AsynchronousMetrics;
class Context;


/**    Automatically sends
  * - difference of ProfileEvents;
  * - values of CurrentMetrics;
  * - values of AsynchronousMetrics;
  *  to Prometheus
  */
class MetricsPrometheus
{
public:
    static std::shared_ptr<prometheus::Registry> getRegistry();

    MetricsPrometheus(Context & context_, const AsynchronousMetrics & async_metrics_);
    ~MetricsPrometheus();

private:
    static std::shared_ptr<prometheus::Registry> registry_instance_ptr;
    static std::mutex registry_instance_mutex;

    static constexpr auto profile_events_path_prefix = "tiflash_system_profile_events_";
    static constexpr auto current_metrics_path_prefix = "tiflash_system_metrics_";
    static constexpr auto asynchronous_metrics_path_prefix = "tiflash_system_asynchronous_metrics_";

    static constexpr auto status_metrics_interval = "status.metrics_interval";
    static constexpr auto status_metrics_addr = "status.metrics_addr";
    static constexpr auto status_metrics_port = "status.metrics_port";

    void run();
    void convertMetrics(std::vector<ProfileEvents::Count> & prev_counters);
    void doConvertMetrics(const GraphiteWriter::KeyValueVector<ssize_t> & key_vals);

    Context & context;
    const AsynchronousMetrics & async_metrics;
    bool quit = false;
    std::mutex mutex;
    std::condition_variable cond;
    std::thread thread{&MetricsPrometheus::run, this};
    std::shared_ptr<prometheus::Gateway> gateway;
    std::shared_ptr<prometheus::Registry> registry;
    std::shared_ptr<prometheus::Exposer> exposer;
    std::map<std::string, prometheus::Gauge &> gauge_map;
    int metricsInterval;
    Logger * log;
};

} // namespace DB
