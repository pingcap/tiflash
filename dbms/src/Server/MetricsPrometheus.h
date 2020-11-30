#pragma once

#include <Common/TiFlashSecurity.h>
#include <Common/Timer.h>
#include <Poco/Net/HTTPServer.h>
#include <common/logger_useful.h>
#include <prometheus/exposer.h>
#include <prometheus/gateway.h>

namespace DB
{

class AsynchronousMetrics;
class Context;
class TiFlashMetrics;
using TiFlashMetricsPtr = std::shared_ptr<TiFlashMetrics>;

/**    Automatically sends
  * - difference of ProfileEvents;
  * - values of CurrentMetrics;
  * - values of AsynchronousMetrics;
  *  to Prometheus
  */
class MetricsPrometheus
{
public:
    MetricsPrometheus(Context & context, const AsynchronousMetrics & async_metrics_, const TiFlashSecurityConfig & config);
    ~MetricsPrometheus();

private:
    static constexpr auto status_metrics_interval = "status.metrics_interval";
    static constexpr auto status_metrics_addr = "status.metrics_addr";
    static constexpr auto status_metrics_port = "status.metrics_port";

    void run();

    Timer timer;
    TiFlashMetricsPtr tiflash_metrics;
    const AsynchronousMetrics & async_metrics;
    Logger * log;

    int metrics_interval;
    std::shared_ptr<prometheus::Gateway> gateway;
    std::shared_ptr<prometheus::Exposer> exposer;

    std::shared_ptr<Poco::Net::HTTPServer> server;
};

} // namespace DB
