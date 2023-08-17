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

#pragma once

#include <Common/Logger.h>
#include <Common/Timer.h>
#include <Poco/Net/HTTPServer.h>
#include <prometheus/exposer.h>
#include <prometheus/gateway.h>

namespace DB
{
class AsynchronousMetrics;
class Context;
class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;

/**    Automatically sends
  * - difference of ProfileEvents;
  * - values of CurrentMetrics;
  * - values of AsynchronousMetrics;
  *  to Prometheus
  */
class MetricsPrometheus
{
public:
    MetricsPrometheus(Context & context, const AsynchronousMetrics & async_metrics_);
    ~MetricsPrometheus();

private:
    static constexpr auto status_metrics_interval = "status.metrics_interval";
    static constexpr auto status_metrics_addr = "status.metrics_addr";
    static constexpr auto status_metrics_port = "status.metrics_port";
    static constexpr auto status_disable_metrics_tls = "status.disable_metrics_tls";

    void run();

    Timer timer;
    PathCapacityMetricsPtr path_capacity_metrics;
    const AsynchronousMetrics & async_metrics;
    LoggerPtr log;

    int metrics_interval;
    std::shared_ptr<prometheus::Gateway> gateway;
    std::shared_ptr<prometheus::Exposer> exposer;

    std::shared_ptr<Poco::Net::HTTPServer> server;
};

} // namespace DB
