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

#include <Common/ProfileEvents.h>

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
  *  to Graphite at beginning of every minute.
  */
class MetricsTransmitter
{
public:
    MetricsTransmitter(Context & context_, const AsynchronousMetrics & async_metrics_, const std::string & config_name_)
        : context(context_)
        , async_metrics(async_metrics_)
        , config_name(config_name_)
    {}
    ~MetricsTransmitter();

private:
    void run();
    void transmit(std::vector<ProfileEvents::Count> & prev_counters);

    Context & context;

    const AsynchronousMetrics & async_metrics;
    const std::string config_name;

    bool quit = false;
    std::mutex mutex;
    std::condition_variable cond;
    std::thread thread{&MetricsTransmitter::run, this};

    static constexpr auto profile_events_path_prefix = "ClickHouse.ProfileEvents.";
    static constexpr auto current_metrics_path_prefix = "ClickHouse.Metrics.";
    static constexpr auto asynchronous_metrics_path_prefix = "ClickHouse.AsynchronousMetrics.";
};

} // namespace DB
