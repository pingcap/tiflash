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

#include "MetricsTransmitter.h"

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/Context.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <daemon/BaseDaemon.h>


namespace DB
{
MetricsTransmitter::~MetricsTransmitter()
{
    try
    {
        {
            std::lock_guard lock{mutex};
            quit = true;
        }

        cond.notify_one();

        thread.join();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void MetricsTransmitter::run()
{
    const auto & config = context.getConfigRef();
    auto interval = config.getInt(config_name + ".interval", 60);

    setThreadName("CHMetricsTrns");

    const auto get_next_time = [](size_t seconds) {
        /// To avoid time drift and transmit values exactly each interval:
        ///  next time aligned to system seconds
        /// (60s -> every minute at 00 seconds, 5s -> every minute:[00, 05, 15 ... 55]s, 3600 -> every hour:00:00
        return std::chrono::steady_clock::time_point(
            (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch())
             / seconds)
                * seconds
            + std::chrono::seconds(seconds));
    };

    std::vector<ProfileEvents::Count> prev_counters(ProfileEvents::end());

    std::unique_lock lock{mutex};

    while (true)
    {
        if (cond.wait_until(lock, get_next_time(interval), [this] { return quit; }))
            break;

        transmit(prev_counters);
    }
}


void MetricsTransmitter::transmit(std::vector<ProfileEvents::Count> & prev_counters)
{
    const auto & config = context.getConfigRef();
    auto async_metrics_values = async_metrics.getValues();

    GraphiteWriter::KeyValueVector<ssize_t> key_vals{};
    key_vals.reserve(ProfileEvents::end() + CurrentMetrics::end() + async_metrics_values.size());


    if (config.getBool(config_name + ".events", true))
    {
        for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
        {
            const auto counter = ProfileEvents::counters[i].load(std::memory_order_relaxed);
            const auto counter_increment = counter - prev_counters[i];
            prev_counters[i] = counter;

            std::string key{ProfileEvents::getDescription(static_cast<ProfileEvents::Event>(i))};
            key_vals.emplace_back(profile_events_path_prefix + key, counter_increment);
        }
    }

    if (config.getBool(config_name + ".metrics", true))
    {
        for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        {
            const auto value = CurrentMetrics::values[i].load(std::memory_order_relaxed);

            std::string key{CurrentMetrics::getDescription(static_cast<CurrentMetrics::Metric>(i))};
            key_vals.emplace_back(current_metrics_path_prefix + key, value);
        }
    }

    if (config.getBool(config_name + ".asynchronous_metrics", true))
    {
        for (const auto & name_value : async_metrics_values)
        {
            key_vals.emplace_back(asynchronous_metrics_path_prefix + name_value.first, name_value.second);
        }
    }

    if (!key_vals.empty())
        BaseDaemon::instance().writeToGraphite(key_vals, config_name);
}
} // namespace DB
