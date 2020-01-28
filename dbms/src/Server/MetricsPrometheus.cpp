#include "MetricsPrometheus.h"

#include <Interpreters/AsynchronousMetrics.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>

#include <daemon/BaseDaemon.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>


namespace DB
{
std::shared_ptr<prometheus::Registry> MetricsPrometheus::registry_instance_ptr = nullptr;

std::mutex MetricsPrometheus::registry_instance_mutex;

std::shared_ptr<prometheus::Registry> MetricsPrometheus::getRegistry()
{
    if (registry_instance_ptr == nullptr)
    {
        std::lock_guard<std::mutex> lk(registry_instance_mutex);
        if (registry_instance_ptr == nullptr)
        {
            registry_instance_ptr = std::make_shared<prometheus::Registry>();
        }
    }
    return registry_instance_ptr;
}

MetricsPrometheus::MetricsPrometheus(Context & context_, const AsynchronousMetrics & async_metrics_)
    : context(context_), async_metrics(async_metrics_), log(&Logger::get("Prometheus"))
{
    auto & conf = context.getConfigRef();
    metrics_interval = conf.getInt(status_metrics_interval, 15);
    if (metrics_interval <= 0 || metrics_interval > 120)
    {
        metrics_interval = 15;
    }

    registry = MetricsPrometheus::getRegistry();

    if (!conf.hasOption(status_metrics_addr))
    {
        LOG_INFO(log, "Disable sending metrics to prometheus, cause " << status_metrics_addr << " is not set!");
    }
    else
    {
        const std::string metrics_addr = conf.getString(status_metrics_addr);

        auto pos = metrics_addr.find(':', 0);
        if (pos == std::string::npos)
        {
            LOG_ERROR(log, "Format error: " << status_metrics_addr << " = " << metrics_addr);
        }
        else
        {
            auto host = metrics_addr.substr(0, pos);
            auto port = metrics_addr.substr(pos + 1, metrics_addr.size());

            auto service_addr = conf.getString("flash.service_addr");
            std::string job_name = service_addr;
            std::replace(job_name.begin(), job_name.end(), ':', '_');
            std::replace(job_name.begin(), job_name.end(), '.', '_');
            job_name = "tiflash_" + job_name;

            char hostname[1024];
            ::gethostname(hostname, sizeof(hostname));

            gateway = std::make_shared<prometheus::Gateway>(host, port, job_name, prometheus::Gateway::GetInstanceLabel(hostname));
            gateway->RegisterCollectable(registry);

            LOG_INFO(log, "Enable sending metrics to prometheus; interval =" << metrics_interval << "; addr = " << metrics_addr);
        }
    }

    if (conf.hasOption(status_metrics_port))
    {
        auto metrics_port = conf.getString(status_metrics_port);
        exposer = std::make_shared<prometheus::Exposer>(metrics_port);
        exposer->RegisterCollectable(registry);
        LOG_INFO(log, "Metrics Port = " << metrics_port);
    }
}

MetricsPrometheus::~MetricsPrometheus()
{
    try
    {
        {
            std::lock_guard<std::mutex> lock{mutex};
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

void MetricsPrometheus::run()
{
    const std::string thread_name = "MetricsPrometheus " + std::to_string(metrics_interval) + "s";
    setThreadName(thread_name.c_str());

    const auto get_next_time = [](size_t seconds) {
        /// To avoid time drift and transmit values exactly each interval:
        ///  next time aligned to system seconds
        /// (60s -> every minute at 00 seconds, 5s -> every minute:[00, 05, 15 ... 55]s, 3600 -> every hour:00:00
        return std::chrono::system_clock::time_point(
            (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()) / seconds) * seconds
            + std::chrono::seconds(seconds));
    };

    std::vector<ProfileEvents::Count> prev_counters(ProfileEvents::end());

    std::unique_lock<std::mutex> lock{mutex};

    while (true)
    {
        if (metrics_interval > 0 && registry != nullptr)
            break;

        if (cond.wait_until(lock, get_next_time(5), [this] { return quit; }))
            break;
    }

    while (true)
    {
        if (cond.wait_until(lock, get_next_time(metrics_interval), [this] { return quit; }))
            break;

        convertMetrics(prev_counters);
    }
}

void MetricsPrometheus::convertMetrics(std::vector<ProfileEvents::Count> & prev_counters)
{
    auto async_metrics_values = async_metrics.getValues();

    GraphiteWriter::KeyValueVector<ssize_t> key_vals{};
    key_vals.reserve(ProfileEvents::end() + CurrentMetrics::end() + async_metrics_values.size());

    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
    {
        const auto counter = ProfileEvents::counters[i].load(std::memory_order_relaxed);
        const auto counter_increment = counter - prev_counters[i];
        prev_counters[i] = counter;

        std::string key{ProfileEvents::getDescription(static_cast<ProfileEvents::Event>(i))};
        key_vals.emplace_back(profile_events_path_prefix + key, counter_increment);
    }

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        const auto value = CurrentMetrics::values[i].load(std::memory_order_relaxed);

        std::string key{CurrentMetrics::getDescription(static_cast<CurrentMetrics::Metric>(i))};
        key_vals.emplace_back(current_metrics_path_prefix + key, value);
    }

    for (const auto & name_value : async_metrics_values)
    {
        key_vals.emplace_back(asynchronous_metrics_path_prefix + name_value.first, name_value.second);
    }

    if (!key_vals.empty())
        doConvertMetrics(key_vals);
}

void MetricsPrometheus::doConvertMetrics(const GraphiteWriter::KeyValueVector<ssize_t> & key_vals)
{
    using namespace prometheus;

    for (const auto & key_val : key_vals)
    {
        auto key = key_val.first;
        auto it = gauge_map.find(key);
        if (it != gauge_map.end())
        {
            auto & guage = it->second;
            guage.Set(key_val.second);
        }
        else
        {
            auto & gauge_family = BuildGauge()
                                      .Name(key_val.first)
                                      .Help("Get from system.metrics, system.events and system.asynchronous_metrics tables")
                                      .Register(*registry);

            auto & guage = gauge_family.Add({});
            guage.Set(key_val.second);

            auto pair = std::pair<std::string, prometheus::Gauge &>(key, guage);
            gauge_map.insert(pair);
        }
    }

    if (gateway != nullptr)
    {
        auto return_code = gateway->Push();
        if (return_code != 200)
        {
            LOG_WARNING(log, "Failed to push metrics to gateway, return code is " << return_code);
        }
    }
}
} // namespace DB
