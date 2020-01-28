#include "MetricsPrometheus.h"

#include <Interpreters/AsynchronousMetrics.h>

#include <Common/CurrentMetrics.h>

#include <daemon/BaseDaemon.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>

#include <Common/FunctionTimerTask.h>


namespace DB
{

constexpr long MILLISECOND = 1000;
constexpr long INIT_DELAY = 5;

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
    : timer(), context(context_), async_metrics(async_metrics_), log(&Logger::get("Prometheus")), registry(MetricsPrometheus::getRegistry())
{
    auto & conf = context.getConfigRef();

    metrics_interval = conf.getInt(status_metrics_interval, 15);
    if (metrics_interval < 5 || metrics_interval > 120)
    {
        metrics_interval = 15;
    }

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

    timer.scheduleAtFixedRate(
        FunctionTimerTask::create(std::bind(&MetricsPrometheus::run, this)), INIT_DELAY * MILLISECOND, metrics_interval * MILLISECOND);
}

MetricsPrometheus::~MetricsPrometheus() { timer.cancel(true); }

void MetricsPrometheus::run()
{
    std::vector<ProfileEvents::Count> prev_counters(ProfileEvents::end());
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
        convertMetrics(key_vals);
}

void MetricsPrometheus::convertMetrics(const GraphiteWriter::KeyValueVector<ssize_t> & key_vals)
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
