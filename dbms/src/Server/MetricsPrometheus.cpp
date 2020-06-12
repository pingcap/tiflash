#include "MetricsPrometheus.h"

#include <Common/CurrentMetrics.h>
#include <Common/FunctionTimerTask.h>
#include <Common/ProfileEvents.h>
#include <Common/TiFlashMetrics.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/Context.h>
#include <daemon/BaseDaemon.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>


namespace DB
{

constexpr long MILLISECOND = 1000;
constexpr long INIT_DELAY = 5;

MetricsPrometheus::MetricsPrometheus(Context & context, const AsynchronousMetrics & async_metrics_)
    : timer(), tiflash_metrics(context.getTiFlashMetrics()), async_metrics(async_metrics_), log(&Logger::get("Prometheus"))
{
    auto & conf = context.getConfigRef();

    metrics_interval = conf.getInt(status_metrics_interval, 15);
    if (metrics_interval < 5)
    {
        LOG_WARNING(log, "Config Error: " << status_metrics_interval << " should >= 5");
        metrics_interval = 5;
    }
    if (metrics_interval > 120)
    {
        LOG_WARNING(log, "Config Error: " << status_metrics_interval << " should <= 120");
        metrics_interval = 120;
    }
    LOG_INFO(log, "Config: " << status_metrics_interval << " = " << metrics_interval);

    if (!conf.hasOption(status_metrics_addr))
    {
        LOG_INFO(log, "Disable prometheus push mode, cause " << status_metrics_addr << " is not set!");
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
            gateway->RegisterCollectable(tiflash_metrics->registry);

            LOG_INFO(log, "Enable prometheus push mode; interval =" << metrics_interval << "; addr = " << metrics_addr);
        }
    }

    if (conf.hasOption(status_metrics_port))
    {
        auto metrics_port = conf.getString(status_metrics_port);
        exposer = std::make_shared<prometheus::Exposer>(metrics_port);
        exposer->RegisterCollectable(tiflash_metrics->registry);
        LOG_INFO(log, "Enable prometheus pull mode; Metrics Port = " << metrics_port);
    }
    else
    {
        LOG_INFO(log, "Disable prometheus pull mode");
    }

    timer.scheduleAtFixedRate(
        FunctionTimerTask::create(std::bind(&MetricsPrometheus::run, this)), INIT_DELAY * MILLISECOND, metrics_interval * MILLISECOND);
}

MetricsPrometheus::~MetricsPrometheus() { timer.cancel(true); }

void MetricsPrometheus::run()
{
    for (ProfileEvents::Event event = 0; event < ProfileEvents::end(); event++)
    {
        const auto value = ProfileEvents::counters[event].load(std::memory_order_relaxed);
        tiflash_metrics->registered_profile_events[event]->Set(value);
    }

    for (CurrentMetrics::Metric metric = 0; metric < CurrentMetrics::end(); metric++)
    {
        const auto value = CurrentMetrics::values[metric].load(std::memory_order_relaxed);
        tiflash_metrics->registered_current_metrics[metric]->Set(value);
    }

    auto async_metric_values = async_metrics.getValues();
    for (const auto & metric : async_metric_values)
    {
        const auto & origin_name = metric.first;
        const auto & value = metric.second;
        if (!tiflash_metrics->registered_async_metrics.count(origin_name))
        {
            // Register this async metric into registry on flight, as async metrics are not accumulated at once.
            auto prometheus_name = TiFlashMetrics::async_metrics_prefix + metric.first;
            // Prometheus doesn't allow metric name containing dot.
            std::replace(prometheus_name.begin(), prometheus_name.end(), '.', '_');
            auto & family = prometheus::BuildGauge()
                                .Name(prometheus_name)
                                .Help("System asynchronous metric " + prometheus_name)
                                .Register(*tiflash_metrics->registry);
            // Use original name as key for the sake of further accesses.
            tiflash_metrics->registered_async_metrics.emplace(origin_name, &family.Add({}));
        }
        tiflash_metrics->registered_async_metrics[origin_name]->Set(value);
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
