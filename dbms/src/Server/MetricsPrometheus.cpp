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

#include <Common/CurrentMetrics.h>
#include <Common/FunctionTimerTask.h>
#include <Common/ProcessCollector.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TiFlashMetrics.h>
#include <Common/TiFlashSecurity.h>
#include <Common/setThreadName.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Poco/Crypto/X509Certificate.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Server/CertificateReloader.h>
#include <Server/MetricsPrometheus.h>
#include <Storages/PathCapacityMetrics.h>
#include <common/logger_useful.h>
#include <daemon/BaseDaemon.h>
#include <fmt/core.h>
#include <prometheus/collectable.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>
#include <prometheus/text_serializer.h>

namespace DB
{
namespace
{
std::string getHostName()
{
    char hostname[1024];
    if (::gethostname(hostname, sizeof(hostname)))
    {
        return {};
    }
    return hostname;
}

std::string getInstanceValue(const Poco::Util::AbstractConfiguration & conf)
{
    if (conf.has("flash.service_addr"))
    {
        auto service_addr = conf.getString("flash.service_addr");
        if (service_addr.empty())
            return getHostName();
        // "0.0.0.0", "127.x.x.x", "localhost", "0:0:0:0:0:0:0:0", "0:0:0:0:0:0:0:1", "::", "::1", ":${port}"
        static const std::vector<std::string> blacklist{// ivp4
                                                        "0.0.0.0",
                                                        "127.",
                                                        "localhost",
                                                        // ipv6
                                                        "0:0:0:0:0:0:0",
                                                        "[0:0:0:0:0:0:0",
                                                        ":",
                                                        "[:"};
        for (const auto & prefix : blacklist)
        {
            if (startsWith(service_addr, prefix))
                return getHostName();
        }
        return service_addr;
    }
    else
    {
        return getHostName();
    }
}
} // namespace

class MetricHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit MetricHandler(const std::vector<std::weak_ptr<prometheus::Collectable>> & collectables_)
        : collectables(collectables_)
    {}

    ~MetricHandler() override = default;

    void handleRequest(Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response) override
    {
        auto metrics = collectMetrics();
        auto serializer = std::unique_ptr<prometheus::Serializer>{new prometheus::TextSerializer()};
        String body = serializer->Serialize(metrics);
        response.sendBuffer(body.data(), body.size());
    }

private:
    std::vector<prometheus::MetricFamily> collectMetrics() const
    {
        auto collected_metrics = std::vector<prometheus::MetricFamily>{};

        for (const auto & collectable : collectables)
        {
            auto collect = collectable.lock();
            if (collect)
            {
                auto && metrics = collect->Collect();
                collected_metrics.insert(
                    collected_metrics.end(),
                    std::make_move_iterator(metrics.begin()),
                    std::make_move_iterator(metrics.end()));
            }
        }
        return collected_metrics;
    }

    std::vector<std::weak_ptr<prometheus::Collectable>> collectables;
};

class MetricHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit MetricHandlerFactory(const std::vector<std::weak_ptr<prometheus::Collectable>> & collectables_)
        : collectables(collectables_)
    {}

    ~MetricHandlerFactory() override = default;

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override
    {
        const String & uri = request.getURI();
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET
            || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD)
        {
            if (uri == "/metrics")
            {
                return new MetricHandler(collectables);
            }
        }
        return nullptr;
    }

private:
    std::vector<std::weak_ptr<prometheus::Collectable>> collectables;
};

std::shared_ptr<Poco::Net::HTTPServer> getHTTPServer(
    Context & global_context,
    std::vector<std::weak_ptr<prometheus::Collectable>> collectables,
    const String & address)
{
    auto security_config = global_context.getSecurityConfig();
    auto [ca_path, cert_path, key_path] = security_config->getPaths();
    Poco::Net::Context::Ptr context = new Poco::Net::Context(
        Poco::Net::Context::TLSV1_2_SERVER_USE,
        key_path,
        cert_path,
        ca_path,
        Poco::Net::Context::VerificationMode::VERIFY_STRICT);

    auto check_common_name = [&](const Poco::Crypto::X509Certificate & cert) {
        return global_context.getSecurityConfig()->checkCommonName(cert);
    };

    context->setAdhocVerification(check_common_name);
#if Poco_NetSSL_FOUND
    CertificateReloader::initSSLCallback(context, &global_context);
#endif
    Poco::Net::SecureServerSocket socket(context);

    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    Poco::Net::SocketAddress addr = Poco::Net::SocketAddress(address);
    socket.bind(addr, true);
    socket.listen();
    auto server = std::make_shared<Poco::Net::HTTPServer>(new MetricHandlerFactory(collectables), socket, http_params);
    return server;
}

constexpr Int64 MILLISECOND = 1000;
constexpr Int64 INIT_DELAY = 5;

namespace
{
inline bool isIPv6(const String & input_address)
{
    if (input_address.empty())
        return false;
    char str[INET6_ADDRSTRLEN];
    return inet_pton(AF_INET6, input_address.c_str(), str) == 1;
}
} // namespace

MetricsPrometheus::MetricsPrometheus(Context & context, const AsynchronousMetrics & async_metrics_)
    : timer("Prometheus")
    , path_capacity_metrics(context.getPathCapacity())
    , async_metrics(async_metrics_)
    , log(Logger::get("Prometheus"))
{
    auto & tiflash_metrics = TiFlashMetrics::instance();
    auto & conf = context.getConfigRef();

    bool should_provide_proxy_metrics
        = (context.getSharedContextDisagg()->isDisaggregatedComputeMode()
           && context.getSharedContextDisagg()->use_autoscaler);
    tiflash_metrics.setProvideProxyProcessMetrics(should_provide_proxy_metrics);

    // Interval to collect `ProfileEvents::Event`/`CurrentMetrics::Metric`/`AsynchronousMetrics`
    // When push mode is enabled, it also define the interval that Prometheus client push to pushgateway.
    metrics_interval = conf.getInt(status_metrics_interval, 15);
    if (metrics_interval < 5)
    {
        LOG_WARNING(log, "Config Error: {} should >= 5", status_metrics_interval);
        metrics_interval = 5;
    }
    if (metrics_interval > 120)
    {
        LOG_WARNING(log, "Config Error: {} should <= 120", status_metrics_interval);
        metrics_interval = 120;
    }
    LOG_INFO(log, "Config: {} = {}", status_metrics_interval, metrics_interval);

    // Usually TiFlash disable prometheus push mode when deployed by TiUP/TiDB-Operator
    if (!conf.hasOption(status_metrics_addr))
    {
        LOG_INFO(log, "Disable prometheus push mode, cause {} is not set!", status_metrics_addr);
    }
    else
    {
        const std::string metrics_addr = conf.getString(status_metrics_addr);

        auto pos = metrics_addr.find(':', 0);
        if (pos == std::string::npos)
        {
            LOG_ERROR(log, "Format error: {} = {}", status_metrics_addr, metrics_addr);
        }
        else
        {
            auto host = metrics_addr.substr(0, pos);
            auto port = metrics_addr.substr(pos + 1, metrics_addr.size());

            const String & job_name = "tiflash";
            const auto & labels = prometheus::Gateway::GetInstanceLabel(getInstanceValue(conf));
            gateway = std::make_shared<prometheus::Gateway>(host, port, job_name, labels);
            gateway->RegisterCollectable(tiflash_metrics.registry);
            gateway->RegisterCollectable(tiflash_metrics.process_collector);

            LOG_INFO(log, "Enable prometheus push mode; interval = {}; addr = {}", metrics_interval, metrics_addr);
        }
    }

    // Usually TiFlash enables Prometheus pull mode when deployed by TiUP/TiDB-Operator.
    // Enable pull mode by default when push mode is disabled.
    if (conf.hasOption(status_metrics_port) || !conf.hasOption(status_metrics_addr))
    {
        auto metrics_port = conf.getString(status_metrics_port, DB::toString(DEFAULT_METRICS_PORT));
        auto listen_host = conf.getString("listen_host", "0.0.0.0");
        String addr;
        if (isIPv6(listen_host))
            addr = "[" + listen_host + "]:" + metrics_port;
        else
            addr = listen_host + ":" + metrics_port;
        if (context.getSecurityConfig()->hasTlsConfig() && !conf.getBool(status_disable_metrics_tls, false))
        {
            std::vector<std::weak_ptr<prometheus::Collectable>> collectables{
                tiflash_metrics.registry,
                tiflash_metrics.process_collector};
            server = getHTTPServer(context, collectables, addr);
            server->start();
            LOG_INFO(
                log,
                "Enable prometheus secure pull mode; Listen Host = {}, Metrics Port = {}",
                listen_host,
                metrics_port);
        }
        else
        {
            exposer = std::make_shared<prometheus::Exposer>(addr);
            exposer->RegisterCollectable(tiflash_metrics.registry);
            exposer->RegisterCollectable(tiflash_metrics.process_collector);
            LOG_INFO(
                log,
                "Enable prometheus pull mode; Listen Host = {}, Metrics Port = {}",
                listen_host,
                metrics_port);
        }
    }
    else
    {
        LOG_INFO(log, "Disable prometheus pull mode");
    }

    timer.scheduleAtFixedRate(
        FunctionTimerTask::create([this] { run(); }),
        INIT_DELAY * MILLISECOND,
        metrics_interval * MILLISECOND);
}

MetricsPrometheus::~MetricsPrometheus()
{
    timer.cancel(true);
}

void MetricsPrometheus::run()
{
    auto & tiflash_metrics = TiFlashMetrics::instance();
    for (ProfileEvents::Event event = 0; event < ProfileEvents::end(); ++event)
    {
        const auto value = ProfileEvents::counters[event].load(std::memory_order_relaxed);
        tiflash_metrics.registered_profile_events[event]->Set(value);
    }

    for (CurrentMetrics::Metric metric = 0; metric < CurrentMetrics::end(); ++metric)
    {
        const auto value = CurrentMetrics::values[metric].load(std::memory_order_relaxed);
        tiflash_metrics.registered_current_metrics[metric]->Set(value);
    }

    size_t total_size = 0;
    {
        // Add keyspace store usage here
        const auto & keyspace_usage = path_capacity_metrics->getKeyspaceUsedSizes();
        for (const auto & [keyspace_id, usage] : keyspace_usage)
        {
            total_size += usage;
            if (!tiflash_metrics.registered_keypace_store_used_metrics.count(keyspace_id))
            {
                // Add new keyspace store usage metric
                tiflash_metrics.registered_keypace_store_used_metrics.emplace(
                    keyspace_id,
                    &tiflash_metrics.registered_keypace_store_used_family->Add(
                        {{"keyspace_id", std::to_string(keyspace_id)}, {"type", "used"}}));
            }
            tiflash_metrics.registered_keypace_store_used_metrics[keyspace_id]->Set(usage);
        }

        for (auto & [keyspace_id, metric] : tiflash_metrics.registered_keypace_store_used_metrics)
        {
            if (!keyspace_usage.count(keyspace_id))
            {
                // Remove stale keyspace store usage metric
                LOG_DEBUG(log, "Remove stale keyspace store usage metric: keyspace_id = {}", keyspace_id);
                tiflash_metrics.registered_keypace_store_used_family->Remove(metric);
                tiflash_metrics.registered_keypace_store_used_metrics.erase(keyspace_id);
                tiflash_metrics.removeReplicaSyncRUCounter(keyspace_id);
            }
        }
    }
    tiflash_metrics.store_used_total_metric->Set(total_size);

    auto async_metric_values = async_metrics.getValues();
    for (const auto & metric : async_metric_values)
    {
        const auto & origin_name = metric.first;
        const auto & value = metric.second;
        if (!tiflash_metrics.registered_async_metrics.count(origin_name))
        {
            // Register this async metric into registry on flight, as async metrics are not accumulated at once.
            auto prometheus_name = TiFlashMetrics::async_metrics_prefix + metric.first;
            // Prometheus doesn't allow metric name containing dot.
            std::replace(prometheus_name.begin(), prometheus_name.end(), '.', '_');
            auto & family = prometheus::BuildGauge()
                                .Name(prometheus_name)
                                .Help("System asynchronous metric " + prometheus_name)
                                .Register(*(tiflash_metrics.registry));
            // Use original name as key for the sake of further accesses.
            tiflash_metrics.registered_async_metrics.emplace(origin_name, &family.Add({}));
        }
        tiflash_metrics.registered_async_metrics[origin_name]->Set(value);
    }

    if (gateway != nullptr)
    {
        if (auto return_code = gateway->Push(); return_code != 200)
        {
            LOG_WARNING(log, "Failed to push metrics to gateway, return code is {}", return_code);
        }
    }
}

} // namespace DB
