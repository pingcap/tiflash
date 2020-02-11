#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gateway.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

namespace DB
{

/// Central place to define metrics across all subsystems.
/// Refer to gtest_tiflash_metrics.cpp for more sample defines.
/// Usage:
/// GET_METRIC(context.getTiFlashMetrics(), tiflash_coprocessor_response_bytes).Increment(1);
/// GET_METRIC(context.getTiFlashMetrics(), tiflash_coprocessor_request_count, type_batch).Set(1);
/// Maintenance notes:
/// 1. Use same name prefix for metrics in same subsystem (coprocessor/schema/tmt/raft/etc.).
/// 2. Keep metrics with same prefix next to each other.
/// 3. Add metrics of new subsystems at tail.
/// 4. Keep it proper formatted using clang-format.
#define APPLY_FOR_METRICS(M, F)                                                                                                   \
    M(tiflash_coprocessor_request_count, "Total number of request", Counter, F(type_batch, {"type", "batch"}),                    \
        F(batch_type_cop, {"batch_type", "cop"}), F(type_cop, {"type", "cop"}), F(cop_type_dag, {"cop_type", "dag"}))             \
    M(tiflash_coprocessor_executor_count, "Total number of each executor", Counter, F(type_ts, {"type", "table_scan"}),           \
        F(type_sel, {"type", "selection"}), F(type_agg, {"type", "aggregation"}), F(type_topn, {"type", "top_n"}),                \
        F(type_limit, {"type", "limit"}))                                                                                         \
    M(tiflash_coprocessor_request_duration_seconds, "Bucketed histogram of request duration", Histogram,                          \
        F(type_batch, {{"type", "batch"}}, ExpBuckets{0.0005, 2, 20}), F(type_cop, {{"type", "cop"}}, ExpBuckets{0.0005, 2, 20})) \
    M(tiflash_coprocessor_request_error, "Total number of request error", Counter, F(reason_meet_lock, {"reason", "meet_lock"}),  \
        F(reason_region_not_found, {"reason", "region_not_found"}), F(reason_epoch_not_match, {"reason", "epoch_not_match"}),     \
        F(reason_kv_client_error, {"reason", "kv_client_error"}), F(reason_internal_error, {"reason", "internal_error"}),         \
        F(reason_other_error, {"reason", "other_error"}))                                                                         \
    M(tiflash_coprocessor_request_handle_seconds, "Bucketed histogram of request handle duration", Histogram,                     \
        F(type_batch, {{"type", "batch"}}, ExpBuckets{0.0005, 2, 20}), F(type_cop, {{"type", "cop"}}, ExpBuckets{0.0005, 2, 20})) \
    M(tiflash_coprocessor_response_bytes, "Total bytes of response body", Counter)                                                \
    M(tiflash_schema_metric1, "Placeholder for schema sync metric", Counter)                                                      \
    M(tiflash_schema_metric2, "Placeholder for schema sync metric", Counter)                                                      \
    M(tiflash_schema_metric3, "Placeholder for schema sync metric", Counter)

struct ExpBuckets
{
    const double start;
    const int base;
    const size_t size;
    inline operator prometheus::Histogram::BucketBoundaries() const &&
    {
        prometheus::Histogram::BucketBoundaries buckets(size);
        double current = start;
        std::for_each(buckets.begin(), buckets.end(), [&](auto & e) {
            e = current;
            current *= base;
        });
        return buckets;
    }
};

template <typename T>
struct MetricFamilyTrait
{
};
template <>
struct MetricFamilyTrait<prometheus::Counter>
{
    using ArgType = std::map<std::string, std::string>;
    static auto build() { return prometheus::BuildCounter(); }
    static auto & add(prometheus::Family<prometheus::Counter> & family, ArgType && arg) { return family.Add(std::forward<ArgType>(arg)); }
};
template <>
struct MetricFamilyTrait<prometheus::Gauge>
{
    using ArgType = std::map<std::string, std::string>;
    static auto build() { return prometheus::BuildGauge(); }
    static auto & add(prometheus::Family<prometheus::Gauge> & family, ArgType && arg) { return family.Add(std::forward<ArgType>(arg)); }
};
template <>
struct MetricFamilyTrait<prometheus::Histogram>
{
    using ArgType = std::tuple<std::map<std::string, std::string>, prometheus::Histogram::BucketBoundaries>;
    static auto build() { return prometheus::BuildHistogram(); }
    static auto & add(prometheus::Family<prometheus::Histogram> & family, ArgType && arg)
    {
        return family.Add(std::move(std::get<0>(arg)), std::move(std::get<1>(arg)));
    }
};

template <typename T>
struct MetricFamily
{
    using MetricTrait = MetricFamilyTrait<T>;
    using MetricArgType = typename MetricTrait::ArgType;

    MetricFamily(
        prometheus::Registry & registry, const std::string & name, const std::string & help, std::initializer_list<MetricArgType> args)
    {
        auto & family = MetricTrait::build().Name(name).Help(help).Register(registry);
        metrics.reserve(args.size() ? args.size() : 1);
        for (auto arg : args)
        {
            auto & metric = MetricTrait::add(family, std::forward<MetricArgType>(arg));
            metrics.emplace_back(&metric);
        }
        if (metrics.empty())
        {
            auto & metric = MetricTrait::add(family, MetricArgType{});
            metrics.emplace_back(&metric);
        }
    }

    T & get(size_t idx = 0) { return *(metrics[idx]); }

private:
    std::vector<T *> metrics;
};

/// Centralized registry of TiFlash metrics.
/// Cope with MetricsPrometheus by registering
/// profile events, current metrics and customized metrics (as individual member for caller to access) into registry ahead of being updated.
/// Asynchronous metrics will be however registered by MetricsPrometheus itself due to the life cycle difference.
class TiFlashMetrics
{
public:
    TiFlashMetrics();

private:
    static constexpr auto profile_events_prefix = "tiflash_system_profile_event_";
    static constexpr auto current_metrics_prefix = "tiflash_system_current_metric_";
    static constexpr auto async_metrics_prefix = "tiflash_system_asynchronous_metric_";

    std::shared_ptr<prometheus::Registry> registry = std::make_shared<prometheus::Registry>();

    std::vector<prometheus::Gauge *> registered_profile_events;
    std::vector<prometheus::Gauge *> registered_current_metrics;
    std::unordered_map<std::string, prometheus::Gauge *> registered_async_metrics;

public:
#define MAKE_METRIC_MEMBER_M(family_name, help, type, ...) \
    MetricFamily<prometheus::type> family_name = MetricFamily<prometheus::type>(*registry, #family_name, #help, {__VA_ARGS__});
#define MAKE_METRIC_MEMBER_F(field_name, ...) \
    {                                         \
        __VA_ARGS__                           \
    }
    APPLY_FOR_METRICS(MAKE_METRIC_MEMBER_M, MAKE_METRIC_MEMBER_F)

    friend class MetricsPrometheus;
};

#define MAKE_METRIC_ENUM_M(family_name, help, type, ...) \
    namespace family_name##_metrics                      \
    {                                                    \
        enum                                             \
        {                                                \
            invalid = -1,                                \
            ##__VA_ARGS__                                \
        };                                               \
    }
#define MAKE_METRIC_ENUM_F(field_name, ...) field_name
APPLY_FOR_METRICS(MAKE_METRIC_ENUM_M, MAKE_METRIC_ENUM_F)

#define __GET_METRIC_MACRO(_1, _2, _3, NAME, ...) NAME
#define __GET_METRIC_0(ptr, family) (ptr)->family.get()
#define __GET_METRIC_1(ptr, family, metric) (ptr)->family.get(family##_metrics::metric)
#define GET_METRIC(...) __GET_METRIC_MACRO(__VA_ARGS__, __GET_METRIC_1, __GET_METRIC_0)(__VA_ARGS__)

} // namespace DB
