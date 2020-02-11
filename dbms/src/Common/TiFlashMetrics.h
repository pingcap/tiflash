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
#define APPLY_FOR_METRICS(M, F)                                                                                                     \
    M(tiflash_coprocessor_request_count, "Total number of request", Counter, 4, F(type_batch, Counter, {"type", "batch"}),          \
        F(batch_type_cop, Counter, {"batch_type", "cop"}), F(type_cop, Counter, {"type", "cop"}),                                   \
        F(cop_type_dag, Counter, {"cop_type", "dag"}))                                                                              \
    M(tiflash_coprocessor_executor_count, "Total number of each executor", Counter, 5, F(type_ts, Counter, {"type", "table_scan"}), \
        F(type_sel, Counter, {"type", "selection"}), F(type_agg, Counter, {"type", "aggregation"}),                                 \
        F(type_topn, Counter, {"type", "top_n"}), F(type_limit, Counter, {"type", "limit"}))                                        \
    M(tiflash_coprocessor_request_duration_seconds, "Bucketed histogram of request duration", Histogram, 2,                         \
        F(type_batch, Histogram, {{"type", "batch"}}, ExpBuckets{0.0005, 2, 20}),                                                   \
        F(type_cop, Histogram, {{"type", "cop"}}, ExpBuckets{0.0005, 2, 20}))                                                       \
    M(tiflash_coprocessor_request_error, "Total number of request error", Counter, 6,                                               \
        F(reason_meet_lock, Counter, {"reason", "meet_lock"}), F(reason_region_not_found, Counter, {"reason", "region_not_found"}), \
        F(reason_epoch_not_match, Counter, {"reason", "epoch_not_match"}),                                                          \
        F(reason_kv_client_error, Counter, {"reason", "kv_client_error"}),                                                          \
        F(reason_internal_error, Counter, {"reason", "internal_error"}), F(reason_other_error, Counter, {"reason", "other_error"})) \
    M(tiflash_coprocessor_request_handle_seconds, "Bucketed histogram of request handle duration", Histogram, 2,                    \
        F(type_batch, Histogram, {{"type", "batch"}}, ExpBuckets{0.0005, 2, 20}),                                                   \
        F(type_cop, Histogram, {{"type", "cop"}}, ExpBuckets{0.0005, 2, 20}))                                                       \
    M(tiflash_coprocessor_response_bytes, "Total bytes of response body", Counter, 0)                                               \
    M(tiflash_schema_metric1, "Placeholder for schema sync metric", Counter, 0)                                                     \
    M(tiflash_schema_metric2, "Placeholder for schema sync metric", Counter, 0)                                                     \
    M(tiflash_schema_metric3, "Placeholder for schema sync metric", Counter, 0)

template <typename T>
struct MetricFamilyTrait
{
};
template <>
struct MetricFamilyTrait<prometheus::Counter>
{
    using MetricType = prometheus::Counter;
    using ArgType = std::map<std::string, std::string>;
    static auto build() { return prometheus::BuildCounter(); }
};
template <>
struct MetricFamilyTrait<prometheus::Gauge>
{
    using MetricType = prometheus::Gauge;
    using ArgType = std::map<std::string, std::string>;
    static auto build() { return prometheus::BuildGauge(); }
};
template <>
struct MetricFamilyTrait<prometheus::Histogram>
{
    using MetricType = prometheus::Histogram;
    using ArgType = std::tuple<std::map<std::string, std::string>, prometheus::Histogram::BucketBoundaries>;
    static auto build() { return prometheus::BuildHistogram(); }
};

using CounterArg = typename MetricFamilyTrait<prometheus::Counter>::ArgType;
using GaugeArg = typename MetricFamilyTrait<prometheus::Gauge>::ArgType;
using HistogramArg = typename MetricFamilyTrait<prometheus::Histogram>::ArgType;

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

template <typename T, size_t n>
struct MetricFamily
{
    template <typename... Args>
    MetricFamily(prometheus::Registry & registry, const std::string & name, const std::string & help, Args &&... args)
    {
        static_assert(sizeof...(Args) == n);
        auto & family = MetricFamilyTrait<T>::build().Name(name).Help(help).Register(registry);
        addMetrics(0, family, std::forward<Args>(args)...);
    }

    MetricFamily(prometheus::Registry & registry, const std::string & name, const std::string & help)
    {
        static_assert(n == 0);
        auto & family = MetricFamilyTrait<T>::build().Name(name).Help(help).Register(registry);
        addMetrics(0, family, typename MetricFamilyTrait<T>::ArgType{});
    }

    template <size_t idx = 0>
    T & get()
    {
        static_assert(idx < actual_size);
        return *(metrics[idx]);
    }

private:
    template <typename Type, typename Arg>
    inline void addMetrics(size_t i, prometheus::Family<Type> & family, Arg && arg)
    {
        metrics[i] = &family.Add(std::forward<Arg>(arg));
    }
    inline void addMetrics(
        size_t i, prometheus::Family<prometheus::Histogram> & family, MetricFamilyTrait<prometheus::Histogram>::ArgType && arg)
    {
        HistogramArg args{std::forward<HistogramArg>(arg)};
        metrics[i] = &family.Add(std::move(std::get<0>(args)), std::move(std::get<1>(args)));
    }
    template <typename Type, typename First, typename... Rest>
    inline void addMetrics(size_t i, prometheus::Family<Type> & family, First && first, Rest &&... rest)
    {
        addMetrics(i, family, std::forward<First>(first));
        addMetrics(i + 1, family, std::forward<Rest>(rest)...);
    }

private:
    static constexpr size_t actual_size = !n ? 1 : n;
    T * metrics[actual_size];
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
#define MAKE_METRIC_MEMBER_M(family_name, help, type, n, ...) \
    MetricFamily<prometheus::type, n> family_name = MetricFamily<prometheus::type, n>(*registry, #family_name, #help, ##__VA_ARGS__);
#define MAKE_METRIC_MEMBER_F(field_name, type, ...) \
    type##Arg { __VA_ARGS__ }
    APPLY_FOR_METRICS(MAKE_METRIC_MEMBER_M, MAKE_METRIC_MEMBER_F)

    friend class MetricsPrometheus;
};

#define MAKE_METRIC_ENUM_M(family_name, help, type, n, ...) \
    namespace family_name##_metrics                         \
    {                                                       \
        enum                                                \
        {                                                   \
            invalid = -1,                                   \
            ##__VA_ARGS__                                   \
        };                                                  \
    }
#define MAKE_METRIC_ENUM_F(field_name, type, ...) field_name
APPLY_FOR_METRICS(MAKE_METRIC_ENUM_M, MAKE_METRIC_ENUM_F)

#define __GET_METRIC_MACRO(_1, _2, _3, NAME, ...) NAME
#define __GET_METRIC_0(ptr, family) (ptr)->family.get()
#define __GET_METRIC_1(ptr, family, metric) (ptr)->family.get<family##_metrics::metric>()
#define GET_METRIC(...) __GET_METRIC_MACRO(__VA_ARGS__, __GET_METRIC_1, __GET_METRIC_0)(__VA_ARGS__)

} // namespace DB
