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
/// context.getTiFlashMetrics()->tiflash_test_counter.get().Set(1);
#ifdef M
#error "Please undefine macro M first."
#endif
#define APPLY_FOR_METRICS(M)                                                                                                \
    M(tiflash_coprocessor_dag_request_count, "Total number of DAG requests", Counter, 2, CounterArg{{"vec_type", "batch"}}, \
        CounterArg{{"vec_type", "normal"}})                                                                                 \
    M(tiflash_coprocessor_executor_count, "Total number of each executor", Counter, 5, CounterArg{{"type", "table_scan"}},  \
        CounterArg{{"type", "selection"}}, CounterArg{{"type", "aggregation"}}, CounterArg{{"type", "top_n"}},              \
        CounterArg{{"type", "limit"}})                                                                                      \
    M(tiflash_coprocessor_request_duration_seconds, "Bucketed histogram of coprocessor request duration", Histogram, 1,     \
        HistogramArg{{{"req", "select"}}, ExpBuckets{0.0005, 2, 20}})                                                       \
    M(tiflash_schema_metric1, "Placeholder for schema sync metric", Counter, 0)                                             \
    M(tiflash_schema_metric2, "Placeholder for schema sync metric", Counter, 0)                                             \
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
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(name, help, type, n, ...) \
    MetricFamily<prometheus::type, n> name = MetricFamily<prometheus::type, n>(*registry, #name, #help, ##__VA_ARGS__);
    APPLY_FOR_METRICS(M)
#undef M

    friend class MetricsPrometheus;
};

} // namespace DB
