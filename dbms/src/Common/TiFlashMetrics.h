#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gateway.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

namespace DB
{

/// Central place to define metrics across all subsystems.
/// Sample defines:
/// M(tiflash_test_counter, "Test counter metric w/o labels", Counter, 0)
/// M(tiflash_test_counter_with_1_label, "Test counter metric with 1 label", Counter, 1, CounterArg{{"label1", "value1"}})
/// M(tiflash_test_counter_with_2_labels, "Test counter metric with 2 labels", Counter, 2, CounterArg{{"label1", "value1"}}, CounterArg{{"label21", "value21"}, {"label22", "value22"}})
/// M(tiflash_test_gauge, "Test gauge metric w/o labels", Gauge, 0)
/// M(tiflash_test_gauge_with_1_label, "Test gauge metric with 1 label", Gauge, 1, GaugeArg{{"label1", "value1"}})
/// M(tiflash_test_gauge_with_2_labels, "Test gauge metric with 2 labels", Gauge, 2, GaugeArg{{"label1", "value1"}}, GaugeArg{{"label21", "value22"}, {"label22", "value22"}})
/// M(tiflash_test_histogram, "Test histogram metric w/o labels", Histogram, 0)
/// M(tiflash_test_histogram_with_1_label, "Test histogram metric with 1 label", Histogram, 1, HistogramArg{{{"label1", "value1"}}, {100}})
/// M(tiflash_test_histogram_with_2_labels, "Test histogram metric with 2 labels", Histogram, 2, HistogramArg{{{"label1", "value1"}}, {0.1, 0.2, 0.3, 0.4}}, HistogramArg{{{"label21", "value21"}, {"label22", "value22"}}, {1, 2, 3, 4}})
/// Sample usage:
/// context.getTiFlashMetrics()->tiflash_test_counter.Set(1);
#ifdef M
#error "Please undefine macro M first."
#endif
#define APPLY_FOR_METRICS(M)                                                             \
    M(tiflash_coprocessor_dag_request_count, "Total number of DAG requests", Counter, 0) \
    M(tiflash_coprocessor_executor_count, "Total number of each executor", Counter, 0)   \
    M(tiflash_coprocessor_request_duration_seconds, "Bucketed histogram of coprocessor request duration", Histogram, 0)

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

    template <>
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
    template <>
    inline void addMetrics<prometheus::Histogram, MetricFamilyTrait<prometheus::Histogram>::ArgType>(
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
    static constexpr auto profile_events_prefix = "tiflash_system_profile_events_";
    static constexpr auto current_metrics_prefix = "tiflash_system_current_metrics_";
    static constexpr auto async_metrics_prefix = "tiflash_system_asynchronous_metrics_";

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
