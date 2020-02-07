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
/// context.getMetricsPrometheus().tiflash_test_counter.Set(1);
#ifdef M
#error "Please undefine macro M first."
#endif
#define APPLY_FOR_METRICS(M)                                                                                                 \
    M(tiflash_coprocessor_dag_request_count, "Total number of DAG requests", Counter, 0)                                     \
    M(tiflash_coprocessor_executor_count, "Total number of each executor", Counter, 0)                                       \
    M(tiflash_coprocessor_request_duration_seconds, "Bucketed histogram of coprocessor request duration", Histogram, 0)      \
    M(tiflash_test_counter, "Test counter metric w/o labels", Counter, 0)                                                    \
    M(tiflash_test_counter_with_1_label, "Test counter metric with 1 label", Counter, 1, CounterArg{{"label1", "value1"}})   \
    M(tiflash_test_counter_with_2_labels, "Test counter metric with 2 labels", Counter, 2, CounterArg{{"label1", "value1"}}, \
        CounterArg{{"label21", "value21"}, {"label22", "value22"}})                                                          \
    M(tiflash_test_gauge, "Test gauge metric w/o labels", Gauge, 0)                                                          \
    M(tiflash_test_gauge_with_1_label, "Test gauge metric with 1 label", Gauge, 1, GaugeArg{{"label1", "value1"}})           \
    M(tiflash_test_gauge_with_2_labels, "Test gauge metric with 2 labels", Gauge, 2, GaugeArg{{"label1", "value1"}},         \
        GaugeArg{{"label21", "value22"}, {"label22", "value22"}})                                                            \
    M(tiflash_test_histogram, "Test histogram metric w/o labels", Histogram, 0)                                              \
    M(tiflash_test_histogram_with_1_label, "Test histogram metric with 1 label", Histogram, 1,                               \
        HistogramArg{{{"label1", "value1"}}, {100}})                                                                         \
    M(tiflash_test_histogram_with_2_labels, "Test histogram metric with 2 labels", Histogram, 2,                             \
        HistogramArg{{{"label1", "value1"}}, {0.1, 0.2, 0.3, 0.4}},                                                          \
        HistogramArg{{{"label21", "value21"}, {"label22", "value22"}}, {1, 2, 3, 4}})

template <typename T, typename First, typename... Rest>
void addMetricsCommon(T * dst[], size_t i, prometheus::Family<T> & family, First && first, Rest &&... rest);
template <typename T, typename Arg>
void addMetricsCommon(T * dst[], size_t i, prometheus::Family<T> & family, Arg && arg);

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
    template <typename... Args>
    static void addMetrics(MetricType * dst[], prometheus::Family<MetricType> & family, Args &&... args)
    {
        addMetricsCommon(dst, 0, family, std::forward<Args>(args)...);
    }
};
template <>
struct MetricFamilyTrait<prometheus::Gauge>
{
    using MetricType = prometheus::Gauge;
    using ArgType = std::map<std::string, std::string>;
    static auto build() { return prometheus::BuildGauge(); }
    template <typename... Args>
    static void addMetrics(MetricType * dst[], prometheus::Family<MetricType> & family, Args &&... args)
    {
        addMetricsCommon(dst, 0, family, std::forward<Args>(args)...);
    }
};
template <>
struct MetricFamilyTrait<prometheus::Histogram>
{
    using MetricType = prometheus::Histogram;
    using ArgType = std::tuple<std::map<std::string, std::string>, prometheus::Histogram::BucketBoundaries>;
    static auto build() { return prometheus::BuildHistogram(); }
    template <typename... Args>
    static void addMetrics(MetricType * dst[], prometheus::Family<MetricType> & family, Args &&... args)
    {
        addMetricsCommon(dst, 0, family, std::forward<Args>(args)...);
    }
};

using CounterArg = typename MetricFamilyTrait<prometheus::Counter>::ArgType;
using GaugeArg = typename MetricFamilyTrait<prometheus::Gauge>::ArgType;
using HistogramArg = typename MetricFamilyTrait<prometheus::Histogram>::ArgType;

template <typename T, typename Arg>
void addMetricsCommon(T * dst[], size_t i, prometheus::Family<T> & family, Arg && arg)
{
    dst[i] = &family.Add(std::forward<Arg>(arg));
}
template <>
void addMetricsCommon<prometheus::Histogram, MetricFamilyTrait<prometheus::Histogram>::ArgType>(prometheus::Histogram * dst[], size_t i,
    prometheus::Family<prometheus::Histogram> & family, MetricFamilyTrait<prometheus::Histogram>::ArgType && arg)
{
    HistogramArg args{std::forward<HistogramArg>(arg)};
    dst[i] = &family.Add(std::move(std::get<0>(args)), std::move(std::get<1>(args)));
}
template <typename T, typename First, typename... Rest>
void addMetricsCommon(T * dst[], size_t i, prometheus::Family<T> & family, First && first, Rest &&... rest)
{
    addMetricsCommon(dst, i, family, std::forward<First>(first));
    addMetricsCommon(dst, i + 1, family, std::forward<Rest>(rest)...);
}

template <typename T, size_t n, typename = void>
struct MetricFamily
{
};

template <typename T, size_t n>
struct MetricFamily<T, n, std::enable_if_t<n != 0>>
{
    template <typename... Args>
    MetricFamily(prometheus::Registry & registry, const std::string & name, const std::string & help, Args &&... args)
    {
        static_assert(sizeof...(Args) == n);
        auto & family = MetricFamilyTrait<T>::build().Name(name).Help(help).Register(registry);
        MetricFamilyTrait<T>::addMetrics(metrics, family, std::forward<Args>(args)...);
    }

    template <size_t idx = 0>
    T & get()
    {
        static_assert(idx < n);
        return *metrics[idx];
    }

private:
    T * metrics[n];
};

template <typename T, size_t n>
struct MetricFamily<T, n, std::enable_if_t<n == 0>>
{
    MetricFamily(prometheus::Registry & registry, const std::string & name, const std::string & help)
    {
        auto & family = MetricFamilyTrait<T>::build().Name(name).Help(help).Register(registry);
        MetricFamilyTrait<T>::addMetrics(&metric, family, typename MetricFamilyTrait<T>::ArgType{});
    }

    T & get() { return *metric; }

private:
    T * metric;
};

} // namespace DB
