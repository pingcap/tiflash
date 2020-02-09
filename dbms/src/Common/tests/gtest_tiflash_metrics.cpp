#include <Common/TiFlashMetrics.h>
#include <gtest/gtest.h>

#include <ext/singleton.h>

namespace DB
{

namespace tests
{

#ifdef M
#error "Please undefine macro M first."
#endif
#undef APPLY_FOR_METRICS
#define APPLY_FOR_METRICS(M)                                                                                         \
    M(test_counter, "Test counter metric w/o labels", Counter, 0)                                                    \
    M(test_counter_with_1_label, "Test counter metric with 1 label", Counter, 1, CounterArg{{"label1", "value1"}})   \
    M(test_counter_with_2_labels, "Test counter metric with 2 labels", Counter, 2, CounterArg{{"label1", "value1"}}, \
        CounterArg{{"label21", "value21"}, {"label22", "value22"}})                                                  \
    M(test_gauge, "Test gauge metric w/o labels", Gauge, 0)                                                          \
    M(test_gauge_with_1_label, "Test gauge metric with 1 label", Gauge, 1, GaugeArg{{"label1", "value1"}})           \
    M(test_gauge_with_2_labels, "Test gauge metric with 2 labels", Gauge, 2, GaugeArg{{"label1", "value1"}},         \
        GaugeArg{{"label21", "value22"}, {"label22", "value22"}})                                                    \
    M(test_histogram, "Test histogram metric w/o labels", Histogram, 0)                                              \
    M(test_histogram_with_1_label, "Test histogram metric with 1 label", Histogram, 1,                               \
        HistogramArg{{{"label1", "value1"}}, ExpBuckets{1.0, 2, 1024}})                                              \
    M(test_histogram_with_2_labels, "Test histogram metric with 2 labels", Histogram, 2,                             \
        HistogramArg{{{"label1", "value1"}}, ExpBuckets{1.0, 2, 1024}},                                              \
        HistogramArg{{{"label21", "value21"}, {"label22", "value22"}}, {1, 2, 3, 4}})

class TestMetrics : public ext::singleton<TestMetrics>
{
public:
    std::shared_ptr<prometheus::Registry> registry = std::make_shared<prometheus::Registry>();

public:
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(name, help, type, n, ...) \
    MetricFamily<prometheus::type, n> name = MetricFamily<prometheus::type, n>(*registry, #name, #help, ##__VA_ARGS__);
    APPLY_FOR_METRICS(M)
#undef M
#undef APPLY_FOR_METRICS
};

TEST(TiFlashMetrics, Counter)
{
    ASSERT_NO_THROW(TestMetrics::instance().test_counter.get().Increment(0));
    ASSERT_NO_THROW(TestMetrics::instance().test_counter.get<0>().Increment(1));
    ASSERT_DOUBLE_EQ(TestMetrics::instance().test_counter.get().Value(), 1);

    ASSERT_NO_THROW(TestMetrics::instance().test_counter_with_1_label.get().Increment(0));
    ASSERT_NO_THROW(TestMetrics::instance().test_counter_with_1_label.get<0>().Increment(2));
    ASSERT_DOUBLE_EQ(TestMetrics::instance().test_counter_with_1_label.get().Value(), 2);

    ASSERT_NO_THROW(TestMetrics::instance().test_counter_with_2_labels.get().Increment(2));
    ASSERT_DOUBLE_EQ(TestMetrics::instance().test_counter_with_2_labels.get<0>().Value(), 2);
    ASSERT_NO_THROW(TestMetrics::instance().test_counter_with_2_labels.get<1>().Increment(3));
    ASSERT_DOUBLE_EQ(TestMetrics::instance().test_counter_with_2_labels.get<1>().Value(), 3);
}

TEST(TiFlashMetrics, Gauge)
{
    ASSERT_NO_THROW(TestMetrics::instance().test_gauge.get().Set(10));
    ASSERT_NO_THROW(TestMetrics::instance().test_gauge.get<0>().Increment(1));
    ASSERT_DOUBLE_EQ(TestMetrics::instance().test_gauge.get().Value(), 11);

    ASSERT_NO_THROW(TestMetrics::instance().test_gauge_with_1_label.get().Set(-10));
    ASSERT_NO_THROW(TestMetrics::instance().test_gauge_with_1_label.get<0>().Increment(2));
    ASSERT_DOUBLE_EQ(TestMetrics::instance().test_gauge_with_1_label.get().Value(), -8);

    ASSERT_NO_THROW(TestMetrics::instance().test_gauge_with_2_labels.get().Set(2));
    ASSERT_DOUBLE_EQ(TestMetrics::instance().test_gauge_with_2_labels.get<0>().Value(), 2);
    ASSERT_NO_THROW(TestMetrics::instance().test_gauge_with_2_labels.get<1>().Set(3));
    ASSERT_DOUBLE_EQ(TestMetrics::instance().test_gauge_with_2_labels.get<1>().Value(), 3);
}

TEST(TiFlashMetrics, Histogram)
{
    ASSERT_NO_THROW(TestMetrics::instance().test_histogram.get().Observe(0.5));
    ASSERT_NO_THROW(TestMetrics::instance().test_histogram.get<0>().Observe(0.5));

    ASSERT_NO_THROW(TestMetrics::instance().test_histogram_with_1_label.get().Observe(-10));
    ASSERT_NO_THROW(TestMetrics::instance().test_histogram_with_1_label.get<0>().Observe(2));

    ASSERT_NO_THROW(TestMetrics::instance().test_histogram_with_2_labels.get().Observe(2));
    ASSERT_NO_THROW(TestMetrics::instance().test_histogram_with_2_labels.get<1>().Observe(3));
}

} // namespace tests

} // namespace DB
