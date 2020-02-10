#include <Common/TiFlashMetrics.h>
#include <gtest/gtest.h>

#include <ext/singleton.h>

namespace DB
{

namespace tests
{

#ifdef F
#error "Please undefine macro F first."
#endif
#ifdef M
#error "Please undefine macro M first."
#endif
#undef APPLY_FOR_METRICS
#define APPLY_FOR_METRICS(M)                                                                                             \
    M(test_counter, "Test counter metric w/o labels", Counter, 0)                                                        \
    M(test_counter_with_1_label, "Test counter metric with 1 label", Counter, 1, F(m1, Counter, {"label1", "value1"}))   \
    M(test_counter_with_2_labels, "Test counter metric with 2 labels", Counter, 2, F(m1, Counter, {"label1", "value1"}), \
        F(m2, Counter, {"label21", "value21"}, {"label22", "value22"}))                                                  \
    M(test_gauge, "Test gauge metric w/o labels", Gauge, 0)                                                              \
    M(test_gauge_with_1_label, "Test gauge metric with 1 label", Gauge, 1, F(m1, Gauge, {"label1", "value1"}))           \
    M(test_gauge_with_2_labels, "Test gauge metric with 2 labels", Gauge, 2, F(m1, Gauge, {"label1", "value1"}),         \
        F(m2, Gauge, {"label21", "value22"}, {"label22", "value22"}))                                                    \
    M(test_histogram, "Test histogram metric w/o labels", Histogram, 0)                                                  \
    M(test_histogram_with_1_label, "Test histogram metric with 1 label", Histogram, 1,                                   \
        F(m1, Histogram, {{"label1", "value1"}}, ExpBuckets{1.0, 2, 1024}))                                              \
    M(test_histogram_with_2_labels, "Test histogram metric with 2 labels", Histogram, 2,                                 \
        F(m1, Histogram, {{"label1", "value1"}}, ExpBuckets{1.0, 2, 1024}),                                              \
        F(m2, Histogram, {{"label21", "value21"}, {"label22", "value22"}}, {1, 2, 3, 4}))

class TestMetrics : public ext::singleton<TestMetrics>
{
public:
    std::shared_ptr<prometheus::Registry> registry = std::make_shared<prometheus::Registry>();

public:
#ifdef F
#error "Please undefine macro F first."
#endif
#define F(field_name, type, ...) \
    type##Arg { __VA_ARGS__ }
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(family_name, help, type, n, ...) \
    MetricFamily<prometheus::type, n> family_name = MetricFamily<prometheus::type, n>(*registry, #family_name, #help, ##__VA_ARGS__);
    APPLY_FOR_METRICS(M)
#undef F
#undef M
};

#ifdef F
#error "Please undefine macro F first."
#endif
#define F(field_name, type, ...) field_name
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(family_name, help, type, n, ...) \
    namespace family_name##_metrics        \
    {                                      \
        enum                               \
        {                                  \
            invalid = -1,                  \
            ##__VA_ARGS__                  \
        };                                 \
    }
APPLY_FOR_METRICS(M)
#undef APPLY_FOR_METRICS
#undef F
#undef M

TEST(TiFlashMetrics, Counter)
{
    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_counter).Increment(0));
    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_counter).Increment(1));
    ASSERT_DOUBLE_EQ(GET_METRIC(&TestMetrics::instance(), test_counter).Value(), 1);

    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_counter_with_1_label).Increment(0));
    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_counter_with_1_label, m1).Increment(2));
    ASSERT_DOUBLE_EQ(GET_METRIC(&TestMetrics::instance(), test_counter_with_1_label).Value(), 2);

    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_counter_with_2_labels).Increment(2));
    ASSERT_DOUBLE_EQ(GET_METRIC(&TestMetrics::instance(), test_counter_with_2_labels, m1).Value(), 2);
    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_counter_with_2_labels, m2).Increment(3));
    ASSERT_DOUBLE_EQ(GET_METRIC(&TestMetrics::instance(), test_counter_with_2_labels, m2).Value(), 3);
}

TEST(TiFlashMetrics, Gauge)
{
    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_gauge).Set(10));
    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_gauge).Increment(1));
    ASSERT_DOUBLE_EQ(GET_METRIC(&TestMetrics::instance(), test_gauge).Value(), 11);

    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_gauge_with_1_label).Set(-10));
    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_gauge_with_1_label, m1).Increment(2));
    ASSERT_DOUBLE_EQ(GET_METRIC(&TestMetrics::instance(), test_gauge_with_1_label).Value(), -8);

    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_gauge_with_2_labels).Set(2));
    ASSERT_DOUBLE_EQ(GET_METRIC(&TestMetrics::instance(), test_gauge_with_2_labels, m1).Value(), 2);
    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_gauge_with_2_labels, m2).Set(3));
    ASSERT_DOUBLE_EQ(GET_METRIC(&TestMetrics::instance(), test_gauge_with_2_labels, m2).Value(), 3);
}

TEST(TiFlashMetrics, Histogram)
{
    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_histogram).Observe(0.5));
    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_histogram).Observe(0.5));

    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_histogram_with_1_label).Observe(-10));
    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_histogram_with_1_label, m1).Observe(2));

    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_histogram_with_2_labels).Observe(2));
    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_histogram_with_2_labels, m1).Observe(2));
    ASSERT_NO_THROW(GET_METRIC(&TestMetrics::instance(), test_histogram_with_2_labels, m2).Observe(3));
}

} // namespace tests

} // namespace DB
