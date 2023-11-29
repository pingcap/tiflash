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

#define GTEST_TIFLASH_METRICS
#include <Common/TiFlashMetrics.h>
#include <gtest/gtest.h>

#include <ext/singleton.h>

namespace DB
{
namespace tests
{
#define TEST_APPLY_FOR_METRICS(M, F)                                                                       \
    M(test_counter, "Test counter metric w/o labels", Counter)                                             \
    M(test_counter_with_1_label, "Test counter metric with 1 label", Counter, F(m1, {"label1", "value1"})) \
    M(test_counter_with_2_labels,                                                                          \
      "Test counter metric with 2 labels",                                                                 \
      Counter,                                                                                             \
      F(m1, {"label1", "value1"}),                                                                         \
      F(m2, {"label21", "value21"}, {"label22", "value22"}))                                               \
    M(test_gauge, "Test gauge metric w/o labels", Gauge)                                                   \
    M(test_gauge_with_1_label, "Test gauge metric with 1 label", Gauge, F(m1, {"label1", "value1"}))       \
    M(test_gauge_with_2_labels,                                                                            \
      "Test gauge metric with 2 labels",                                                                   \
      Gauge,                                                                                               \
      F(m1, {"label1", "value1"}),                                                                         \
      F(m2, {"label21", "value22"}, {"label22", "value22"}))                                               \
    M(test_histogram, "Test histogram metric w/o labels", Histogram)                                       \
    M(test_histogram_with_1_label,                                                                         \
      "Test histogram metric with 1 label",                                                                \
      Histogram,                                                                                           \
      F(m1, {{"label1", "value1"}}, ExpBuckets{1.0, 2, 24}))                                               \
    M(test_histogram_with_2_labels,                                                                        \
      "Test histogram metric with 2 labels",                                                               \
      Histogram,                                                                                           \
      F(m1, {{"label1", "value1"}}, ExpBuckets{1.0, 2, 24}),                                               \
      F(m2, {{"label21", "value21"}, {"label22", "value22"}}, {1, 2, 3, 4}))

class TestMetrics : public ext::Singleton<TestMetrics>
{
public:
    std::shared_ptr<prometheus::Registry> registry = std::make_shared<prometheus::Registry>();

public:
    TEST_APPLY_FOR_METRICS(MAKE_METRIC_MEMBER_M, MAKE_METRIC_MEMBER_F)
};

TEST_APPLY_FOR_METRICS(MAKE_METRIC_ENUM_M, MAKE_METRIC_ENUM_F)

TEST(TiFlashMetrics, Counter)
{
    ASSERT_NO_THROW(GET_METRIC(test_counter).Increment(0));
    ASSERT_NO_THROW(GET_METRIC(test_counter).Increment(1));
    ASSERT_DOUBLE_EQ(GET_METRIC(test_counter).Value(), 1);

    ASSERT_NO_THROW(GET_METRIC(test_counter_with_1_label).Increment(0));
    ASSERT_NO_THROW(GET_METRIC(test_counter_with_1_label, m1).Increment(2));
    ASSERT_DOUBLE_EQ(GET_METRIC(test_counter_with_1_label).Value(), 2);

    ASSERT_NO_THROW(GET_METRIC(test_counter_with_2_labels).Increment(2));
    ASSERT_DOUBLE_EQ(GET_METRIC(test_counter_with_2_labels, m1).Value(), 2);
    ASSERT_NO_THROW(GET_METRIC(test_counter_with_2_labels, m2).Increment(3));
    ASSERT_DOUBLE_EQ(GET_METRIC(test_counter_with_2_labels, m2).Value(), 3);
}

TEST(TiFlashMetrics, Gauge)
{
    ASSERT_NO_THROW(GET_METRIC(test_gauge).Set(10));
    ASSERT_NO_THROW(GET_METRIC(test_gauge).Increment(1));
    ASSERT_DOUBLE_EQ(GET_METRIC(test_gauge).Value(), 11);

    ASSERT_NO_THROW(GET_METRIC(test_gauge_with_1_label).Set(-10));
    ASSERT_NO_THROW(GET_METRIC(test_gauge_with_1_label, m1).Increment(2));
    ASSERT_DOUBLE_EQ(GET_METRIC(test_gauge_with_1_label).Value(), -8);

    ASSERT_NO_THROW(GET_METRIC(test_gauge_with_2_labels).Set(2));
    ASSERT_DOUBLE_EQ(GET_METRIC(test_gauge_with_2_labels, m1).Value(), 2);
    ASSERT_NO_THROW(GET_METRIC(test_gauge_with_2_labels, m2).Set(3));
    ASSERT_DOUBLE_EQ(GET_METRIC(test_gauge_with_2_labels, m2).Value(), 3);
}

TEST(TiFlashMetrics, Histogram)
{
    ASSERT_NO_THROW(GET_METRIC(test_histogram).Observe(0.5));
    ASSERT_NO_THROW(GET_METRIC(test_histogram).Observe(0.5));

    ASSERT_NO_THROW(GET_METRIC(test_histogram_with_1_label).Observe(-10));
    ASSERT_NO_THROW(GET_METRIC(test_histogram_with_1_label, m1).Observe(2));

    ASSERT_NO_THROW(GET_METRIC(test_histogram_with_2_labels).Observe(2));
    ASSERT_NO_THROW(GET_METRIC(test_histogram_with_2_labels, m1).Observe(2));
    ASSERT_NO_THROW(GET_METRIC(test_histogram_with_2_labels, m2).Observe(3));
}

TEST(TiFlashMetrics, ExpBucketsWithRange)
{
    ASSERT_EQ(2, ExpBucketsWithRange::getSize(1.0, 2.0, 2)); // 1 2
    ASSERT_EQ(3, ExpBucketsWithRange::getSize(1.0, 3.0, 2)); // 1 2 4
    ASSERT_EQ(2, ExpBucketsWithRange::getSize(2.0, 3.0, 2)); // 2 4
    ASSERT_EQ(2, ExpBucketsWithRange::getSize(2.0, 4.0, 2)); // 2 4
    ASSERT_EQ(3, ExpBucketsWithRange::getSize(2.0, 5.0, 2)); // 2 4 8
    ASSERT_EQ(1, ExpBucketsWithRange::getSize(2.0, 2.0, 2)); // 2
    ASSERT_EQ(3, ExpBucketsWithRange::getSize(2.0, 12.0, 3)); // 2 6 18
    ASSERT_EQ(3, ExpBucketsWithRange::getSize(2.0, 18.0, 3)); // 2 6 18
    ASSERT_EQ(4, ExpBucketsWithRange::getSize(2.0, 19.0, 3)); // 2 6 18 54

    ASSERT_ANY_THROW({ ExpBucketsWithRange(2.0, 1.0, 3); });
    ASSERT_ANY_THROW({ ExpBucketsWithRange(2.0, 2.0, 2.0); });
}

} // namespace tests

} // namespace DB
