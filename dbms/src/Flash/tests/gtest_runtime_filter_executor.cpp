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

#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>

namespace DB
{
namespace tests
{
class RuntimeFilterExecutorTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.mockStorage()->setUseDeltaMerge(true);
    }

    static constexpr size_t concurrency = 10;
};

#define WRAP_FOR_RF_TEST_BEGIN                     \
    std::vector<bool> pipeline_bools{false, true}; \
    for (auto enable_pipeline : pipeline_bools)    \
    {                                              \
        enablePipeline(enable_pipeline);

#define WRAP_FOR_RF_TEST_END }

TEST_F(RuntimeFilterExecutorTestRunner, RuntimeFilterTest)
try
{
    context.context->getSettingsRef().dt_segment_stable_pack_rows = 1;
    context.context->getSettingsRef().dt_segment_limit_rows = 1;
    context.context->getSettingsRef().dt_segment_delta_cache_limit_rows = 1;
    context.context->getSettingsRef().dt_segment_force_split_size = 70;
    context.addMockDeltaMerge(
        {"test_db", "left_table"},
        {{"col0", TiDB::TP::TypeLongLong}, {"k1", TiDB::TP::TypeLong}, {"k2", TiDB::TP::TypeLong}},
        {toVec<Int64>("col0", {0, 1, 2}), toNullableVec<Int32>("k1", {1, 2, 3}), toNullableVec<Int32>("k2", {1, 2, 3})},
        concurrency);

    context.addExchangeReceiver(
        "right_exchange_table",
        {{"k1", TiDB::TP::TypeLong}, {"k2", TiDB::TP::TypeLong}},
        {toNullableVec<Int32>("k1", {2, 2, 3, 4}), toNullableVec<Int32>("k2", {2, 2, 3, 4})});
    context.addExchangeReceiver("right_empty_table", {{"k1", TiDB::TP::TypeLong}, {"k2", TiDB::TP::TypeLong}});

    WRAP_FOR_RF_TEST_BEGIN
    {
        // without runtime filter, table_scan_0 return 3 rows
        auto request = context.scan("test_db", "left_table")
                           .join(context.receive("right_exchange_table"), tipb::JoinType::TypeInnerJoin, {col("k1")})
                           .build(context);
        Expect expect{
            {"table_scan_0", {3, enable_pipeline ? concurrency : 1}},
            {"exchange_receiver_1", {4, concurrency}},
            {"Join_2", {3, concurrency}}};
        testForExecutionSummary(request, expect);
    }

    {
        // with runtime filter, table_scan_0 return 2 rows
        mock::MockRuntimeFilter rf(1, col("k1"), col("k1"), "exchange_receiver_1", "table_scan_0");
        auto request
            = context.scan("test_db", "left_table", std::vector<int>{1})
                  .join(context.receive("right_exchange_table"), tipb::JoinType::TypeInnerJoin, {col("k1")}, rf)
                  .build(context);
        Expect expect{
            {"table_scan_0", {2, enable_pipeline ? concurrency : 1}},
            {"exchange_receiver_1", {4, concurrency}},
            {"Join_2", {3, concurrency}}};
        testForExecutionSummary(request, expect);
    }

    {
        // issue #45300
        // test empty build side, with runtime filter, table_scan_0 return 0 rows
        mock::MockRuntimeFilter rf(1, col("k1"), col("k1"), "exchange_receiver_1", "table_scan_0");
        auto request = context.scan("test_db", "left_table", std::vector<int>{1})
                           .join(context.receive("right_empty_table"), tipb::JoinType::TypeInnerJoin, {col("k1")}, rf)
                           .build(context);
        Expect expect{
            {"table_scan_0", {0, enable_pipeline ? concurrency : 1}},
            {"exchange_receiver_1", {0, concurrency}},
            {"Join_2", {0, concurrency}}};
        testForExecutionSummary(request, expect);
    }
    WRAP_FOR_RF_TEST_END
}
CATCH

#undef WRAP_FOR_RF_TEST_BEGIN
#undef WRAP_FOR_RF_TEST_END

} // namespace tests
} // namespace DB
