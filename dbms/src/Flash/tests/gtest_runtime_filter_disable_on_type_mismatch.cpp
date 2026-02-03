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

#include <Debug/MockRuntimeFilter.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>

namespace DB
{
namespace tests
{

/// Coverage for the conservative guard in `PhysicalJoin::build`:
/// - If join-side key protobuf field types are not compatible, we should skip creating/registering runtime filter.
///
/// Behavioral contract in this test:
/// - We still attach a RuntimeFilter request in the mock DAG.
/// - Because key types mismatch, the optimization should be disabled (i.e. no filtering happens).
/// - The join result should remain correct (same row count as “without runtime filter”).
class RuntimeFilterDisableOnTypeMismatchTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.mockStorage()->setUseDeltaMerge(true);
    }

    static constexpr size_t concurrency = 10;
};

#define WrapForRuntimeFilterTestBegin             \
    std::vector<bool> pipelineBools{false, true}; \
    for (auto enablePipelineFlag : pipelineBools) \
    {                                             \
        enablePipeline(enablePipelineFlag);

#define WrapForRuntimeFilterTestEnd }

TEST_F(RuntimeFilterDisableOnTypeMismatchTestRunner, DisableRuntimeFilterWhenJoinKeyFieldTypeMismatch)
try
{
    context.context->getSettingsRef().dt_segment_stable_pack_rows = 1;
    context.context->getSettingsRef().dt_segment_limit_rows = 1;
    context.context->getSettingsRef().dt_segment_delta_cache_limit_rows = 1;
    context.context->getSettingsRef().dt_segment_force_split_size = 70;
    context.context->getSettingsRef().enable_hash_join_v2 = false;

    // Probe(left) join key: Int32
    // Note: When using DeltaMerge in tests, the primary key column is expected to be representable by integer.
    // So we add an explicit integer handle column `pk` as the primary key and keep `k1` as a normal column.
    context.addMockDeltaMerge(
        {"test_db", "left_table"},
        {{"pk", TiDB::TP::TypeLongLong, false}, {"k1", TiDB::TP::TypeLong}},
        {toVec<Int64>("pk", {1, 2, 3}), toNullableVec<Int32>("k1", {1, 2, 3})},
        concurrency);

    // Build(right) join key: Int64 (mismatch)
    context.addExchangeReceiver(
        "right_exchange_table_i64",
        {{"k1", TiDB::TP::TypeLongLong}},
        {toNullableVec<Int64>("k1", {2, 2, 3, 4})});

    // Build(right) join key: UInt32 (mismatch due to unsigned flag)
    context.addExchangeReceiver(
        "right_exchange_table_u32",
        {{"k1", TiDB::TP::TypeLong, true}},
        {toNullableVec<UInt32>("k1", {2, 2, 3, 4})});

    WrapForRuntimeFilterTestBegin
    {
        // Baseline: without runtime filter.
        auto request
            = context.scan("test_db", "left_table")
                  .join(context.receive("right_exchange_table_i64"), tipb::JoinType::TypeInnerJoin, {col("k1")})
                  .build(context);
        Expect expect{
            {"table_scan_0", {not_check_rows, not_check_concurrency}},
            {"exchange_receiver_1", {4, concurrency}},
            {"Join_2", {3, concurrency}}};
        testForExecutionSummary(request, expect);
    }

    {
        // With runtime filter requested, but type mismatch => runtime filter should be disabled.
        mock::MockRuntimeFilter rf(1, col("k1"), col("k1"), "exchange_receiver_1", "table_scan_0");
        auto request
            = context.scan("test_db", "left_table", std::vector<int>{1})
                  .join(context.receive("right_exchange_table_i64"), tipb::JoinType::TypeInnerJoin, {col("k1")}, rf)
                  .build(context);
        // Expect no RF pruning, same as baseline.
        Expect expect{
            {"table_scan_0", {not_check_rows, not_check_concurrency}},
            {"exchange_receiver_1", {4, concurrency}},
            {"Join_2", {3, concurrency}}};
        testForExecutionSummary(request, expect);
    }

    {
        // With runtime filter requested, but signed/unsigned mismatch => runtime filter should be disabled.
        mock::MockRuntimeFilter rf(1, col("k1"), col("k1"), "exchange_receiver_1", "table_scan_0");
        auto request
            = context.scan("test_db", "left_table", std::vector<int>{1})
                  .join(context.receive("right_exchange_table_u32"), tipb::JoinType::TypeInnerJoin, {col("k1")}, rf)
                  .build(context);
        // Expect no RF pruning, same as baseline.
        Expect expect{
            {"table_scan_0", {not_check_rows, not_check_concurrency}},
            {"exchange_receiver_1", {4, concurrency}},
            {"Join_2", {3, concurrency}}};
        testForExecutionSummary(request, expect);
    }
    WrapForRuntimeFilterTestEnd
}
CATCH

#undef WrapForRuntimeFilterTestBegin
#undef WrapForRuntimeFilterTestEnd

} // namespace tests
} // namespace DB
