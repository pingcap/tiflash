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

#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>

namespace DB::tests
{
class FineGrainedShuffleTestRunner : public DB::tests::ExecutorTest
{
    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        DB::MockColumnInfoVec column_infos{{"partition", TiDB::TP::TypeLong}, {"value", TiDB::TP::TypeLong}};
        DB::MockColumnInfoVec partition_column_infos{{"partition", TiDB::TP::TypeLong}};
        ColumnsWithTypeAndName column_data;
        ColumnsWithTypeAndName common_column_data;
        size_t table_rows = 1024;
        for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(column_infos))
        {
            ColumnGeneratorOpts opts{
                table_rows,
                getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
                RANDOM,
                column_info.name};
            column_data.push_back(ColumnGenerator::instance().generate(opts));
        }
        ColumnWithTypeAndName shuffle_column = ColumnGenerator::instance().generate({table_rows, "UInt64", RANDOM});
        IColumn::Permutation perm;
        shuffle_column.column->getPermutation(false, 0, -1, perm);
        for (auto & column : column_data)
        {
            column.column = column.column->permute(perm, 0);
        }

        context.addExchangeReceiver(
            "exchange_receiver_1_concurrency",
            column_infos,
            column_data,
            1,
            partition_column_infos);
        context.addExchangeReceiver(
            "exchange_receiver_3_concurrency",
            column_infos,
            column_data,
            3,
            partition_column_infos);
        context.addExchangeReceiver(
            "exchange_receiver_5_concurrency",
            column_infos,
            column_data,
            5,
            partition_column_infos);
        context.addExchangeReceiver(
            "exchange_receiver_10_concurrency",
            column_infos,
            column_data,
            10,
            partition_column_infos);
    }
};

TEST_F(FineGrainedShuffleTestRunner, simpleReceiver)
try
{
    std::vector<size_t> exchange_receiver_concurrency = {1, 3, 5, 10};

    auto gen_request = [&](size_t exchange_concurrency) {
        return context
            .receive(fmt::format("exchange_receiver_{}_concurrency", exchange_concurrency), exchange_concurrency)
            .build(context);
    };

    auto baseline = executeStreams(gen_request(1), 1);
    for (size_t exchange_concurrency : exchange_receiver_concurrency)
    {
        executeAndAssertColumnsEqual(gen_request(exchange_concurrency), baseline);
    }
}
CATCH

TEST_F(FineGrainedShuffleTestRunner, FineGrainedShuffleReceiverAndThenNonFineGrainedShuffleAgg)
try
{
    std::vector<size_t> exchange_receiver_concurrency = {1, 3, 5, 10};

    auto gen_request = [&](size_t exchange_concurrency) {
        return context
            .receive(fmt::format("exchange_receiver_{}_concurrency", exchange_concurrency), exchange_concurrency)
            .aggregation({Max(col("partition"))}, {col("value")})
            .build(context);
    };

    auto baseline = executeStreams(gen_request(1), 1);
    for (size_t exchange_concurrency : exchange_receiver_concurrency)
    {
        executeAndAssertColumnsEqual(gen_request(exchange_concurrency), baseline);
    }
}
CATCH

} // namespace DB::tests
