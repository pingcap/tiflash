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

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <benchmark/benchmark.h>
#include <google/protobuf/util/json_util.h>
#include <tipb/executor.pb.h>

#include <random>
#include <string>

namespace DB
{
namespace tests
{
static const std::string agg_tipb_json
    = R"({"groupBy":[{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":15,"flag":0,"flen":32,"decimal":0,"collate":-46,"charset":"utf8mb4","array":false},"hasDistinct":false},{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":15,"flag":0,"flen":64,"decimal":0,"collate":-46,"charset":"utf8mb4","array":false},"hasDistinct":false},{"tp":"ColumnRef","val":"gAAAAAAAAAI=","sig":"Unspecified","fieldType":{"tp":15,"flag":0,"flen":16,"decimal":0,"collate":-46,"charset":"utf8mb4","array":false},"hasDistinct":false},{"tp":"ColumnRef","val":"gAAAAAAAAAM=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":-63,"charset":"binary","array":false},"hasDistinct":false},{"tp":"ColumnRef","val":"gAAAAAAAAAQ=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":-63,"charset":"binary","array":false},"hasDistinct":false},{"tp":"ColumnRef","val":"gAAAAAAAAAU=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":-63,"charset":"binary","array":false},"hasDistinct":false}],"aggFunc":[{"tp":"Sum","children":[{"tp":"ColumnRef","val":"gAAAAAAAAAY=","sig":"Unspecified","fieldType":{"tp":246,"flag":0,"flen":19,"decimal":6,"collate":-63,"charset":"binary","array":false},"hasDistinct":false}],"sig":"Unspecified","fieldType":{"tp":246,"flag":128,"flen":41,"decimal":6,"collate":-63,"charset":"binary","array":false},"hasDistinct":false,"aggFuncMode":"CompleteMode"},{"tp":"Sum","children":[{"tp":"ColumnRef","val":"gAAAAAAAAAc=","sig":"Unspecified","fieldType":{"tp":246,"flag":0,"flen":19,"decimal":6,"collate":-63,"charset":"binary","array":false},"hasDistinct":false}],"sig":"Unspecified","fieldType":{"tp":246,"flag":128,"flen":41,"decimal":6,"collate":-63,"charset":"binary","array":false},"hasDistinct":false,"aggFuncMode":"CompleteMode"},{"tp":"Sum","children":[{"tp":"ColumnRef","val":"gAAAAAAAAAg=","sig":"Unspecified","fieldType":{"tp":246,"flag":0,"flen":19,"decimal":6,"collate":-63,"charset":"binary","array":false},"hasDistinct":false}],"sig":"Unspecified","fieldType":{"tp":246,"flag":128,"flen":41,"decimal":6,"collate":-63,"charset":"binary","array":false},"hasDistinct":false,"aggFuncMode":"CompleteMode"},{"tp":"First","children":[{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":15,"flag":0,"flen":32,"decimal":0,"collate":-46,"charset":"utf8mb4","array":false},"hasDistinct":false}],"sig":"Unspecified","fieldType":{"tp":15,"flag":0,"flen":32,"decimal":0,"collate":-46,"charset":"utf8mb4","array":false},"hasDistinct":false,"aggFuncMode":"CompleteMode"},{"tp":"First","children":[{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":15,"flag":0,"flen":64,"decimal":0,"collate":-46,"charset":"utf8mb4","array":false},"hasDistinct":false}],"sig":"Unspecified","fieldType":{"tp":15,"flag":0,"flen":64,"decimal":0,"collate":-46,"charset":"utf8mb4","array":false},"hasDistinct":false,"aggFuncMode":"CompleteMode"},{"tp":"First","children":[{"tp":"ColumnRef","val":"gAAAAAAAAAI=","sig":"Unspecified","fieldType":{"tp":15,"flag":0,"flen":16,"decimal":0,"collate":-46,"charset":"utf8mb4","array":false},"hasDistinct":false}],"sig":"Unspecified","fieldType":{"tp":15,"flag":0,"flen":16,"decimal":0,"collate":-46,"charset":"utf8mb4","array":false},"hasDistinct":false,"aggFuncMode":"CompleteMode"},{"tp":"First","children":[{"tp":"ColumnRef","val":"gAAAAAAAAAM=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":-63,"charset":"binary","array":false},"hasDistinct":false}],"sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":-63,"charset":"binary","array":false},"hasDistinct":false,"aggFuncMode":"CompleteMode"},{"tp":"First","children":[{"tp":"ColumnRef","val":"gAAAAAAAAAQ=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":-63,"charset":"binary","array":false},"hasDistinct":false}],"sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":-63,"charset":"binary","array":false},"hasDistinct":false,"aggFuncMode":"CompleteMode"},{"tp":"First","children":[{"tp":"ColumnRef","val":"gAAAAAAAAAU=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":-63,"charset":"binary","array":false},"hasDistinct":false}],"sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":-63,"charset":"binary","array":false},"hasDistinct":false,"aggFuncMode":"CompleteMode"}],"streamed":false,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIGA4NK8qMydBhABIg4xMjcuMC4wLjE6MzkzMCgBMJGZlt/io8viFzgHQIgOSAI="],"fieldTypes":[{"tp":15,"flag":0,"flen":32,"decimal":0,"collate":-46,"charset":"utf8mb4","array":false},{"tp":15,"flag":0,"flen":64,"decimal":0,"collate":-46,"charset":"utf8mb4","array":false},{"tp":15,"flag":0,"flen":16,"decimal":0,"collate":-46,"charset":"utf8mb4","array":false},{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":-63,"charset":"binary","array":false},{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":-63,"charset":"binary","array":false},{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":-63,"charset":"binary","array":false},{"tp":246,"flag":0,"flen":19,"decimal":6,"collate":-63,"charset":"binary","array":false},{"tp":246,"flag":0,"flen":19,"decimal":6,"collate":-63,"charset":"binary","array":false},{"tp":246,"flag":0,"flen":19,"decimal":6,"collate":-63,"charset":"binary","array":false}]},"executorId":"ExchangeReceiver_18","fineGrainedShuffleStreamCount":"10","fineGrainedShuffleBatchSize":"8192"}})";

using BlockPtr = std::shared_ptr<Block>;

template <typename T>
std::vector<T> getRandomInt(T min, T max, size_t n)
{
    static_assert(std::is_integral<T>::value, "arguments of getRandomInt() should be integral");

    assert(max > min);
    assert(n > 0);

    std::mt19937_64 gen(123);
    std::uniform_int_distribution<T> dist(min, max);

    std::vector<T> results;
    results.reserve(n);
    for (size_t i = 0; i < n; ++i)
    {
        results.push_back(dist(gen));
    }
    return results;
}

std::vector<std::string> getRandomStr(size_t min_len, size_t max_len, size_t n)
{
    constexpr static const char letters[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    auto size_vec = getRandomInt<size_t>(min_len, max_len, n);

    std::vector<std::string> results;
    results.reserve(n);

    std::mt19937_64 gen(123);

    for (auto len : size_vec)
    {
        std::uniform_int_distribution<size_t> dist(0, sizeof(letters) / sizeof(char) - 1);
        std::string str;
        str.reserve(len);
        for (size_t i = 0; i < len; ++i)
        {
            str.push_back(letters[dist(gen)]);
        }
        results.push_back(str);
    }
    return results;
}

std::vector<std::string> getRandomCurrency(size_t total_rows)
{
    const static std::vector<std::string> currency{"USD", "EUR", "GBP", "JPY", "CNY", "CAD", "AUD",
                                                   "CHF", "NZD", "ZAR", "SEK", "RUB", "BRL", "INR",
                                                   "SGD", "KRW", "MXN", "TRY", "AED", "ARS"};
    auto index_vec = getRandomInt<UInt32>(0, currency.size() - 1, total_rows);
    std::vector<std::string> results;
    results.reserve(total_rows);
    for (auto idx : index_vec)
    {
        results.push_back(currency[idx]);
    }
    return results;
}

class BenchProbeAggHashMap : public ::benchmark::Fixture
{
public:
    void SetUp(const ::benchmark::State &) override
    {
        try
        {
            if (log != nullptr) // already inited.
                return;

            log = Logger::get("BenchProbeAggHashMap");
            ::DB::registerAggregateFunctions();

            test_blocks = generateData(20000000, 4096);

            context = TiFlashTestEnv::getContext();

            auto src_header = test_blocks[0]->cloneEmpty();
            ColumnNumbers keys{0, 1, 2, 3, 4, 5};

            auto data_type_string = std::make_shared<DataTypeString>();
            auto data_type_int = std::make_shared<DataTypeInt32>();
            auto data_type_decimal = std::make_shared<DataTypeDecimal128>();
            ::tipb::Aggregation agg_tipb;
            ::google::protobuf::util::JsonStringToMessage(agg_tipb_json, &agg_tipb);
            DAGExpressionAnalyzer analyzer(src_header, *context);
            ExpressionActionsPtr before_agg_actions = PhysicalPlanHelper::newActions(src_header);
            AggregateDescriptions aggregate_desc;
            NamesAndTypes aggregate_output_columns;
            Names aggregation_keys;
            TiDB::TiDBCollators collators;
            std::unordered_set<String> agg_key_set;
            KeyRefAggFuncMap key_ref_agg_func;
            AggFuncRefKeyMap agg_func_ref_key;
            analyzer.buildAggFuncs(agg_tipb, before_agg_actions, aggregate_desc, aggregate_output_columns);
            analyzer.buildAggGroupBy(
                agg_tipb.group_by(),
                before_agg_actions,
                aggregate_desc,
                aggregate_output_columns,
                aggregation_keys,
                agg_key_set,
                /*collation sensitive*/ true,
                collators);
            analyzer.tryRemoveGroupByKeyWithCollator(
                aggregation_keys,
                collators,
                aggregate_desc,
                aggregate_output_columns,
                key_ref_agg_func);
            analyzer.tryRemoveFirstRow(
                aggregation_keys,
                collators,
                aggregate_desc,
                aggregate_output_columns,
                agg_func_ref_key);

            // Fill argument number of agg func.
            AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_desc, src_header);
            SpillConfig spill_config(
                context->getTemporaryPath(),
                fmt::format("{}_aggregation", log->identifier()),
                context->getSettingsRef().max_cached_data_bytes_in_spiller,
                context->getSettingsRef().max_spilled_rows_per_file,
                context->getSettingsRef().max_spilled_bytes_per_file,
                context->getFileProvider(),
                context->getSettingsRef().max_threads,
                context->getSettingsRef().max_block_size);
            params = AggregationInterpreterHelper::buildParams(
                *context,
                src_header,
                /*before_agg_streams_size*/ 1,
                /*agg_streams_size*/ 1,
                aggregation_keys,
                key_ref_agg_func,
                agg_func_ref_key,
                collators,
                aggregate_desc,
                /*is_final_agg*/ true,
                spill_config);
        }
        CATCH
    }

    void TearDown(benchmark::State &) override
    {
        // Make sure context/TMTContext is destroyed before TiFlashMetrics.
        context.reset();
    }

    static std::vector<BlockPtr> generateData(size_t total_rows, size_t rows_per_block)
    {
        auto data_type_string = std::make_shared<DataTypeString>();
        auto data_type_int = std::make_shared<DataTypeInt32>();
        auto data_type_decimal = std::make_shared<DataTypeDecimal128>();
        auto data_type_date = std::make_shared<DataTypeDate>();

        std::vector<std::string> random_str_2 = getRandomCurrency(total_rows);
        std::vector<std::string> random_str_3 = getRandomStr(/*min_len*/ 1, /*max_len*/ 50, total_rows);

        std::vector<int32_t> random_int_1 = getRandomInt<int32_t>(/*min*/ 1, /*max*/ 50, total_rows);
        std::vector<int32_t> random_int_2 = getRandomInt<int32_t>(/*min*/ 1, /*max*/ 50, total_rows);
        std::vector<int32_t> random_int_3 = getRandomInt<int32_t>(/*min*/ 1, /*max*/ 50, total_rows);

        auto random_prec_1 = getRandomInt<Int128>(/*min*/ 1, /*max*/ 999999999, total_rows);
        auto random_scale_1 = getRandomInt<UInt32>(/*min*/ 1, /*max*/ 6, total_rows);
        auto random_prec_2 = getRandomInt<Int128>(/*min*/ 1, /*max*/ 999999999, total_rows);
        auto random_scale_2 = getRandomInt<UInt32>(/*min*/ 1, /*max*/ 6, total_rows);
        auto random_prec_3 = getRandomInt<Int128>(/*min*/ 1, /*max*/ 999999999, total_rows);
        auto random_scale_3 = getRandomInt<UInt32>(/*min*/ 1, /*max*/ 6, total_rows);

        MutableColumnPtr col_varchar_1;
        MutableColumnPtr col_varchar_2;
        MutableColumnPtr col_varchar_3;
        MutableColumnPtr col_int_1;
        MutableColumnPtr col_int_2;
        MutableColumnPtr col_int_3;
        MutableColumnPtr col_decimal_1;
        MutableColumnPtr col_decimal_2;
        MutableColumnPtr col_decimal_3;

        std::vector<BlockPtr> blocks;
        blocks.reserve(total_rows / rows_per_block);
        BlockPtr cur_block;

        for (size_t i = 0; i < total_rows; ++i)
        {
            if (col_varchar_1 == nullptr || col_varchar_1->size() == rows_per_block)
            {
                if (col_varchar_1 != nullptr)
                {
                    ColumnsWithTypeAndName cols{
                        ColumnWithTypeAndName(std::move(col_varchar_1), data_type_string, "col_varchar_1"),
                        ColumnWithTypeAndName(std::move(col_varchar_2), data_type_string, "col_varchar_2"),
                        ColumnWithTypeAndName(std::move(col_varchar_3), data_type_string, "col_varchar_3"),

                        ColumnWithTypeAndName(std::move(col_int_1), data_type_int, "col_int_1"),
                        ColumnWithTypeAndName(std::move(col_int_2), data_type_int, "col_int_2"),
                        ColumnWithTypeAndName(std::move(col_int_3), data_type_int, "col_int_3"),

                        ColumnWithTypeAndName(std::move(col_decimal_1), data_type_decimal, "col_decimal_1"),
                        ColumnWithTypeAndName(std::move(col_decimal_2), data_type_decimal, "col_decimal_2"),
                        ColumnWithTypeAndName(std::move(col_decimal_3), data_type_decimal, "col_decimal_3"),
                    };
                    blocks.push_back(std::make_shared<Block>(cols));
                }

                col_varchar_1 = data_type_string->createColumn();
                col_varchar_2 = data_type_string->createColumn();
                col_varchar_3 = data_type_string->createColumn();

                col_int_1 = data_type_int->createColumn();
                col_int_2 = data_type_int->createColumn();
                col_int_3 = data_type_int->createColumn();

                col_decimal_1 = data_type_decimal->createColumn();
                col_decimal_2 = data_type_decimal->createColumn();
                col_decimal_3 = data_type_decimal->createColumn();
            }

            col_varchar_1->insert(Field("111", 3));
            col_varchar_2->insert(Field(random_str_2[i].data(), random_str_2[i].size()));
            col_varchar_3->insert(Field(random_str_3[i].data(), random_str_3[i].size()));

            col_int_1->insert(Field(static_cast<Int64>(random_int_1[i])));
            col_int_2->insert(Field(static_cast<Int64>(random_int_2[i])));
            col_int_3->insert(Field(static_cast<Int64>(random_int_3[i])));

            col_decimal_1->insert(Field(DecimalField<Decimal128>(random_prec_1[i], random_scale_1[i])));
            col_decimal_2->insert(Field(DecimalField<Decimal128>(random_prec_2[i], random_scale_2[i])));
            col_decimal_3->insert(Field(DecimalField<Decimal128>(random_prec_3[i], random_scale_3[i])));
        }

        return blocks;
    }

    ContextPtr context;
    std::vector<BlockPtr> test_blocks;
    std::shared_ptr<Aggregator::Params> params;
    std::shared_ptr<Aggregator> aggregator;
    std::shared_ptr<AggregatedDataVariants> data_variants;
    LoggerPtr log;
};

BENCHMARK_DEFINE_F(BenchProbeAggHashMap, basic)(benchmark::State & state)
try
{
    for (const auto & _ : state)
    {
        // watchout order of data_variants and aggregator.
        // should destroy data_variants first.
        data_variants = std::make_shared<AggregatedDataVariants>();
        RegisterOperatorSpillContext register_operator_spill_context;
        aggregator = std::make_shared<Aggregator>(
            *params,
            "BenchProbeAggHashMap",
            /*concurrency*/ 1,
            register_operator_spill_context);
        data_variants->aggregator = aggregator.get();

        Aggregator::AggProcessInfo agg_process_info(aggregator.get());
        Stopwatch build_side_watch;
        {
            build_side_watch.start();
            for (auto & block : test_blocks)
            {
                agg_process_info.resetBlock(*block);
                aggregator->executeOnBlock(agg_process_info, *data_variants, 1);
            }
            build_side_watch.stop();
        }

        std::vector<AggregatedDataVariantsPtr> variants{data_variants};
        auto merging_buckets = aggregator->mergeAndConvertToBlocks(variants, true, 1);
        std::vector<Block> res_block;

        Stopwatch probe_side_watch;
        {
            probe_side_watch.start();
            for (;;)
            {
                auto block = merging_buckets->getData(0);
                if (!block)
                    break;
                res_block.push_back(block);
            }
            probe_side_watch.stop();
        }
        size_t total_rows = 0;
        for (const auto & block : res_block)
        {
            total_rows += block.rows();
        }
        LOG_DEBUG(log, "probe_side_watch: {}, res rows: {}", probe_side_watch.elapsed(), total_rows);
    }
}
CATCH
BENCHMARK_REGISTER_F(BenchProbeAggHashMap, basic);

} // namespace tests
} // namespace DB
