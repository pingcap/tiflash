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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Interpreters/Context.h>
#include <Operators/AutoPassThroughHashAggContext.h>
#include <TestUtils/FailPointUtils.h>
#include <TestUtils/MPPTaskTestUtils.h>

#include <random>

namespace DB
{
namespace FailPoints
{
extern const char exception_before_mpp_register_non_root_mpp_task[];
extern const char exception_before_mpp_make_non_root_mpp_task_active[];
extern const char exception_before_mpp_register_root_mpp_task[];
extern const char exception_before_mpp_make_root_mpp_task_active[];
extern const char exception_before_mpp_register_tunnel_for_non_root_mpp_task[];
extern const char exception_before_mpp_register_tunnel_for_root_mpp_task[];
extern const char exception_during_mpp_register_tunnel_for_non_root_mpp_task[];
extern const char exception_before_mpp_non_root_task_run[];
extern const char exception_before_mpp_root_task_run[];
extern const char exception_during_mpp_non_root_task_run[];
extern const char exception_during_mpp_root_task_run[];
extern const char exception_during_query_run[];
} // namespace FailPoints

namespace tests
{
LoggerPtr MPPTaskTestUtils::log_ptr = nullptr;
size_t MPPTaskTestUtils::server_num = 0;
MPPTestMeta MPPTaskTestUtils::test_meta = {};

class ComputeServerRunner : public DB::tests::MPPTaskTestUtils
{
public:
    void initializeContext() override
    try
    {
        ExecutorTest::initializeContext();
        switcher = std::make_shared<AutoPassThroughSwitcher>(true, ::tipb::TiFlashPreAggMode::Auto);
        auto_pass_through_test_data.init(context);

        /// for agg
        context.addMockTable(
            {"test_db", "test_table_1"},
            {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", {1, {}, 10000000, 10000000}),
             toNullableVec<String>("s2", {"apple", {}, "banana", "test"}),
             toNullableVec<String>("s3", {"apple", {}, "banana", "test"})});

        /// agg table with 200 rows
        std::vector<std::optional<TypeTraits<int>::FieldType>> agg_s1(200);
        std::vector<std::optional<String>> agg_s2(200);
        std::vector<std::optional<String>> agg_s3(200);
        for (size_t i = 0; i < 200; ++i)
        {
            if (i % 30 != 0)
            {
                agg_s1[i] = i % 20;
                agg_s2[i] = {fmt::format("val_{}", i % 10)};
                agg_s3[i] = {fmt::format("val_{}", i)};
            }
        }
        context.addMockTable(
            {"test_db", "test_table_2"},
            {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", agg_s1),
             toNullableVec<String>("s2", agg_s2),
             toNullableVec<String>("s3", agg_s3)});

        /// for join
        context.addMockTable(
            {"test_db", "l_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", {}, "banana"}),
             toNullableVec<String>("join_c", {"apple", {}, "banana"})});
        context.addMockTable(
            {"test_db", "r_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", {}, "banana"}),
             toNullableVec<String>("join_c", {"apple", {}, "banana"})});

        /// join left table with 200 rows
        std::vector<std::optional<TypeTraits<int>::FieldType>> join_s1(200);
        std::vector<std::optional<String>> join_s2(200);
        std::vector<std::optional<String>> join_s3(200);
        for (size_t i = 0; i < 200; ++i)
        {
            if (i % 20 != 0)
            {
                agg_s1[i] = i % 5;
                agg_s2[i] = {fmt::format("val_{}", i % 6)};
                agg_s3[i] = {fmt::format("val_{}", i)};
            }
        }
        context.addMockTable(
            {"test_db", "l_table_2"},
            {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", agg_s1),
             toNullableVec<String>("s2", agg_s2),
             toNullableVec<String>("s3", agg_s3)});

        /// join right table with 100 rows
        std::vector<std::optional<TypeTraits<int>::FieldType>> join_r_s1(100);
        std::vector<std::optional<String>> join_r_s2(100);
        std::vector<std::optional<String>> join_r_s3(100);
        for (size_t i = 0; i < 100; ++i)
        {
            if (i % 20 != 0)
            {
                join_r_s1[i] = i % 6;
                join_r_s2[i] = {fmt::format("val_{}", i % 7)};
                join_r_s3[i] = {fmt::format("val_{}", i)};
            }
        }
        context.addMockTable(
            {"test_db", "r_table_2"},
            {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", join_r_s1),
             toNullableVec<String>("s2", join_r_s2),
             toNullableVec<String>("s3", join_r_s3)});

        context.addMockTable(
            {"test_db", "auto_pass_through_empty_tbl"},
            {{"col1", TiDB::TP::TypeLong}},
            {toNullableVec<Int32>("col1", {})});
    }
    CATCH

    void addOneGather(
        std::vector<std::thread> & running_queries,
        std::vector<MPPGatherId> & gather_ids,
        const DAGProperties & properties)
    {
        MPPGatherId gather_id(
            properties.gather_id,
            properties.query_ts,
            properties.local_query_id,
            properties.server_id,
            properties.start_ts,
            /*resource_group_name=*/"",
            0,
            "");
        gather_ids.push_back(gather_id);
        running_queries.emplace_back([&, properties, gather_id]() {
            BlockInputStreamPtr stream;
            try
            {
                std::function<DAGRequestBuilder()> gen_builder = [&]() {
                    return context.scan("test_db", "l_table")
                        .aggregation({Max(col("l_table.s"))}, {col("l_table.s")})
                        .project({col("max(l_table.s)"), col("l_table.s")});
                };
                QueryTasks tasks = prepareMPPTasks(gen_builder, properties);
                executeProblematicMPPTasks(tasks, properties, stream);
            }
            catch (...)
            {
                MockComputeServerManager::instance().cancelGather(gather_id);
                EXPECT_TRUE(assertGatherCancelled(gather_id));
            }
        });
    }
    void addOneQuery(size_t query_ts, std::vector<std::thread> & running_queries, std::vector<MPPGatherId> & gather_ids)
    {
        auto properties = DB::tests::getDAGPropertiesForTest(serverNum(), 1, 1, query_ts);
        addOneGather(running_queries, gather_ids, properties);
    }

    // To test auto pass through hashagg.
    struct AutoPassThroughTestData
    {
        void init(MockDAGRequestContext & context)
        {
            context.context->getSettingsRef().max_block_size = block_size;

            if (inited)
                return;

            inited = true;

            col1_data_type = std::make_shared<DataTypeString>();
            col2_data_type = std::make_shared<DataTypeInt64>();
            col3_data_type = std::make_shared<DataTypeDecimal<Decimal64>>(col3_prec, col3_scale);
            col4_data_type = std::make_shared<DataTypeUInt8>();

            auto_pass_through_context = buildAutoPassHashAggThroughContext(context);
            // 1 block for adjust state, 3 blocks for other state
            auto_pass_through_context->updateAdjustStateRowLimitUnitNum(3);
            auto_pass_through_context->updateOtherStateRowLimitUnitNum(3);

            std::tie(low_ndv_blocks, low_ndv_block) = buildBlocks(/*block_num*/ 20, /*distinct_num*/ 10);
            std::tie(high_ndv_blocks, high_ndv_block) = buildBlocks(/*block_num*/ 20, /*distinct_num*/ 20 * block_size);
            std::tie(medium_ndv_blocks, medium_ndv_block) = buildBlocksForMediumNDV(/*block_num*/ 40);
            std::tie(random_blocks, random_block) = buildBlocks(/*block_num*/ 20, /*distinct_num*/ 20 * block_size);

            MockColumnInfoVec column_infos{
                {col1_name, TiDB::TP::TypeString, false},
                {col2_name, TiDB::TP::TypeLongLong, false},
                {col4_name, TiDB::TP::TypeTiny, false}};

            context.addMockTable(
                {db_name, high_ndv_tbl_name},
                column_infos,
                high_ndv_block.getColumnsWithTypeAndName());
            context.addMockTable({db_name, low_ndv_tbl_name}, column_infos, low_ndv_block.getColumnsWithTypeAndName());
            context.addMockTable(
                {db_name, medium_ndv_tbl_name},
                column_infos,
                medium_ndv_block.getColumnsWithTypeAndName());

            MockColumnInfoVec nullable_column_infos{
                {col1_name, TiDB::TP::TypeString, true},
                {col2_name, TiDB::TP::TypeLongLong, true},
                {col4_name, TiDB::TP::TypeTiny, true}};

            context.addMockTable(
                {db_name, nullable_high_ndv_tbl_name},
                nullable_column_infos,
                high_ndv_block.getColumnsWithTypeAndName());
            context.addMockTable(
                {db_name, nullable_low_ndv_tbl_name},
                nullable_column_infos,
                low_ndv_block.getColumnsWithTypeAndName());
            context.addMockTable(
                {db_name, nullable_medium_ndv_tbl_name},
                nullable_column_infos,
                medium_ndv_block.getColumnsWithTypeAndName());
        }

        static void appendAggDescription(
            const Names & arg_names,
            const DataTypes & arg_types,
            TiDB::TiDBCollators & arg_collators,
            const String & agg_func_name,
            AggregateDescriptions & aggregate_descriptions,
            NamesAndTypes & aggregated_columns,
            bool empty_input_as_null,
            const Context & context)
        {
            assert(arg_names.size() == arg_collators.size() && arg_names.size() == arg_types.size());

            AggregateDescription aggregate;
            aggregate.argument_names = arg_names;
            String func_string = genFuncString(agg_func_name, aggregate.argument_names, arg_collators);

            aggregate.column_name = func_string;
            aggregate.parameters = Array();
            aggregate.function = AggregateFunctionFactory::instance()
                                     .get(context, agg_func_name, arg_types, {}, 0, empty_input_as_null);
            aggregate.function->setCollators(arg_collators);

            aggregated_columns.emplace_back(func_string, aggregate.function->getReturnType());

            aggregate_descriptions.emplace_back(std::move(aggregate));
        }

        // select repo_name, sum(commit_num) from repo_history group by repo_name;
        std::unique_ptr<AutoPassThroughHashAggContext> buildAutoPassHashAggThroughContext(
            MockDAGRequestContext & context)
        {
            const String req_id("TestAutoPassThroughAggContext");

            Block before_agg_header(NamesAndTypes{{col1_name, col1_data_type}, {col2_name, col2_data_type}});

            size_t agg_streams_size = 1;
            size_t before_agg_stream_size = 1;
            Names key_names{col1_name};

            KeyRefAggFuncMap key_ref_agg_func;
            AggFuncRefKeyMap agg_func_ref_key;

            // Assume no collator
            std::unordered_map<String, TiDB::TiDBCollatorPtr> collators{{col1_name, nullptr}};

            TiDB::TiDBCollators arg_collators{nullptr};

            AggregateDescriptions aggregate_descriptions;
            NamesAndTypes aggregated_columns;
            appendAggDescription(
                Names{col1_name},
                DataTypes{col1_data_type},
                arg_collators,
                "first_row",
                aggregate_descriptions,
                aggregated_columns,
                /*empty_input_as_null*/ false,
                *context.context);
            appendAggDescription(
                Names{col2_name},
                DataTypes{col2_data_type},
                arg_collators,
                "sum",
                aggregate_descriptions,
                aggregated_columns,
                /*empty_input_as_null*/ false,
                *context.context);

            AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);

            SpillConfig spill_config(
                context.context->getTemporaryPath(),
                req_id,
                context.context->getSettingsRef().max_cached_data_bytes_in_spiller,
                context.context->getSettingsRef().max_spilled_rows_per_file,
                context.context->getSettingsRef().max_spilled_bytes_per_file,
                context.context->getFileProvider(),
                context.context->getSettingsRef().max_threads,
                context.context->getSettingsRef().max_block_size);

            auto params = AggregationInterpreterHelper::buildParams(
                *context.context,
                before_agg_header,
                before_agg_stream_size,
                agg_streams_size,
                key_names,
                key_ref_agg_func,
                agg_func_ref_key,
                collators,
                aggregate_descriptions,
                /*final=*/true,
                spill_config);

            ColumnsWithTypeAndName cols{{nullptr, col1_data_type, col1_name}, {nullptr, col2_data_type, col2_name}};
            Block child_header(cols);

            return std::make_unique<AutoPassThroughHashAggContext>(
                child_header,
                *params,
                [&]() { return false; },
                req_id,
                context.context->getSettings().max_block_size);
        }

        // Each block has 50% NDV.
        std::pair<BlocksList, Block> buildBlocksForMediumNDV(size_t block_num)
        {
            auto distinct_repo_names = generateDistinctStrings(block_num * block_size / 2);
            auto distinct_commit_nums = generateDistinctIntegers(block_num * block_size / 2);

            auto col1_in_one = col1_data_type->createColumn();
            auto col2_in_one = col2_data_type->createColumn();
            auto col4_in_one = col4_data_type->createColumn();

            BlocksList res;
            for (size_t i = 0; i < block_num; ++i)
            {
                auto col1 = col1_data_type->createColumn();
                auto col2 = col2_data_type->createColumn();
                auto col4 = col4_data_type->createColumn();

                std::vector<String> cur_distinct_repo_names;
                cur_distinct_repo_names.reserve(block_size / 2);
                std::vector<Int64> cur_distinct_commit_nums;
                cur_distinct_commit_nums.reserve(block_size / 2);
                for (size_t j = 0; j < block_size / 2; ++j)
                {
                    auto idx = i * block_size / 2 + j;
                    cur_distinct_repo_names.push_back(distinct_repo_names[idx]);
                    cur_distinct_commit_nums.push_back(distinct_commit_nums[idx]);
                }

                for (size_t j = 0; j < block_size; ++j)
                {
                    size_t idx = j % cur_distinct_repo_names.size();
                    auto repo_name = cur_distinct_repo_names[idx];
                    auto commit_num = cur_distinct_commit_nums[idx];
                    col1->insert(Field(repo_name.data(), repo_name.size()));
                    col2->insert(Field(static_cast<Int64>(commit_num)));
                    // DecimalField<Decimal64> decimal_field(Decimal64(static_cast<Int64>(commit_num)), col3_scale);
                    // col3->insert(Field(decimal_field));
                    col4->insert(Field(static_cast<UInt64>(commit_num)));

                    col1_in_one->insert(Field(repo_name.data(), repo_name.size()));
                    col2_in_one->insert(Field(static_cast<Int64>(commit_num)));
                    // col3_in_one->insert(Field(decimal_field));
                    col4_in_one->insert(Field(static_cast<UInt64>(commit_num)));
                }
                ColumnsWithTypeAndName cols{
                    {std::move(col1), col1_data_type, col1_name},
                    {std::move(col2), col2_data_type, col2_name},
                    {std::move(col4), col4_data_type, col4_name}};
                res.push_back(Block(cols));
            }
            ColumnsWithTypeAndName cols{
                {std::move(col1_in_one), col1_data_type, col1_name},
                {std::move(col2_in_one), col2_data_type, col2_name},
                {std::move(col4_in_one), col4_data_type, col4_name}};
            Block res_in_one(cols);
            return {res, res_in_one};
        }

        std::pair<BlocksList, Block> buildBlocks(size_t block_num, size_t distinct_num)
        {
            auto distinct_repo_names = generateDistinctStrings(distinct_num);
            auto distinct_commit_nums = generateDistinctIntegers(distinct_num);

            auto col1_in_one = col1_data_type->createColumn();
            auto col2_in_one = col2_data_type->createColumn();
            auto col4_in_one = col4_data_type->createColumn();

            BlocksList res;
            for (size_t i = 0; i < block_num; ++i)
            {
                auto col1 = col1_data_type->createColumn();
                auto col2 = col2_data_type->createColumn();
                auto col4 = col4_data_type->createColumn();

                for (size_t j = 0; j < block_size; ++j)
                {
                    size_t idx = (i * block_size + j) % distinct_num;
                    auto repo_name = distinct_repo_names[idx];
                    auto commit_num = distinct_commit_nums[idx];
                    col1->insert(Field(repo_name.data(), repo_name.size()));
                    col2->insert(Field(static_cast<Int64>(commit_num)));
                    // DecimalField<Decimal64> decimal_field(Decimal64(static_cast<Int64>(commit_num)), col3_scale);
                    // col3->insert(Field(decimal_field));
                    col4->insert(Field(static_cast<UInt64>(commit_num)));

                    col1_in_one->insert(Field(repo_name.data(), repo_name.size()));
                    col2_in_one->insert(Field(static_cast<Int64>(commit_num)));
                    // col3_in_one->insert(Field(decimal_field));
                    col4_in_one->insert(Field(static_cast<UInt64>(commit_num)));
                }
                ColumnsWithTypeAndName cols{
                    {std::move(col1), col1_data_type, col1_name},
                    {std::move(col2), col2_data_type, col2_name},
                    {std::move(col4), col4_data_type, col4_name}};
                res.push_back(Block(cols));
            }
            ColumnsWithTypeAndName cols{
                {std::move(col1_in_one), col1_data_type, col1_name},
                {std::move(col2_in_one), col2_data_type, col2_name},
                {std::move(col4_in_one), col4_data_type, col4_name}};
            Block res_in_one(cols);
            return {res, res_in_one};
        }

        static std::string generateRandomString(
            size_t length,
            std::mt19937 & gen,
            std::uniform_int_distribution<> & charDist)
        {
            const std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            std::string result;
            for (size_t i = 0; i < length; ++i)
            {
                result += chars[charDist(gen)];
            }
            return result;
        }

        static std::vector<std::string> generateDistinctStrings(size_t n)
        {
            std::unordered_set<std::string> unique_strings;
            std::vector<std::string> result;

            std::random_device rd;
            std::mt19937 gen(rd());

            std::normal_distribution<> length_dist(50, 15);
            std::uniform_int_distribution<> char_dist(0, 61);

            while (unique_strings.size() < n)
            {
                size_t length;
                do
                {
                    length = static_cast<size_t>(std::round(length_dist(gen)));
                } while (length < 1 || length > 100);

                std::string new_string = generateRandomString(length, gen, char_dist);

                if (unique_strings.find(new_string) == unique_strings.end())
                {
                    unique_strings.insert(new_string);
                    result.push_back(new_string);
                }
            }

            return result;
        }

        static std::vector<Int32> generateDistinctIntegers(size_t n)
        {
            std::unordered_set<int> unique_ints;
            std::vector<int> result;

            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dist(0, std::numeric_limits<Int32>::max());

            while (unique_ints.size() < n)
            {
                int new_int = dist(gen);
                if (unique_ints.find(new_int) == unique_ints.end())
                {
                    unique_ints.insert(new_int);
                    result.push_back(new_int);
                }
            }

            return result;
        }
        const size_t block_size = 8192;
        std::unique_ptr<AutoPassThroughHashAggContext> auto_pass_through_context;
        const String col1_name = "repo_name";
        const String col2_name = "commit_num";
        // todo: col3 is used to test decimal type, but execute test framework doesn't support decimal for now.
        // So it's not used for now.
        const String col3_name = "quantity_num";
        const String col4_name = "status";
        const String db_name = "test_db";
        const String high_ndv_tbl_name = "test_tbl_high_ndv";
        const String low_ndv_tbl_name = "test_tbl_low_ndv";
        const String medium_ndv_tbl_name = "test_tbl_medium_ndv";
        const String nullable_high_ndv_tbl_name = "test_nullable_tbl_high_ndv";
        const String nullable_low_ndv_tbl_name = "test_nullable_tbl_low_ndv";
        const String nullable_medium_ndv_tbl_name = "test_nullable_tbl_medium_ndv";

        DataTypePtr col1_data_type;
        DataTypePtr col2_data_type;
        DataTypePtr col3_data_type;
        DataTypePtr col4_data_type;

        const size_t col3_prec = 15;
        const size_t col3_scale = 2;

        BlocksList high_ndv_blocks;
        BlocksList low_ndv_blocks;
        BlocksList medium_ndv_blocks;
        BlocksList random_blocks;

        Block high_ndv_block;
        Block low_ndv_block;
        Block medium_ndv_block;
        Block random_block;

        bool inited = false;
    };

    AutoPassThroughTestData auto_pass_through_test_data;
    std::shared_ptr<AutoPassThroughSwitcher> switcher;
};


#define WRAP_FOR_SERVER_TEST_BEGIN                 \
    std::vector<bool> pipeline_bools{false, true}; \
    for (auto enable_pipeline : pipeline_bools)    \
    {                                              \
        enablePipeline(enable_pipeline);

#define WRAP_FOR_SERVER_TEST_END }

#ifdef SANITIZER
#define ADAPTIVE_SLEEP(X, Y) std::this_thread::sleep_for(Y)
#else
#define ADAPTIVE_SLEEP(X, Y) std::this_thread::sleep_for(X)
#endif // SANITIZER

TEST_F(ComputeServerRunner, simpleExchange)
try
{
    std::vector<std::optional<TypeTraits<Int32>::FieldType>> s1_col(10000);
    for (size_t i = 0; i < s1_col.size(); ++i)
        s1_col[i] = i;
    auto expected_cols = {toNullableVec<Int32>("s1", s1_col)};
    context.addMockTable({"test_db", "big_table"}, {{"s1", TiDB::TP::TypeLong}}, expected_cols);

    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(100)));

    WRAP_FOR_SERVER_TEST_BEGIN
    // For PassThrough and Broadcast, use only one server for testing, as multiple servers will double the result size.
    {
        startServers(1);
        {
            std::vector<String> expected_strings = {
                R"(
exchange_sender_2 | type:PassThrough, {<0, Long>}
 project_1 | {<0, Long>}
  table_scan_0 | {<0, Long>})"};
            ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(
                context.scan("test_db", "big_table").project({"s1"}),
                expected_strings,
                expected_cols);
        }
        {
            std::vector<String> expected_strings = {
                R"(
exchange_sender_1 | type:PassThrough, {<0, Long>}
 table_scan_0 | {<0, Long>})",
                R"(
exchange_sender_4 | type:PassThrough, {<0, Long>}
 project_3 | {<0, Long>}
  exchange_receiver_2 | type:PassThrough, {<0, Long>})"};
            ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(
                context.scan("test_db", "big_table")
                    .exchangeSender(tipb::ExchangeType::PassThrough)
                    .exchangeReceiver("recv", {{"s1", TiDB::TP::TypeLong}})
                    .project({"s1"}),
                expected_strings,
                expected_cols);
        }
        {
            std::vector<String> expected_strings = {
                R"(
exchange_sender_1 | type:Broadcast, {<0, Long>}
 table_scan_0 | {<0, Long>})",
                R"(
exchange_sender_4 | type:PassThrough, {<0, Long>}
 project_3 | {<0, Long>}
  exchange_receiver_2 | type:Broadcast, {<0, Long>})"};
            ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(
                context.scan("test_db", "big_table")
                    .exchangeSender(tipb::ExchangeType::Broadcast)
                    .exchangeReceiver("recv", {{"s1", TiDB::TP::TypeLong}})
                    .project({"s1"}),
                expected_strings,
                expected_cols);
        }
    }
    // For Hash, multiple servers will not double the result.
    {
        startServers(2);
        std::vector<String> expected_strings = {
            R"(
exchange_sender_1 | type:Hash, {<0, Long>}
 table_scan_0 | {<0, Long>})",
            R"(
exchange_sender_1 | type:Hash, {<0, Long>}
 table_scan_0 | {<0, Long>})",
            R"(
exchange_sender_4 | type:PassThrough, {<0, Long>}
 project_3 | {<0, Long>}
  exchange_receiver_2 | type:Hash, {<0, Long>})",
            R"(
exchange_sender_4 | type:PassThrough, {<0, Long>}
 project_3 | {<0, Long>}
  exchange_receiver_2 | type:Hash, {<0, Long>})"};
        std::vector<uint64_t> fine_grained_shuffle_stream_count{8, 0};
        for (uint64_t stream_count : fine_grained_shuffle_stream_count)
        {
            ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(
                context.scan("test_db", "big_table")
                    .exchangeSender(tipb::ExchangeType::Hash, {"test_db.big_table.s1"}, stream_count)
                    .exchangeReceiver("recv", {{"s1", TiDB::TP::TypeLong}}, stream_count)
                    .project({"s1"}),
                expected_strings,
                expected_cols);
        }
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

TEST_F(ComputeServerRunner, runAggTasks)
try
{
    WRAP_FOR_SERVER_TEST_BEGIN
    startServers(4);
    {
        std::vector<String> expected_strings = {
            R"(exchange_sender_5 | type:Hash, {<0, Long>, <1, String>, <2, String>}
 aggregation_4 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  table_scan_0 | {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_5 | type:Hash, {<0, Long>, <1, String>, <2, String>}
 aggregation_4 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  table_scan_0 | {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_5 | type:Hash, {<0, Long>, <1, String>, <2, String>}
 aggregation_4 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  table_scan_0 | {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_5 | type:Hash, {<0, Long>, <1, String>, <2, String>}
 aggregation_4 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  table_scan_0 | {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_3 | type:PassThrough, {<0, Long>}
 project_2 | {<0, Long>}
  aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
   exchange_receiver_6 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_3 | type:PassThrough, {<0, Long>}
 project_2 | {<0, Long>}
  aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
   exchange_receiver_6 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
)",
            R"(
exchange_sender_3 | type:PassThrough, {<0, Long>}
 project_2 | {<0, Long>}
  aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
   exchange_receiver_6 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_3 | type:PassThrough, {<0, Long>}
 project_2 | {<0, Long>}
  aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
   exchange_receiver_6 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
)"};
        auto expected_cols = {toNullableVec<Int32>({1, {}, 10000000, 10000000})};

        ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(
            context.scan("test_db", "test_table_1")
                .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                .project({"max(s1)"}),
            expected_strings,
            expected_cols);
    }

    {
        auto properties = getDAGPropertiesForTest(1);
        auto tasks = context.scan("test_db", "test_table_1")
                         .aggregation({Count(col("s1"))}, {})
                         .project({"count(s1)"})
                         .buildMPPTasks(context, properties);
        std::vector<String> expected_strings = {
            R"(exchange_sender_5 | type:PassThrough, {<0, Longlong>}
 aggregation_4 | group_by: {}, agg_func: {count(<0, Long>)}
  table_scan_0 | {<0, Long>}
            )",
            R"(exchange_sender_3 | type:PassThrough, {<0, Longlong>}
 project_2 | {<0, Longlong>}
  aggregation_1 | group_by: {}, agg_func: {sum(<0, Longlong>)}
   exchange_receiver_6 | type:PassThrough, {<0, Longlong>})"};

        size_t task_size = tasks.size();
        for (size_t i = 0; i < task_size; ++i)
        {
            ASSERT_DAGREQUEST_EQAUL(expected_strings[i], tasks[i].dag_request);
        }
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

TEST_F(ComputeServerRunner, runJoinTasks)
try
{
    WRAP_FOR_SERVER_TEST_BEGIN
    startServers(3);
    {
        auto expected_cols
            = {toNullableVec<String>({{}, "banana", "banana"}),
               toNullableVec<String>({{}, "apple", "banana"}),
               toNullableVec<String>({{}, "banana", "banana"}),
               toNullableVec<String>({{}, "apple", "banana"})};

        std::vector<String> expected_strings = {
            R"(exchange_sender_5 | type:Hash, {<0, String>, <1, String>}
 table_scan_1 | {<0, String>, <1, String>})",
            R"(exchange_sender_5 | type:Hash, {<0, String>, <1, String>}
 table_scan_1 | {<0, String>, <1, String>})",
            R"(exchange_sender_5 | type:Hash, {<0, String>, <1, String>}
 table_scan_1 | {<0, String>, <1, String>})",
            R"(exchange_sender_4 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_4 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_4 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_3 | type:PassThrough, {<0, String>, <1, String>, <2, String>, <3, String>}
 Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
  exchange_receiver_6 | type:PassThrough, {<0, String>, <1, String>}
  exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>})",
            R"(exchange_sender_3 | type:PassThrough, {<0, String>, <1, String>, <2, String>, <3, String>}
 Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
  exchange_receiver_6 | type:PassThrough, {<0, String>, <1, String>}
  exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>})",
            R"(exchange_sender_3 | type:PassThrough, {<0, String>, <1, String>, <2, String>, <3, String>}
 Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
  exchange_receiver_6 | type:PassThrough, {<0, String>, <1, String>}
  exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>})"};

        ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(
            context.scan("test_db", "l_table")
                .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}),
            expected_strings,
            expected_cols);
    }

    {
        auto properties = getDAGPropertiesForTest(1);
        auto tasks = context.scan("test_db", "l_table")
                         .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                         .buildMPPTasks(context, properties);

        std::vector<String> expected_strings = {
            R"(exchange_sender_5 | type:Hash, {<0, String>, <1, String>}
 table_scan_1 | {<0, String>, <1, String>})",
            R"(exchange_sender_4 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_3 | type:PassThrough, {<0, String>, <1, String>, <2, String>, <3, String>}
 Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
  exchange_receiver_6 | type:PassThrough, {<0, String>, <1, String>}
  exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>})"};

        size_t task_size = tasks.size();
        for (size_t i = 0; i < task_size; ++i)
        {
            ASSERT_DAGREQUEST_EQAUL(expected_strings[i], tasks[i].dag_request);
        }
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

TEST_F(ComputeServerRunner, runJoinThenAggTasks)
try
{
    WRAP_FOR_SERVER_TEST_BEGIN
    startServers(3);
    {
        std::vector<String> expected_strings = {
            R"(exchange_sender_10 | type:Hash, {<0, String>}
 table_scan_1 | {<0, String>})",
            R"(exchange_sender_10 | type:Hash, {<0, String>}
 table_scan_1 | {<0, String>})",
            R"(exchange_sender_10 | type:Hash, {<0, String>}
 table_scan_1 | {<0, String>})",
            R"(exchange_sender_9 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_9 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_9 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_7 | type:Hash, {<0, String>, <1, String>}
 aggregation_6 | group_by: {<0, String>}, agg_func: {max(<0, String>)}
  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
   exchange_receiver_11 | type:PassThrough, {<0, String>, <1, String>}
   exchange_receiver_12 | type:PassThrough, {<0, String>})",
            R"(exchange_sender_7 | type:Hash, {<0, String>, <1, String>}
 aggregation_6 | group_by: {<0, String>}, agg_func: {max(<0, String>)}
  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
   exchange_receiver_11 | type:PassThrough, {<0, String>, <1, String>}
   exchange_receiver_12 | type:PassThrough, {<0, String>})",
            R"(exchange_sender_7 | type:Hash, {<0, String>, <1, String>}
 aggregation_6 | group_by: {<0, String>}, agg_func: {max(<0, String>)}
  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
   exchange_receiver_11 | type:PassThrough, {<0, String>, <1, String>}
   exchange_receiver_12 | type:PassThrough, {<0, String>})",
            R"(exchange_sender_5 | type:PassThrough, {<0, String>, <1, String>}
 project_4 | {<0, String>, <1, String>}
  aggregation_3 | group_by: {<1, String>}, agg_func: {max(<0, String>)}
   exchange_receiver_8 | type:PassThrough, {<0, String>, <1, String>})",
            R"(exchange_sender_5 | type:PassThrough, {<0, String>, <1, String>}
 project_4 | {<0, String>, <1, String>}
  aggregation_3 | group_by: {<1, String>}, agg_func: {max(<0, String>)}
   exchange_receiver_8 | type:PassThrough, {<0, String>, <1, String>})",
            R"(exchange_sender_5 | type:PassThrough, {<0, String>, <1, String>}
 project_4 | {<0, String>, <1, String>}
  aggregation_3 | group_by: {<1, String>}, agg_func: {max(<0, String>)}
   exchange_receiver_8 | type:PassThrough, {<0, String>, <1, String>})"};

        auto expected_cols = {toNullableVec<String>({{}, "banana"}), toNullableVec<String>({{}, "banana"})};

        ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(
            context.scan("test_db", "l_table")
                .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                .aggregation({Max(col("l_table.s"))}, {col("l_table.s")})
                .project({col("max(l_table.s)"), col("l_table.s")}),
            expected_strings,
            expected_cols);
    }

    {
        auto properties = getDAGPropertiesForTest(1);
        auto tasks = context.scan("test_db", "l_table")
                         .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                         .aggregation({Max(col("l_table.s"))}, {col("l_table.s")})
                         .project({col("max(l_table.s)"), col("l_table.s")})
                         .buildMPPTasks(context, properties);

        std::vector<String> expected_strings = {
            R"(exchange_sender_10 | type:Hash, {<0, String>}
 table_scan_1 | {<0, String>})",
            R"(exchange_sender_9 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_7 | type:Hash, {<0, String>, <1, String>}
 aggregation_6 | group_by: {<0, String>}, agg_func: {max(<0, String>)}
  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
   exchange_receiver_11 | type:PassThrough, {<0, String>, <1, String>}
   exchange_receiver_12 | type:PassThrough, {<0, String>})",
            R"(exchange_sender_5 | type:PassThrough, {<0, String>, <1, String>}
 project_4 | {<0, String>, <1, String>}
  aggregation_3 | group_by: {<1, String>}, agg_func: {max(<0, String>)}
   exchange_receiver_8 | type:PassThrough, {<0, String>, <1, String>})",
        };

        size_t task_size = tasks.size();
        for (size_t i = 0; i < task_size; ++i)
        {
            ASSERT_DAGREQUEST_EQAUL(expected_strings[i], tasks[i].dag_request);
        }
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

TEST_F(ComputeServerRunner, aggWithColumnPrune)
try
{
    WRAP_FOR_SERVER_TEST_BEGIN
    startServers(3);

    context.addMockTable(
        {"test_db", "test_table_2"},
        {{"i1", TiDB::TP::TypeLong},
         {"i2", TiDB::TP::TypeLong},
         {"s1", TiDB::TP::TypeString},
         {"s2", TiDB::TP::TypeString},
         {"s3", TiDB::TP::TypeString},
         {"s4", TiDB::TP::TypeString},
         {"s5", TiDB::TP::TypeString}},
        {toNullableVec<Int32>("i1", {0, 0, 0}),
         toNullableVec<Int32>("i2", {1, 1, 1}),
         toNullableVec<String>("s1", {"1", "9", "8"}),
         toNullableVec<String>("s2", {"1", "9", "8"}),
         toNullableVec<String>("s3", {"4", "9", "99"}),
         toNullableVec<String>("s4", {"4", "9", "999"}),
         toNullableVec<String>("s5", {"4", "9", "9999"})});
    std::vector<String> res{"9", "9", "99", "999", "9999"};
    std::vector<String> max_cols{"s1", "s2", "s3", "s4", "s5"};
    for (size_t i = 0; i < 1; ++i)
    {
        {
            auto request = context.scan("test_db", "test_table_2").aggregation({Max(col(max_cols[i]))}, {col("i1")});
            auto expected_cols = {toNullableVec<String>({res[i]}), toNullableVec<Int32>({{0}})};
            ASSERT_COLUMNS_EQ_UR(expected_cols, buildAndExecuteMPPTasks(request));
        }

        {
            auto request = context.scan("test_db", "test_table_2").aggregation({Max(col(max_cols[i]))}, {col("i2")});
            auto expected_cols = {toNullableVec<String>({res[i]}), toNullableVec<Int32>({{1}})};
            ASSERT_COLUMNS_EQ_UR(expected_cols, buildAndExecuteMPPTasks(request));
        }

        {
            auto request
                = context.scan("test_db", "test_table_2").aggregation({Max(col(max_cols[i]))}, {col("i1"), col("i2")});
            auto expected_cols
                = {toNullableVec<String>({res[i]}), toNullableVec<Int32>({{0}}), toNullableVec<Int32>({{1}})};
            ASSERT_COLUMNS_EQ_UR(expected_cols, buildAndExecuteMPPTasks(request));
        }
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

TEST_F(ComputeServerRunner, cancelAggTasks)
try
{
    WRAP_FOR_SERVER_TEST_BEGIN
    startServers(4);
    setCancelTest();
    {
        /// case 1, cancel after dispatch MPPTasks
        auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
        MPPGatherId gather_id(
            properties.gather_id,
            properties.query_ts,
            properties.local_query_id,
            properties.server_id,
            properties.start_ts,
            /*resource_group_name=*/"",
            0,
            "");
        auto res = prepareMPPStreams(
            context.scan("test_db", "test_table_1")
                .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                .project({"max(s1)"}),
            properties);
        EXPECT_TRUE(assertQueryActive(gather_id.query_id));
        MockComputeServerManager::instance().cancelGather(gather_id);
        EXPECT_TRUE(assertQueryCancelled(gather_id.query_id));
    }
    {
        /// case 2, cancel before dispatch MPPTasks
        auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
        MPPGatherId gather_id(
            properties.gather_id,
            properties.query_ts,
            properties.local_query_id,
            properties.server_id,
            properties.start_ts,
            /*resource_group_name=*/"",
            0,
            "");
        auto tasks = prepareMPPTasks(
            context.scan("test_db", "test_table_1")
                .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                .project({"max(s1)"}),
            properties);
        EXPECT_TRUE(!assertQueryActive(gather_id.query_id));
        MockComputeServerManager::instance().cancelGather(gather_id);
        try
        {
            executeMPPTasks(tasks, properties);
        }
        catch (...)
        {}
        EXPECT_TRUE(assertQueryCancelled(gather_id.query_id));
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

TEST_F(ComputeServerRunner, cancelJoinTasks)
try
{
    WRAP_FOR_SERVER_TEST_BEGIN
    startServers(4);
    {
        setCancelTest();
        auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
        MPPGatherId gather_id(
            properties.gather_id,
            properties.query_ts,
            properties.local_query_id,
            properties.server_id,
            properties.start_ts,
            /*resource_group_name=*/"",
            0,
            "");
        auto res = prepareMPPStreams(
            context.scan("test_db", "l_table")
                .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}),
            properties);
        EXPECT_TRUE(assertQueryActive(gather_id.query_id));
        MockComputeServerManager::instance().cancelGather(gather_id);
        EXPECT_TRUE(assertQueryCancelled(gather_id.query_id));
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

TEST_F(ComputeServerRunner, cancelJoinThenAggTasks)
try
{
    WRAP_FOR_SERVER_TEST_BEGIN
    startServers(4);
    {
        setCancelTest();
        auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
        MPPGatherId gather_id(
            properties.gather_id,
            properties.query_ts,
            properties.local_query_id,
            properties.server_id,
            properties.start_ts,
            /*resource_group_name=*/"",
            0,
            "");
        auto stream = prepareMPPStreams(
            context.scan("test_db", "l_table")
                .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                .aggregation({Max(col("l_table.s"))}, {col("l_table.s")})
                .project({col("max(l_table.s)"), col("l_table.s")}),
            properties);
        EXPECT_TRUE(assertQueryActive(gather_id.query_id));
        MockComputeServerManager::instance().cancelGather(gather_id);
        EXPECT_TRUE(assertQueryCancelled(gather_id.query_id));
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

TEST_F(ComputeServerRunner, multipleQuery)
try
{
    WRAP_FOR_SERVER_TEST_BEGIN
    startServers(4);
    setCancelTest();
    {
        auto properties1 = DB::tests::getDAGPropertiesForTest(serverNum());
        MPPGatherId gather_id1(
            properties1.gather_id,
            properties1.query_ts,
            properties1.local_query_id,
            properties1.server_id,
            properties1.start_ts,
            /*resource_group_name=*/"",
            0,
            "");
        auto res1 = prepareMPPStreams(
            context.scan("test_db", "l_table")
                .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}),
            properties1);
        auto properties2 = DB::tests::getDAGPropertiesForTest(serverNum());
        MPPGatherId gather_id2(
            properties2.gather_id,
            properties2.query_ts,
            properties2.local_query_id,
            properties2.server_id,
            properties2.start_ts,
            /*resource_group_name=*/"",
            0,
            "");
        auto res2 = prepareMPPStreams(
            context.scan("test_db", "l_table")
                .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                .aggregation({Max(col("l_table.s"))}, {col("l_table.s")})
                .project({col("max(l_table.s)"), col("l_table.s")}),
            properties2);

        EXPECT_TRUE(assertQueryActive(gather_id1.query_id));
        MockComputeServerManager::instance().cancelGather(gather_id1);
        EXPECT_TRUE(assertQueryCancelled(gather_id1.query_id));

        EXPECT_TRUE(assertQueryActive(gather_id2.query_id));
        MockComputeServerManager::instance().cancelGather(gather_id2);
        EXPECT_TRUE(assertQueryCancelled(gather_id2.query_id));
    }

    // start 10 queries
    {
        size_t query_num = 10;
        std::vector<std::tuple<MPPGatherId, BlockInputStreamPtr>> queries;
        for (size_t i = 0; i < query_num; ++i)
        {
            auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
            MPPGatherId gather_id(
                properties.gather_id,
                properties.query_ts,
                properties.local_query_id,
                properties.server_id,
                properties.start_ts,
                /*resource_group_name=*/"",
                0,
                "");
            queries.push_back(std::make_tuple(
                gather_id,
                prepareMPPStreams(
                    context.scan("test_db", "l_table")
                        .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}),
                    properties)));
        }

        {
            auto thread_mgr = newThreadManager();
            for (size_t i = 0; i < query_num; ++i)
            {
                auto gather_id = std::get<0>(queries[i]);
                thread_mgr->schedule(false, "test_cancel", [=]() {
                    EXPECT_TRUE(assertQueryActive(gather_id.query_id));
                    MockComputeServerManager::instance().cancelGather(gather_id);
                    EXPECT_TRUE(assertQueryCancelled(gather_id.query_id));
                });
            }
            thread_mgr->wait();
        }
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

TEST_F(ComputeServerRunner, runCoprocessor)
try
{
    // In coprocessor test, we only need to start 1 server.
    WRAP_FOR_SERVER_TEST_BEGIN
    startServers(1);
    {
        auto request = context.scan("test_db", "l_table").build(context);

        auto expected_cols
            = {toNullableVec<String>({{"banana", {}, "banana"}}), toNullableVec<String>({{"apple", {}, "banana"}})};
        ASSERT_COLUMNS_EQ_UR(expected_cols, executeCoprocessorTask(request));
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

TEST_F(ComputeServerRunner, runFineGrainedShuffleJoinTest)
try
{
    WRAP_FOR_SERVER_TEST_BEGIN
    startServers(3);
    constexpr size_t join_type_num = 7;
    constexpr tipb::JoinType join_types[join_type_num] = {
        tipb::JoinType::TypeInnerJoin,
        tipb::JoinType::TypeLeftOuterJoin,
        tipb::JoinType::TypeRightOuterJoin,
        tipb::JoinType::TypeSemiJoin,
        tipb::JoinType::TypeAntiSemiJoin,
        tipb::JoinType::TypeLeftOuterSemiJoin,
        tipb::JoinType::TypeAntiLeftOuterSemiJoin,
    };
    // fine-grained shuffle is enabled.
    constexpr uint64_t enable = 8;
    constexpr uint64_t disable = 0;

    for (auto join_type : join_types)
    {
        auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
        auto request = context.scan("test_db", "l_table_2")
                           .join(context.scan("test_db", "r_table_2"), join_type, {col("s1"), col("s2")}, disable)
                           .project({col("l_table_2.s1"), col("l_table_2.s2"), col("l_table_2.s3")});
        const auto expected_cols = buildAndExecuteMPPTasks(request);

        auto request2 = context.scan("test_db", "l_table_2")
                            .join(context.scan("test_db", "r_table_2"), join_type, {col("s1"), col("s2")}, enable)
                            .project({col("l_table_2.s1"), col("l_table_2.s2"), col("l_table_2.s3")});
        auto tasks = request2.buildMPPTasks(context, properties);
        const auto actual_cols = executeMPPTasks(tasks, properties);
        ASSERT_COLUMNS_EQ_UR(expected_cols, actual_cols);
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

TEST_F(ComputeServerRunner, runFineGrainedShuffleAggTest)
try
{
    WRAP_FOR_SERVER_TEST_BEGIN
    startServers(3);
    // fine-grained shuffle is enabled.
    constexpr uint64_t enable = 8;
    constexpr uint64_t disable = 0;
    {
        auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
        auto request
            = context.scan("test_db", "test_table_2").aggregation({Max(col("s3"))}, {col("s1"), col("s2")}, disable);
        const auto expected_cols = buildAndExecuteMPPTasks(request);

        auto request2
            = context.scan("test_db", "test_table_2").aggregation({Max(col("s3"))}, {col("s1"), col("s2")}, enable);
        auto tasks = request2.buildMPPTasks(context, properties);
        const auto actual_cols = executeMPPTasks(tasks, properties);
        ASSERT_COLUMNS_EQ_UR(expected_cols, actual_cols);
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

TEST_F(ComputeServerRunner, randomFailpointForPipeline)
try
{
    enablePipeline(true);
    startServers(3);
    std::vector<String> failpoints{
        "random_pipeline_model_task_run_failpoint-0.8",
        "random_pipeline_model_task_construct_failpoint-1.0",
        "random_pipeline_model_event_schedule_failpoint-1.0",
        // Because the mock table scan will always output data, there will be no event triggering decActiveRefCount, so the query will not terminate.
        // "random_pipeline_model_event_finish_failpoint-0.99",
        "random_pipeline_model_operator_run_failpoint-0.8",
        "random_pipeline_model_cancel_failpoint-0.8",
        "random_pipeline_model_execute_prefix_failpoint-1.0",
        "random_pipeline_model_execute_suffix_failpoint-1.0"};
    for (const auto & failpoint : failpoints)
    {
        auto config_str = fmt::format("[flash]\nrandom_fail_points = \"{}\"", failpoint);
        initRandomFailPoint(config_str);
        auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
        MPPQueryId query_id(
            properties.query_ts,
            properties.local_query_id,
            properties.server_id,
            properties.start_ts,
            /*resource_group_name=*/"",
            0,
            "");
        try
        {
            BlockInputStreamPtr tmp = prepareMPPStreams(
                context.scan("test_db", "l_table")
                    .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                    .aggregation({Max(col("l_table.s"))}, {col("l_table.s")})
                    .project({col("max(l_table.s)"), col("l_table.s")}),
                properties);
        }
        catch (...)
        {
            // Only consider whether a crash occurs
            ::DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        // Check if the query is stuck
        EXPECT_TRUE(assertQueryCancelled(query_id)) << "fail in " << failpoint;
        disableRandomFailPoint(config_str);
    }
}
CATCH

TEST_F(ComputeServerRunner, testErrorMessage)
try
{
    WRAP_FOR_SERVER_TEST_BEGIN
    startServers(3);
    setCancelTest();
    std::vector<String> failpoint_names{
        FailPoints::exception_before_mpp_register_non_root_mpp_task,
        FailPoints::exception_before_mpp_make_non_root_mpp_task_active,
        FailPoints::exception_before_mpp_make_root_mpp_task_active,
        FailPoints::exception_before_mpp_register_tunnel_for_non_root_mpp_task,
        FailPoints::exception_before_mpp_register_tunnel_for_root_mpp_task,
        FailPoints::exception_during_mpp_register_tunnel_for_non_root_mpp_task,
        FailPoints::exception_before_mpp_non_root_task_run,
        FailPoints::exception_before_mpp_root_task_run,
        FailPoints::exception_during_mpp_non_root_task_run,
        FailPoints::exception_during_mpp_root_task_run,
        FailPoints::exception_during_query_run,
    };
    size_t query_index = 0;
    for (const auto & failpoint : failpoint_names)
    {
        query_index++;
        for (size_t i = 0; i < 5; ++i)
        {
            auto properties = DB::tests::getDAGPropertiesForTest(serverNum(), query_index, i);
            MPPGatherId gather_id(
                properties.gather_id,
                properties.query_ts,
                properties.local_query_id,
                properties.server_id,
                properties.start_ts,
                /*resource_group_name=*/"",
                0,
                "");
            /// currently all the failpoints are automatically disabled after triggered once, so have to enable it before every run
            FailPointHelper::enableFailPoint(failpoint);
            BlockInputStreamPtr stream;
            try
            {
                auto tasks = prepareMPPTasks(
                    context.scan("test_db", "l_table")
                        .aggregation({Max(col("l_table.s"))}, {col("l_table.s")})
                        .project({col("max(l_table.s)"), col("l_table.s")}),
                    properties);
                executeProblematicMPPTasks(tasks, properties, stream);
            }
            catch (...)
            {
                auto error_message = getCurrentExceptionMessage(false);
                MockComputeServerManager::instance().cancelGather(gather_id);
                ASSERT_TRUE(
                    error_message.find(failpoint) != std::string::npos
                    || error_message.find("tunnel") != std::string::npos)
                    << " error message is " << error_message << " failpoint is " << failpoint;
                EXPECT_TRUE(assertQueryCancelled(gather_id.query_id)) << "fail in " << failpoint;
                FailPointHelper::disableFailPoint(failpoint);
                continue;
            }
            GTEST_FAIL();
        }
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

TEST_F(ComputeServerRunner, testMinTSOActiveSetSoftLimit)
try
{
    UInt64 active_set_soft_limit = 2;
    context.context->setSetting("task_scheduler_active_set_soft_limit", active_set_soft_limit);
    startServers(1);
    setCancelTest();
    ASSERT_TRUE(
        TiFlashMetrics::instance()
            .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_active_queries_count, "")
            .Value()
        == 0);
    ASSERT_TRUE(
        TiFlashMetrics::instance()
            .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_waiting_queries_count, "")
            .Value()
        == 0);
    std::vector<std::thread> running_queries;
    std::vector<MPPGatherId> gather_ids;
    try
    {
        /// case 1, min tso can be added
        for (size_t i = 0; i < active_set_soft_limit; ++i)
        {
            addOneQuery(i + 10, running_queries, gather_ids);
        }
        using namespace std::literals::chrono_literals;
        ADAPTIVE_SLEEP(4s, 8s);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_active_queries_count, "")
                .Value()
            == 2);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_waiting_queries_count, "")
                .Value()
            == 0);
        addOneQuery(1, running_queries, gather_ids);
        ADAPTIVE_SLEEP(4s, 8s);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_active_queries_count, "")
                .Value()
            == 3);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_waiting_queries_count, "")
                .Value()
            == 0);
        for (const auto & gather_id : gather_ids)
            MockComputeServerManager::instance().cancelGather(gather_id);
        for (auto & t : running_queries)
            t.join();
        running_queries.clear();
        gather_ids.clear();
        /// case 2, non-min tso can't be added
        for (size_t i = 0; i < active_set_soft_limit; ++i)
        {
            addOneQuery((i + 1) * 20, running_queries, gather_ids);
        }
        using namespace std::literals::chrono_literals;
        ADAPTIVE_SLEEP(4s, 8s);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_active_queries_count, "")
                .Value()
            == 2);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_waiting_queries_count, "")
                .Value()
            == 0);
        addOneQuery(30, running_queries, gather_ids);
        ADAPTIVE_SLEEP(4s, 8s);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_active_queries_count, "")
                .Value()
            == 2);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_waiting_queries_count, "")
                .Value()
            == 1);
        /// cancel 1 running query
        MockComputeServerManager::instance().cancelGather(gather_ids[0]);
        running_queries[0].join();
        ADAPTIVE_SLEEP(4s, 8s);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_active_queries_count, "")
                .Value()
            == 2);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_waiting_queries_count, "")
                .Value()
            == 0);
        for (size_t i = 1; i < running_queries.size(); i++)
            MockComputeServerManager::instance().cancelGather(gather_ids[i]);
        for (size_t i = 1; i < running_queries.size(); i++)
            running_queries[i].join();
    }
    catch (...)
    {
        for (const auto & gather_id : gather_ids)
            MockComputeServerManager::instance().cancelGather(gather_id);
        for (auto & t : running_queries)
            if (t.joinable())
                t.join();
        throw;
    }
}
CATCH

TEST_F(ComputeServerRunner, testCancelMPPGather)
try
{
    startServers(1);
    setCancelTest();
    ASSERT_TRUE(
        TiFlashMetrics::instance()
            .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_active_queries_count, "")
            .Value()
        == 0);
    ASSERT_TRUE(
        TiFlashMetrics::instance()
            .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_waiting_queries_count, "")
            .Value()
        == 0);
    std::vector<std::thread> running_queries;
    std::vector<MPPGatherId> gather_ids;
    auto multiple_gathers_properties = DB::tests::getDAGPropertiesForTest(serverNum(), 1, 1, 1);
    auto single_gather_properties = DB::tests::getDAGPropertiesForTest(serverNum(), 1, 1, 2);
    try
    {
        for (size_t i = 0; i < 5; ++i)
        {
            multiple_gathers_properties.gather_id = i + 1;
            addOneGather(running_queries, gather_ids, multiple_gathers_properties);
        }
        single_gather_properties.gather_id = 1;
        addOneGather(running_queries, gather_ids, single_gather_properties);
        using namespace std::literals::chrono_literals;
        ADAPTIVE_SLEEP(4s, 16s);
        /// 6 gathers, but two query
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_active_queries_count, "")
                .Value()
            == 2);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_waiting_queries_count, "")
                .Value()
            == 0);
        std::vector<size_t> killed_gathers{0, 2, 4};
        std::vector<size_t> remaining_gathers{1, 3};
        for (const auto i : killed_gathers)
        {
            MockComputeServerManager::instance().cancelGather(gather_ids[i]);
            assertGatherCancelled(gather_ids[i]);
        }
        for (const auto i : remaining_gathers)
        {
            /// these gathers should not be affected
            assertGatherActive(gather_ids[i]);
        }
        /// the active query count should not change
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_active_queries_count, "")
                .Value()
            == 2);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_waiting_queries_count, "")
                .Value()
            == 0);
        /// kill single gather query
        MockComputeServerManager::instance().cancelGather(gather_ids[5]);
        assertGatherCancelled(gather_ids[5]);
        /// the active query count should be 1
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_active_queries_count, "")
                .Value()
            == 1);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_waiting_queries_count, "")
                .Value()
            == 0);
        /// kill the rest gathers
        for (const auto i : remaining_gathers)
        {
            MockComputeServerManager::instance().cancelGather(gather_ids[i]);
            assertGatherCancelled(gather_ids[i]);
        }
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_active_queries_count, "")
                .Value()
            == 0);
        ASSERT_TRUE(
            TiFlashMetrics::instance()
                .tiflash_task_scheduler.get(tiflash_task_scheduler_metrics::type_waiting_queries_count, "")
                .Value()
            == 0);
        for (auto & t : running_queries)
            if (t.joinable())
                t.join();
    }
    catch (...)
    {
        for (const auto & gather_id : gather_ids)
            MockComputeServerManager::instance().cancelGather(gather_id);
        for (auto & t : running_queries)
            if (t.joinable())
                t.join();
        throw;
    }
}
CATCH

TEST_F(ComputeServerRunner, runAggTasks1)
try
{
    WRAP_FOR_SERVER_TEST_BEGIN
    startServers(4);
    // Basic simple test.
    {
        std::vector<String> expected_strings = {
            R"(exchange_sender_4 | type:Hash, {<0, Long>, <1, String>, <2, String>}
 aggregation_3 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  table_scan_0 | {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_4 | type:Hash, {<0, Long>, <1, String>, <2, String>}
 aggregation_3 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  table_scan_0 | {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_4 | type:Hash, {<0, Long>, <1, String>, <2, String>}
 aggregation_3 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  table_scan_0 | {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_4 | type:Hash, {<0, Long>, <1, String>, <2, String>}
 aggregation_3 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  table_scan_0 | {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_2 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
 aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  exchange_receiver_5 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_2 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
 aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  exchange_receiver_5 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_2 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
 aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  exchange_receiver_5 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_2 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
 aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  exchange_receiver_5 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
)"};
        auto expected_cols
            = {toNullableVec<Int32>({1, {}, 10000000, 10000000}),
               toNullableVec<String>({"apple", {}, "banana", "test"}),
               toNullableVec<String>({"apple", {}, "banana", "test"})};

        ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(
            context.scan("test_db", "test_table_1").aggregation({Max(col("s1"))}, {col("s2"), col("s3")}, 0, switcher),
            expected_strings,
            expected_cols);
    }

    // Big dataset test.
    {
        std::vector<String> expected_strings = {
            R"(exchange_sender_4 | type:Hash, {<0, Longlong>, <1, Longlong>}
 aggregation_3 | group_by: {<1, Longlong>}, agg_func: {sum(<0, Longlong>)}
  table_scan_0 | {<0, Longlong>, <1, Longlong>}
)",
            R"(exchange_sender_4 | type:Hash, {<0, Longlong>, <1, Longlong>}
 aggregation_3 | group_by: {<1, Longlong>}, agg_func: {sum(<0, Longlong>)}
  table_scan_0 | {<0, Longlong>, <1, Longlong>}
)",
            R"(exchange_sender_4 | type:Hash, {<0, Longlong>, <1, Longlong>}
 aggregation_3 | group_by: {<1, Longlong>}, agg_func: {sum(<0, Longlong>)}
  table_scan_0 | {<0, Longlong>, <1, Longlong>}
)",
            R"(exchange_sender_4 | type:Hash, {<0, Longlong>, <1, Longlong>}
 aggregation_3 | group_by: {<1, Longlong>}, agg_func: {sum(<0, Longlong>)}
  table_scan_0 | {<0, Longlong>, <1, Longlong>}
)",
            R"(exchange_sender_2 | type:PassThrough, {<0, Longlong>, <1, Longlong>}
 aggregation_1 | group_by: {<1, Longlong>}, agg_func: {sum(<0, Longlong>)}
  exchange_receiver_5 | type:PassThrough, {<0, Longlong>, <1, Longlong>}
)",
            R"(exchange_sender_2 | type:PassThrough, {<0, Longlong>, <1, Longlong>}
 aggregation_1 | group_by: {<1, Longlong>}, agg_func: {sum(<0, Longlong>)}
  exchange_receiver_5 | type:PassThrough, {<0, Longlong>, <1, Longlong>}
)",
            R"(exchange_sender_2 | type:PassThrough, {<0, Longlong>, <1, Longlong>}
 aggregation_1 | group_by: {<1, Longlong>}, agg_func: {sum(<0, Longlong>)}
  exchange_receiver_5 | type:PassThrough, {<0, Longlong>, <1, Longlong>}
)",
            R"(exchange_sender_2 | type:PassThrough, {<0, Longlong>, <1, Longlong>}
 aggregation_1 | group_by: {<1, Longlong>}, agg_func: {sum(<0, Longlong>)}
  exchange_receiver_5 | type:PassThrough, {<0, Longlong>, <1, Longlong>}
)"};
        const size_t distinct_num = 10240;
        const size_t row_num_per_type = 4;
        const size_t row_num = distinct_num * row_num_per_type;

        std::vector<std::optional<Int64>> col1_data;
        col1_data.reserve(row_num);
        std::vector<std::optional<Int64>> col2_data;
        col2_data.reserve(row_num);

        for (size_t i = 0; i < row_num_per_type; ++i)
        {
            for (size_t j = 0; j < distinct_num; ++j)
            {
                col1_data.push_back(j);
                col2_data.push_back(j);
            }
        }

        context.addMockTable(
            {"test_db", "test_auto_pass_through_tbl"},
            {{"col1", TiDB::TP::TypeLongLong}, {"col2", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("col1", col1_data), toNullableVec<Int64>("col2", col2_data)});

        std::vector<std::optional<Int64>> expected_group_by_col_data;
        expected_group_by_col_data.reserve(distinct_num);
        std::vector<std::optional<Int64>> expected_agg_func_col_data;
        expected_agg_func_col_data.reserve(distinct_num);

        for (size_t i = 0; i < distinct_num; ++i)
        {
            expected_group_by_col_data.push_back(i);
            expected_agg_func_col_data.push_back(i * row_num_per_type);
        }

        auto expected_col_datas
            = {toNullableVec<Int64>(expected_agg_func_col_data), toNullableVec<Int64>(expected_group_by_col_data)};

        ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(
            context.scan("test_db", "test_auto_pass_through_tbl")
                .aggregation({Sum(col("col1"))}, {col("col2")}, 0, switcher),
            expected_strings,
            expected_col_datas);
    }

    {
        auto properties = getDAGPropertiesForTest(1);
        auto tasks = context.scan("test_db", "test_table_1")
                         .aggregation({Count(col("s1"))}, {}, 0, switcher)
                         .project({"count(s1)"})
                         .buildMPPTasks(context, properties);
        std::vector<String> expected_strings = {
            R"(exchange_sender_5 | type:PassThrough, {<0, Longlong>}
 aggregation_4 | group_by: {}, agg_func: {count(<0, Long>)}
  table_scan_0 | {<0, Long>}
            )",
            R"(exchange_sender_3 | type:PassThrough, {<0, Longlong>}
 project_2 | {<0, Longlong>}
  aggregation_1 | group_by: {}, agg_func: {sum(<0, Longlong>)}
   exchange_receiver_6 | type:PassThrough, {<0, Longlong>})"};

        size_t task_size = tasks.size();
        for (size_t i = 0; i < task_size; ++i)
        {
            ASSERT_DAGREQUEST_EQAUL(expected_strings[i], tasks[i].dag_request);
        }
    }
    WRAP_FOR_SERVER_TEST_END
}
CATCH

// todo add case to cover two level hashmap; spill
// Using different workload(low/high/medium ndv) to run auto_pass_through hashagg,
// which cover logic of interpretion, execution and exchange.
TEST_F(ComputeServerRunner, autoPassThroughIntegrationTest)
try
{
    auto workloads = std::vector{
        auto_pass_through_test_data.low_ndv_tbl_name,
        auto_pass_through_test_data.high_ndv_tbl_name,
        auto_pass_through_test_data.medium_ndv_tbl_name,
        auto_pass_through_test_data.nullable_low_ndv_tbl_name,
        auto_pass_through_test_data.nullable_high_ndv_tbl_name,
        auto_pass_through_test_data.nullable_medium_ndv_tbl_name,
    };
    for (const auto & tbl_name : workloads)
    {
        const String db_name = auto_pass_through_test_data.db_name;
        const String col1_name = auto_pass_through_test_data.col1_name;
        const String col2_name = auto_pass_through_test_data.col2_name;
        const String col4_name = auto_pass_through_test_data.col4_name;

        // todo other agg funcs.
        MockAstVec agg_func_asts{
            makeASTFunction("first_row", col(col1_name)),
            makeASTFunction("first_row", col(col2_name)),
            makeASTFunction("min", col(col2_name)),
            makeASTFunction("max", col(col2_name)),
            makeASTFunction("sum", col(col2_name)),
            makeASTFunction("count", col(col2_name)),
            makeASTFunction("first_row", col(col4_name)),
            makeASTFunction("min", col(col4_name)),
            makeASTFunction("max", col(col4_name)),
            makeASTFunction("sum", col(col4_name)),
            makeASTFunction("count", col(col4_name)),
            makeASTFunction("sum", lit(Field(static_cast<UInt64>(1)))),
            makeASTFunction("count", lit(Field(static_cast<UInt64>(1)))),
            makeASTFunction("min", lit(Field(static_cast<UInt64>(1)))),
            makeASTFunction("max", lit(Field(static_cast<UInt64>(1)))),
        };
        LOG_DEBUG(Logger::get(), "TestAutoPassThroughAggContext iteration, tbl_name: {}", tbl_name);

        auto builder = context.scan(db_name, tbl_name).aggregation(agg_func_asts, {col(col1_name)}, 0, nullptr);
        startServers(2);
        auto res_no_pass_through = getResultBlocks(context, builder, serverNum());

        // 2-staged Aggregation.
        // Expect the columns result is same with non-auto_pass_through hashagg.
        WRAP_FOR_SERVER_TEST_BEGIN
        LOG_DEBUG(Logger::get(), "TestAutoPassThroughAggContext iteration, start test auto pass");
        std::vector<String> expected_strings = {
            R"(exchange_sender_4 | type:Hash, {<0, String>, <1, Longlong>, <2, Longlong>, <3, Longlong>, <4, Longlong>, <5, Longlong>, <6, Tiny>, <7, Tiny>, <8, Tiny>, <9, Tiny>, <10, Longlong>, <11, Longlong>, <12, Longlong>, <13, Longlong>, <14, Longlong>, <15, String>}
 aggregation_3 | group_by: {<0, String>}, agg_func: {first_row(<0, String>), first_row(<1, Longlong>), min(<1, Longlong>), max(<1, Longlong>), sum(<1, Longlong>), count(<1, Longlong>), first_row(<2, Tiny>), min(<2, Tiny>), max(<2, Tiny>), sum(<2, Tiny>), count(<2, Tiny>), sum(<UInt64_1, Tiny>), count(<UInt64_1, Tiny>), min(<UInt64_1, Tiny>), max(<UInt64_1, Tiny>)}
  table_scan_0 | {<0, String>, <1, Longlong>, <2, Tiny>}
)",
            R"(exchange_sender_4 | type:Hash, {<0, String>, <1, Longlong>, <2, Longlong>, <3, Longlong>, <4, Longlong>, <5, Longlong>, <6, Tiny>, <7, Tiny>, <8, Tiny>, <9, Tiny>, <10, Longlong>, <11, Longlong>, <12, Longlong>, <13, Longlong>, <14, Longlong>, <15, String>}
 aggregation_3 | group_by: {<0, String>}, agg_func: {first_row(<0, String>), first_row(<1, Longlong>), min(<1, Longlong>), max(<1, Longlong>), sum(<1, Longlong>), count(<1, Longlong>), first_row(<2, Tiny>), min(<2, Tiny>), max(<2, Tiny>), sum(<2, Tiny>), count(<2, Tiny>), sum(<UInt64_1, Tiny>), count(<UInt64_1, Tiny>), min(<UInt64_1, Tiny>), max(<UInt64_1, Tiny>)}
  table_scan_0 | {<0, String>, <1, Longlong>, <2, Tiny>}
)",
            R"(exchange_sender_2 | type:PassThrough, {<0, String>, <1, Longlong>, <2, Longlong>, <3, Longlong>, <4, Longlong>, <5, Longlong>, <6, Tiny>, <7, Tiny>, <8, Tiny>, <9, Tiny>, <10, Longlong>, <11, Longlong>, <12, Longlong>, <13, Longlong>, <14, Longlong>, <15, String>}
 aggregation_1 | group_by: {<15, String>}, agg_func: {first_row(<0, String>), first_row(<1, Longlong>), min(<2, Longlong>), max(<3, Longlong>), sum(<4, Longlong>), sum(<5, Longlong>), first_row(<6, Tiny>), min(<7, Tiny>), max(<8, Tiny>), sum(<9, Tiny>), sum(<10, Longlong>), sum(<11, Longlong>), sum(<12, Longlong>), min(<13, Longlong>), max(<14, Longlong>)}
  exchange_receiver_5 | type:PassThrough, {<0, String>, <1, Longlong>, <2, Longlong>, <3, Longlong>, <4, Longlong>, <5, Longlong>, <6, Tiny>, <7, Tiny>, <8, Tiny>, <9, Tiny>, <10, Longlong>, <11, Longlong>, <12, Longlong>, <13, Longlong>, <14, Longlong>, <15, String>}
)",
            R"(exchange_sender_2 | type:PassThrough, {<0, String>, <1, Longlong>, <2, Longlong>, <3, Longlong>, <4, Longlong>, <5, Longlong>, <6, Tiny>, <7, Tiny>, <8, Tiny>, <9, Tiny>, <10, Longlong>, <11, Longlong>, <12, Longlong>, <13, Longlong>, <14, Longlong>, <15, String>}
 aggregation_1 | group_by: {<15, String>}, agg_func: {first_row(<0, String>), first_row(<1, Longlong>), min(<2, Longlong>), max(<3, Longlong>), sum(<4, Longlong>), sum(<5, Longlong>), first_row(<6, Tiny>), min(<7, Tiny>), max(<8, Tiny>), sum(<9, Tiny>), sum(<10, Longlong>), sum(<11, Longlong>), sum(<12, Longlong>), min(<13, Longlong>), max(<14, Longlong>)}
  exchange_receiver_5 | type:PassThrough, {<0, String>, <1, Longlong>, <2, Longlong>, <3, Longlong>, <4, Longlong>, <5, Longlong>, <6, Tiny>, <7, Tiny>, <8, Tiny>, <9, Tiny>, <10, Longlong>, <11, Longlong>, <12, Longlong>, <13, Longlong>, <14, Longlong>, <15, String>}
)"};
        ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(
            context.scan(db_name, tbl_name).aggregation(agg_func_asts, {col(col1_name)}, 0, switcher),
            expected_strings,
            res_no_pass_through);
        WRAP_FOR_SERVER_TEST_END
    }
}
CATCH

TEST_F(ComputeServerRunner, stateSwitchMediumNDV)
try
{
    auto & auto_pass_through_context = auto_pass_through_test_data.auto_pass_through_context;
    auto & medium_ndv_blocks = auto_pass_through_test_data.medium_ndv_blocks;
    auto & random_blocks = auto_pass_through_test_data.random_blocks;
    auto log = Logger::get();

    // Expect InitState
    ASSERT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Init);
    size_t init_consumed_block = 0;
    while (auto_pass_through_context->getCurState() == AutoPassThroughHashAggContext::State::Init
           && !random_blocks.empty())
    {
        ++init_consumed_block;
        auto_pass_through_context->onBlock<false>(random_blocks.front());
        random_blocks.pop_front();
    }
    LOG_DEBUG(log, "ComputeServerRunner.stateSwitchMediumNDV init_consumed_block: {}", init_consumed_block);

    const auto state_processed_row_limit = auto_pass_through_context->getOtherStateRowLimit();
    const auto adjust_state_rows_limit = auto_pass_through_context->getAdjustRowLimit();

    size_t state_processed_rows = 0;
    while (!medium_ndv_blocks.empty())
    {
        while (!medium_ndv_blocks.empty())
        {
            auto block = medium_ndv_blocks.front();
            medium_ndv_blocks.pop_front();
            auto_pass_through_context->onBlock<false>(block);
            state_processed_rows += block.rows();
            LOG_DEBUG(
                log,
                "stateSwitchMediumNDV execute one block, state: {}, cur state processed rows: {}, state rows limit: {}",
                magic_enum::enum_name(auto_pass_through_context->getCurState()),
                state_processed_rows,
                adjust_state_rows_limit);
            if (state_processed_rows < adjust_state_rows_limit)
            {
                ASSERT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Adjust);
            }
            else
            {
                state_processed_rows = 0;
                ASSERT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Selective);
                break;
            }
        }

        while (!medium_ndv_blocks.empty())
        {
            auto block = medium_ndv_blocks.front();
            medium_ndv_blocks.pop_front();
            auto_pass_through_context->onBlock<false>(block);
            state_processed_rows += block.rows();
            LOG_DEBUG(
                log,
                "stateSwitchMediumNDV execute one block, state: {}, cur state processed rows: {}, state rows limit: {}",
                magic_enum::enum_name(auto_pass_through_context->getCurState()),
                state_processed_rows,
                state_processed_row_limit);
            if (state_processed_rows < state_processed_row_limit)
            {
                ASSERT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Selective);
            }
            else
            {
                state_processed_rows = 0;
                ASSERT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Adjust);
                break;
            }
        }
    }
}
CATCH

TEST_F(ComputeServerRunner, autoPassThroughEmptyTable)
try
{
    std::vector<String> expected_strings = {
        R"(exchange_sender_4 | type:PassThrough, {<0, Longlong>}
 aggregation_3 | group_by: {}, agg_func: {count(<0, Long>)}
  table_scan_0 | {<0, Long>}
 )",
        R"(exchange_sender_4 | type:PassThrough, {<0, Longlong>}
 aggregation_3 | group_by: {}, agg_func: {count(<0, Long>)}
  table_scan_0 | {<0, Long>}
 )",
        R"(exchange_sender_2 | type:PassThrough, {<0, Longlong>}
 aggregation_1 | group_by: {}, agg_func: {sum(<0, Longlong>)}
  exchange_receiver_5 | type:PassThrough, {<0, Longlong>}
 )"};
    startServers(2);
    auto expected_cols = {toVec<UInt64>("count", {0})};
    ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(
        context.scan("test_db", "auto_pass_through_empty_tbl")
            .aggregation({makeASTFunction("count", col("col1"))}, {}, 0, switcher),
        expected_strings,
        expected_cols);
}
CATCH

#undef WRAP_FOR_SERVER_TEST_BEGIN
#undef WRAP_FOR_SERVER_TEST_END

} // namespace tests
} // namespace DB
