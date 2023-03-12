// Copyright 2023 PingCAP, Ltd.
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
#include <Common/FmtUtils.h>
#include <Debug/MockComputeServerManager.h>
#include <Debug/MockStorage.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/executeQuery.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/executorSerializer.h>

#include <functional>

namespace DB::tests
{
TiDB::TP dataTypeToTP(const DataTypePtr & type)
{
    // TODO support more types.
    switch (removeNullable(type)->getTypeId())
    {
    case TypeIndex::UInt8:
    case TypeIndex::Int8:
        return TiDB::TP::TypeTiny;
    case TypeIndex::UInt16:
    case TypeIndex::Int16:
        return TiDB::TP::TypeShort;
    case TypeIndex::UInt32:
    case TypeIndex::Int32:
        return TiDB::TP::TypeLong;
    case TypeIndex::UInt64:
    case TypeIndex::Int64:
        return TiDB::TP::TypeLongLong;
    case TypeIndex::String:
        return TiDB::TP::TypeString;
    case TypeIndex::Float32:
        return TiDB::TP::TypeFloat;
    case TypeIndex::Float64:
        return TiDB::TP::TypeDouble;
    default:
        throw Exception("Unsupport type");
    }
}

void ExecutorTest::SetUp()
{
    initializeContext();
    initializeClientInfo();
    TaskSchedulerConfig config{8};
    assert(!TaskScheduler::instance);
    TaskScheduler::instance = std::make_unique<TaskScheduler>(config);
}

void ExecutorTest::TearDown()
{
    if (context.mockStorage())
        context.mockStorage()->clear();

    assert(TaskScheduler::instance);
    TaskScheduler::instance.reset();
}

DAGContext & ExecutorTest::getDAGContext()
{
    assert(dag_context_ptr != nullptr);
    return *dag_context_ptr;
}

void ExecutorTest::initializeContext()
{
    dag_context_ptr = std::make_unique<DAGContext>(1024);
    context = MockDAGRequestContext(TiFlashTestEnv::getContext());
    context.initMockStorage();
    dag_context_ptr->log = Logger::get("executorTest");
    TiFlashTestEnv::getGlobalContext().setExecutorTest();
}

void ExecutorTest::SetUpTestCase()
{
    auto register_func = [](std::function<void()> func) {
        try
        {
            func();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registered, ignore exception here.
        }
    };

    register_func(DB::registerFunctions);
    register_func(DB::registerAggregateFunctions);
    register_func(DB::registerWindowFunctions);
}

void ExecutorTest::initializeClientInfo()
{
    context.context->setCurrentQueryId("test");
    ClientInfo & client_info = context.context->getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::GRPC;
}

void ExecutorTest::executeInterpreter(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency)
{
    DAGContext dag_context(*request, "interpreter_test", concurrency);
    TiFlashTestEnv::setUpTestContext(*context.context, &dag_context, context.mockStorage(), TestType::INTERPRETER_TEST);

    // Don't care regions information in interpreter tests.
    auto query_executor = queryExecute(*context.context, /*internal=*/true);
    ASSERT_EQ(Poco::trim(expected_string), Poco::trim(query_executor->toString()));
}

void ExecutorTest::executeInterpreterWithDeltaMerge(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency)
{
    DAGContext dag_context(*request, "interpreter_test_with_delta_merge", concurrency);
    TiFlashTestEnv::setUpTestContext(*context.context, &dag_context, context.mockStorage(), TestType::EXECUTOR_TEST);
    // Don't care regions information in interpreter tests.
    auto query_executor = queryExecute(*context.context, /*internal=*/true);
    ASSERT_EQ(Poco::trim(expected_string), Poco::trim(query_executor->toString()));
}

namespace
{
String testInfoMsg(const std::shared_ptr<tipb::DAGRequest> & request, bool enable_planner, bool enable_pipeline, size_t concurrency, size_t block_size)
{
    const auto & test_info = testing::UnitTest::GetInstance()->current_test_info();
    assert(test_info);
    return fmt::format(
        "test info:\n"
        "    file: {}\n"
        "    line: {}\n"
        "    test_case_name: {}\n"
        "    test_func_name: {}\n"
        "    enable_planner: {}\n"
        "    enable_pipeline: {}\n"
        "    concurrency: {}\n"
        "    block_size: {}\n"
        "    dag_request: \n{}",
        test_info->file(),
        test_info->line(),
        test_info->test_case_name(),
        test_info->name(),
        enable_planner,
        enable_pipeline,
        concurrency,
        block_size,
        ExecutorSerializer().serialize(request.get()));
}
} // namespace

void ExecutorTest::executeExecutor(
    const std::shared_ptr<tipb::DAGRequest> & request,
    std::function<::testing::AssertionResult(const ColumnsWithTypeAndName &)> assert_func)
{
    WRAP_FOR_TEST_BEGIN
    std::vector<size_t> concurrencies{1, 2, 10};
    for (auto concurrency : concurrencies)
    {
        std::vector<size_t> block_sizes{1, 2, DEFAULT_BLOCK_SIZE};
        for (auto block_size : block_sizes)
        {
            context.context->setSetting("max_block_size", Field(static_cast<UInt64>(block_size)));
            auto res = executeStreams(request, concurrency);
            ASSERT_TRUE(assert_func(res)) << testInfoMsg(request, enable_planner, enable_pipeline, concurrency, block_size);
        }
    }
    WRAP_FOR_TEST_END
}

void ExecutorTest::checkBlockSorted(
    const std::shared_ptr<tipb::DAGRequest> & request,
    const SortInfos & sort_infos,
    std::function<::testing::AssertionResult(const ColumnsWithTypeAndName &, const ColumnsWithTypeAndName &)> assert_func)
{
    WRAP_FOR_TEST_BEGIN
    std::vector<size_t> concurrencies{2, 5, 10};
    for (auto concurrency : concurrencies)
    {
        auto expected_res = executeStreams(request, concurrency);
        std::vector<size_t> block_sizes{1, 2, DEFAULT_BLOCK_SIZE};
        for (auto block_size : block_sizes)
        {
            context.context->setSetting("max_block_size", Field(static_cast<UInt64>(block_size)));
            auto return_blocks = getExecuteStreamsReturnBlocks(request, concurrency);
            if (!return_blocks.empty())
            {
                SortDescription sort_desc;
                for (auto sort_info : sort_infos)
                    sort_desc.emplace_back(return_blocks.back().getColumnsWithTypeAndName()[sort_info.column_index].name, sort_info.desc ? -1 : 1, -1);

                for (auto & block : return_blocks)
                    ASSERT_TRUE(isAlreadySorted(block, sort_desc)) << testInfoMsg(request, enable_planner, enable_pipeline, concurrency, block_size);

                auto res = vstackBlocks(std::move(return_blocks)).getColumnsWithTypeAndName();
                ASSERT_TRUE(assert_func(expected_res, res)) << testInfoMsg(request, enable_planner, enable_pipeline, concurrency, block_size);
            }
        };
        WRAP_FOR_TEST_END
    }
}

void ExecutorTest::executeAndAssertColumnsEqual(const std::shared_ptr<tipb::DAGRequest> & request, const ColumnsWithTypeAndName & expect_columns)
{
    executeExecutor(
        request,
        [&](const ColumnsWithTypeAndName & res) {
            return columnsEqual(expect_columns, res, /*_restrict=*/false) << "\n  expect_block: \n"
                                                                          << getColumnsContent(expect_columns)
                                                                          << "\n actual_block: \n"
                                                                          << getColumnsContent(res);
        });
}

void ExecutorTest::executeAndAssertSortedBlocks(const std::shared_ptr<tipb::DAGRequest> & request, const SortInfos & sort_infos)
{
    checkBlockSorted(request, sort_infos, [&](const ColumnsWithTypeAndName & expect_columns, const ColumnsWithTypeAndName & res) {
        return columnsEqual(expect_columns, res, /*_restrict=*/false) << "\n  expect_block: \n"
                                                                      << getColumnsContent(expect_columns)
                                                                      << "\n actual_block: \n"
                                                                      << getColumnsContent(res);
    });
}

void ExecutorTest::executeAndAssertRowsEqual(const std::shared_ptr<tipb::DAGRequest> & request, size_t expect_rows)
{
    executeExecutor(request, [&](const ColumnsWithTypeAndName & res) {
        auto actual_rows = Block(res).rows();
        if (expect_rows != actual_rows)
        {
            String msg = fmt::format("\nColumns rows mismatch\nexpected_rows: {}\nactual_rows: {}", expect_rows, actual_rows);
            return testing::AssertionFailure() << msg;
        }
        return testing::AssertionSuccess();
    });
}

void readStream(Blocks & blocks, BlockInputStreamPtr stream)
{
    stream->readPrefix();
    while (auto block = stream->read())
    {
        blocks.push_back(block);
    }
    stream->readSuffix();
}

DB::ColumnsWithTypeAndName readBlock(BlockInputStreamPtr stream)
{
    return readBlocks({stream});
}

DB::ColumnsWithTypeAndName readBlocks(std::vector<BlockInputStreamPtr> streams)
{
    Blocks actual_blocks;
    for (const auto & stream : streams)
        readStream(actual_blocks, stream);
    return vstackBlocks(std::move(actual_blocks)).getColumnsWithTypeAndName();
}

void ExecutorTest::enablePlanner(bool is_enable)
{
    context.context->setSetting("enable_planner", is_enable ? "true" : "false");
}

void ExecutorTest::enablePipeline(bool is_enable)
{
    context.context->setSetting("enable_pipeline", is_enable ? "true" : "false");
}

DB::ColumnsWithTypeAndName ExecutorTest::executeStreams(
    const std::shared_ptr<tipb::DAGRequest> & request,
    size_t concurrency,
    bool enable_memory_tracker)
{
    DAGContext dag_context(*request, "executor_test", concurrency);
    return executeStreams(&dag_context, enable_memory_tracker);
}

ColumnsWithTypeAndName ExecutorTest::executeStreams(DAGContext * dag_context, bool enable_memory_tracker)
{
    TiFlashTestEnv::setUpTestContext(*context.context, dag_context, context.mockStorage(), TestType::EXECUTOR_TEST);
    // Currently, don't care about regions information in tests.
    Blocks blocks;
    queryExecute(*context.context, /*internal=*/!enable_memory_tracker)->execute([&blocks](const Block & block) { blocks.push_back(block); }).verify();
    return vstackBlocks(std::move(blocks)).getColumnsWithTypeAndName();
}

Blocks ExecutorTest::getExecuteStreamsReturnBlocks(const std::shared_ptr<tipb::DAGRequest> & request,
                                                   size_t concurrency,
                                                   bool enable_memory_tracker)
{
    DAGContext dag_context(*request, "executor_test", concurrency);
    TiFlashTestEnv::setUpTestContext(*context.context, &dag_context, context.mockStorage(), TestType::EXECUTOR_TEST);
    // Currently, don't care about regions information in tests.
    Blocks blocks;
    queryExecute(*context.context, /*internal=*/!enable_memory_tracker)->execute([&blocks](const Block & block) { blocks.push_back(block); }).verify();
    return blocks;
}

DB::ColumnsWithTypeAndName ExecutorTest::executeRawQuery(const String & query, size_t concurrency)
{
    DAGProperties properties;
    properties.is_mpp_query = false;
    properties.start_ts = 1;
    auto [query_tasks, func_wrap_output_stream] = compileQuery(
        *context.context,
        query,
        [&](const String & database_name, const String & table_name) {
            return context.mockStorage()->getTableInfo(database_name + "." + table_name);
        },
        properties);

    return executeStreams(query_tasks[0].dag_request, concurrency);
}

void ExecutorTest::dagRequestEqual(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & actual)
{
    ASSERT_EQ(Poco::trim(expected_string), Poco::trim(ExecutorSerializer().serialize(actual.get())));
}

} // namespace DB::tests
