<<<<<<< HEAD
// Copyright 2022 PingCAP, Ltd.
=======
// Copyright 2023 PingCAP, Inc.
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
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

#include <Common/FmtUtils.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Interpreters/executeQuery.h>
#include <TestUtils/InterpreterTestUtils.h>
#include <TestUtils/executorSerializer.h>
namespace DB::tests
{
DAGContext & InterpreterTest::getDAGContext()
{
    assert(dag_context_ptr != nullptr);
    return *dag_context_ptr;
}

void InterpreterTest::initializeContext()
{
    dag_context_ptr = std::make_unique<DAGContext>(1024);
    context = MockDAGRequestContext(TiFlashTestEnv::getContext());
    dag_context_ptr->log = Logger::get("interpreterTest");
}

void InterpreterTest::SetUpTestCase()
{
    try
    {
        DB::registerFunctions();
        DB::registerAggregateFunctions();
    }
    catch (DB::Exception &)
    {
        // Maybe another test has already registered, ignore exception here.
    }
}

void InterpreterTest::initializeClientInfo()
{
<<<<<<< HEAD
    context.context.setCurrentQueryId("test");
    ClientInfo & client_info = context.context.getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::GRPC;
=======
    if (!just_record)
        return;

    auto out_path = getOutputPath();
    Poco::FileOutputStream fos(out_path, std::ios::out | std::ios::app);
    for (const auto & suite_entry : case_expect_results)
    {
        const auto & suite_key = suite_entry.first;
        for (size_t i = 0; i < suite_entry.second.size(); ++i)
        {
            fos << suite_key << "\n~result_index: " << i << '\n' << suite_entry.second[i] << "\n@\n";
        }
    }
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
}

void InterpreterTest::executeInterpreter(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency)
{
    DAGContext dag_context(*request, "interpreter_test", concurrency);
    context.context.setDAGContext(&dag_context);
    // Currently, don't care about regions information in interpreter tests.
    DAGQuerySource dag(context.context);
    auto res = executeQuery(dag, context.context, false, QueryProcessingStage::Complete);
    FmtBuffer fb;
    res.in->dumpTree(fb);
    ASSERT_EQ(Poco::trim(expected_string), Poco::trim(fb.toString()));
}

void InterpreterTest::dagRequestEqual(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & actual)
{
    ASSERT_EQ(Poco::trim(expected_string), Poco::trim(ExecutorSerializer().serialize(actual.get())));
}

<<<<<<< HEAD
=======
void InterpreterTestUtils::runAndAssert(const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency)
{
    const auto & test_info = testing::UnitTest::GetInstance()->current_test_info();
    assert(test_info);
    String test_suite_name = test_info->name();
    assert(!test_suite_name.empty());
    auto dag_request_str = Poco::trim(ExecutorSerializer().serialize(request.get()));
    auto test_info_msg = [&]() {
        return fmt::format(
            "test info:\n"
            "    file: {}\n"
            "    line: {}\n"
            "    test_case_name: {}\n"
            "    test_suite_name: {}\n"
            "    dag_request: \n{}",
            test_info->file(),
            test_info->line(),
            test_info->test_case_name(),
            test_suite_name,
            dag_request_str);
    };
    auto suite_key = fmt::format("~test_suite_name: {}", test_suite_name);

    DAGContext dag_context(*request, "interpreter_test", concurrency);
    TiFlashTestEnv::setUpTestContext(*context.context, &dag_context, context.mockStorage(), TestType::INTERPRETER_TEST);
    // Don't care regions information in interpreter tests.
    auto query_executor = queryExecute(*context.context, /*internal=*/true);
    auto compare_result = fmt::format("~result:\n{}", Poco::trim(query_executor->toString()));
    auto cur_result_index = expect_result_index++;

    if (just_record)
    {
        assert(case_expect_results[suite_key].size() == cur_result_index);
        case_expect_results[suite_key].push_back(compare_result);
        return;
    }

    auto it = case_expect_results.find(suite_key);
    if (it == case_expect_results.end())
        FAIL() << "can not find expect result\n" << test_info_msg();

    const auto & func_expect_results = it->second;
    assert(func_expect_results.size() > cur_result_index);
    String expect_string = func_expect_results[cur_result_index];
    ASSERT_EQ(expect_string, compare_result) << test_info_msg();
}
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
} // namespace DB::tests
