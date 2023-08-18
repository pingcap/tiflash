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

#include <Common/FmtUtils.h>
#include <Common/StringUtils/StringUtils.h>
#include <Debug/MockStorage.h>
#include <Flash/executeQuery.h>
#include <Poco/File.h>
#include <Poco/FileStream.h>
#include <TestUtils/ExecutorSerializer.h>
#include <TestUtils/InterpreterTestUtils.h>

#include <string>

namespace DB::tests
{
namespace
{
std::vector<std::string> stringSplit(const String & str, char delim)
{
    std::stringstream ss(str);
    std::string item;
    std::vector<std::string> elems;
    while (std::getline(ss, item, delim))
    {
        if (!item.empty())
            elems.push_back(item);
    }
    return elems;
}

String getOutputPath()
{
    const auto & test_info = testing::UnitTest::GetInstance()->current_test_info();
    assert(test_info);
    String file_name = test_info->file();
    auto out_path = file_name.replace(file_name.find(".cpp"), 4, ".out");
    Poco::File file(out_path);
    if (!file.exists())
        file.createFile();
    return out_path;
}
} // namespace

void InterpreterTestUtils::initExpectResults()
{
    if (just_record)
    {
        case_expect_results.clear();
        return;
    }

    auto out_path = getOutputPath();
    Poco::FileInputStream fis(out_path);
    String buf;
    while (std::getline(fis, buf, '@'))
    {
        buf = Poco::trim(buf);
        if (!buf.empty())
        {
            auto spilts = stringSplit(buf, '~');
            assert(spilts.size() == 3);
            auto suite_key = fmt::format("~{}", Poco::trim(spilts[0]));
            auto unit_result = fmt::format("~{}", Poco::trim(spilts[2]));
            case_expect_results[suite_key].push_back(unit_result);
        }
    }
}

void InterpreterTestUtils::appendExpectResults()
{
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
}

void InterpreterTestUtils::SetUp()
{
    ExecutorTest::SetUp();
    initExpectResults();
}

void InterpreterTestUtils::TearDown()
{
    appendExpectResults();
    ExecutorTest::TearDown();
}

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
} // namespace DB::tests
