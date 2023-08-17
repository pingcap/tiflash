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

#pragma once

#include <TestUtils/ExecutorTestUtils.h>

#include <unordered_map>
#include <vector>

namespace DB::tests
{
class InterpreterTestUtils : public ExecutorTest
{
public:
    void runAndAssert(const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency);

protected:
    void initExpectResults();
    void appendExpectResults();

    void SetUp() override;
    void TearDown() override;

    void setRecord() { just_record = true; }

private:
    // The following steps update the expected results of cases in bulk
    // 1. manually delete the *.out file
    // 2. call setRecord()
    // 3. run unit test cases
    bool just_record = false;

    // <func_name, vector<result>>
    std::unordered_map<String, std::vector<String>> case_expect_results;
    size_t expect_result_index = 0;
};

} // namespace DB::tests
