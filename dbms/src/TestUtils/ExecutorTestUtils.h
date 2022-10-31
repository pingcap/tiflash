// Copyright 2022 PingCAP, Ltd.
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

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/executorSerializer.h>
#include <TestUtils/mockExecutor.h>
#include <WindowFunctions/registerWindowFunctions.h>

#include <functional>

namespace DB::tests
{
TiDB::TP dataTypeToTP(const DataTypePtr & type);

ColumnsWithTypeAndName readBlock(BlockInputStreamPtr stream);
ColumnsWithTypeAndName readBlocks(std::vector<BlockInputStreamPtr> streams);
Block mergeBlocks(Blocks blocks);


#define WRAP_FOR_DIS_ENABLE_PLANNER_BEGIN \
    std::vector<bool> bools{false, true}; \
    for (auto enable_planner : bools)     \
    {                                     \
        enablePlanner(enable_planner);

#define WRAP_FOR_DIS_ENABLE_PLANNER_END }

class ExecutorTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        initializeContext();
        initializeClientInfo();
    }

public:
    ExecutorTest()
        : context(TiFlashTestEnv::getContext())
    {}

    static void SetUpTestCase();

    virtual void initializeContext();

    void initializeClientInfo();

    DAGContext & getDAGContext();

    void enablePlanner(bool is_enable);

    static void dagRequestEqual(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & actual);

    void executeInterpreter(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency);
    ColumnsWithTypeAndName executeRawQuery(const String & query, size_t concurrency = 1);
    void executeAndAssertColumnsEqual(const std::shared_ptr<tipb::DAGRequest> & request, const ColumnsWithTypeAndName & expect_columns);
    void executeAndAssertRowsEqual(const std::shared_ptr<tipb::DAGRequest> & request, size_t expect_rows);

    enum SourceType
    {
        TableScan,
        ExchangeReceiver
    };

    // for single source query, the source executor name is ${type}_0
    static String getSourceName(SourceType type)
    {
        switch (type)
        {
        case TableScan:
            return "table_scan_0";
        case ExchangeReceiver:
            return "exchange_receiver_0";
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Unknown Executor Source type {}",
                            type);
        }
    }

    ColumnsWithTypeAndName executeStreams(
        const std::shared_ptr<tipb::DAGRequest> & request,
        size_t concurrency = 1);

private:
    void executeExecutor(
        const std::shared_ptr<tipb::DAGRequest> & request,
        std::function<::testing::AssertionResult(const ColumnsWithTypeAndName &)> assert_func);

protected:
    MockDAGRequestContext context;
    std::unique_ptr<DAGContext> dag_context_ptr;
};

#define ASSERT_DAGREQUEST_EQAUL(str, request) dagRequestEqual((str), (request));
#define ASSERT_BLOCKINPUTSTREAM_EQAUL(str, request, concurrency) executeInterpreter((str), (request), (concurrency))

} // namespace DB::tests
