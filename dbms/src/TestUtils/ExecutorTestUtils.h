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

namespace DB::tests
{
void executeInterpreter(const std::shared_ptr<tipb::DAGRequest> & request, Context & context);
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

    static void dagRequestEqual(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & actual);

    void executeInterpreter(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency);

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

    void executeStreams(
        const std::shared_ptr<tipb::DAGRequest> & request,
        std::unordered_map<String, ColumnsWithTypeAndName> & source_columns_map,
        const ColumnsWithTypeAndName & expect_columns,
        size_t concurrency = 1);
    void executeStreams(
        const std::shared_ptr<tipb::DAGRequest> & request,
        const ColumnsWithTypeAndName & expect_columns,
        size_t concurrency = 1);

    void executeStreamsWithSingleSource(
        const std::shared_ptr<tipb::DAGRequest> & request,
        const ColumnsWithTypeAndName & source_columns,
        const ColumnsWithTypeAndName & expect_columns,
        SourceType type = TableScan,
        size_t concurrency = 1);

protected:
    MockDAGRequestContext context;
    std::unique_ptr<DAGContext> dag_context_ptr;
};

#define ASSERT_DAGREQUEST_EQAUL(str, request) dagRequestEqual((str), (request));
#define ASSERT_BLOCKINPUTSTREAM_EQAUL(str, request, concurrency) executeInterpreter((str), (request), (concurrency))
} // namespace DB::tests