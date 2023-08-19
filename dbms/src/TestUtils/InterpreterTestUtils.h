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

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Common/FmtUtils.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TestUtils/executorSerializer.h>
#include <TestUtils/mockExecutor.h>
namespace DB::tests
{
void executeInterpreter(const std::shared_ptr<tipb::DAGRequest> & request, Context & context);
class InterpreterTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        initializeContext();
        initializeClientInfo();
    }

public:
    InterpreterTest()
        : context(TiFlashTestEnv::getContext())
    {}
    static void SetUpTestCase();

    virtual void initializeContext();

    void initializeClientInfo();

    DAGContext & getDAGContext();

    static void dagRequestEqual(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & actual);

    void executeInterpreter(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency);

protected:
    MockDAGRequestContext context;
    std::unique_ptr<DAGContext> dag_context_ptr;
};

#define ASSERT_DAGREQUEST_EQAUL(str, request) dagRequestEqual((str), (request));
#define ASSERT_BLOCKINPUTSTREAM_EQAUL(str, request, concurrency) executeInterpreter((str), (request), (concurrency))
} // namespace DB::tests