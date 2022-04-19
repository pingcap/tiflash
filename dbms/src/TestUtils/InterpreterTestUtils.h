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
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TestUtils/mockExecutor.h>
namespace DB::tests
{
void dagRequestEqual(String & expected_string, const std::shared_ptr<tipb::DAGRequest> & actual);
class MockExecutorTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        initializeContext();
    }

public:
    MockExecutorTest()
        : context(TiFlashTestEnv::getContext())
    {}

    static void SetUpTestCase()
    {
        try
        {
            DB::registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registered, ignore exception here.
        }
    }

    virtual void initializeContext()
    {
        dag_context_ptr = std::make_unique<DAGContext>(1024);
        context.setDAGContext(dag_context_ptr.get());
        mock_dag_request_context = MockDAGRequestContext();
    }

    DAGContext & getDAGContext()
    {
        assert(dag_context_ptr != nullptr);
        return *dag_context_ptr;
    }

protected:
    Context context;
    MockDAGRequestContext mock_dag_request_context;
    std::unique_ptr<DAGContext> dag_context_ptr;
};

#define ASSERT_DAGREQUEST_EQAUL(str, request) dagRequestEqual(str, request);
} // namespace DB::tests