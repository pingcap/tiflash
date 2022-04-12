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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
String toTreeString(const tipb::Executor & root_executor, size_t level = 0);

String toTreeString(const tipb::DAGRequest * dag_request)
{
    assert((dag_request->executors_size() > 0) != dag_request->has_root_executor());
    if (dag_request->has_root_executor())
    {
        return toTreeString(dag_request->root_executor());
    }
    else
    {
        FmtBuffer buffer;
        String prefix;
        traverseExecutors(dag_request, [&buffer, &prefix](const tipb::Executor & executor) {
            assert(executor.has_executor_id());
            buffer.fmtAppend("{}{}\n", prefix, executor.executor_id());
            prefix.append(" ");
            return true;
        });
        return buffer.toString();
    }
}

String toTreeString(const tipb::Executor & root_executor, size_t level)
{
    FmtBuffer buffer;

    auto append_str = [&buffer, &level](const tipb::Executor & executor) {
        assert(executor.has_executor_id());
        for (size_t i = 0; i < level; ++i)
            buffer.append(" ");
        buffer.append(executor.executor_id()).append("\n");
    };

    traverseExecutorTree(root_executor, [&](const tipb::Executor & executor) {
        if (executor.has_join())
        {
            for (const auto & child : executor.join().children())
                buffer.append(toTreeString(child, level));
            return false;
        }
        else
        {
            append_str(executor);
            ++level;
            return true;
        }
    });

    return buffer.toString();
}

String & trim(String & str)
{
    if (str.empty())
    {
        return str;
    }

    str.erase(0, str.find_first_not_of(' '));
    str.erase(str.find_last_not_of(' ') + 1);
    return str;
}
class MockExecutorTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        initializeDAGContext();
        DAGRequestBuilder::executor_index = 0;
    }

public:
    MockExecutorTest()
        : context(TiFlashTestEnv::getContext())
    {}

    virtual void initializeDAGContext()
    {
        dag_context_ptr = std::make_unique<DAGContext>(1024);
        context.setDAGContext(dag_context_ptr.get());
    }

    DAGContext & getDAGContext()
    {
        assert(dag_context_ptr != nullptr);
        return *dag_context_ptr;
    }

protected:
    Context context;
    std::unique_ptr<DAGContext> dag_context_ptr;
};

} // namespace tests
} // namespace DB