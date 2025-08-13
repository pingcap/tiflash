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

#include <Common/Exception.h>
#include <Common/ThreadManager.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Executor/ResultQueue.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/FailPointUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{
class PipelineExecutorContextTestRunner : public ExecutorTest
{
public:
    ~PipelineExecutorContextTestRunner() override = default;
};

TEST_F(PipelineExecutorContextTestRunner, suffixExceptionTest)
try
{
    context.addMockTable(
        "simple_test",
        "t1",
        {{"a", TiDB::TP::TypeString}, {"b", TiDB::TP::TypeString}},
        {toNullableVec<String>("a", {"1"}), toNullableVec<String>("b", {"3"})});

    auto req = context.scan("simple_test", "t1").aggregation({Count(col("a"))}, {col("a")}).build(context);

    const auto failpoints = std::vector{
        "random_pipeline_model_execute_suffix_failpoint-1",
        "random_pipeline_model_execute_prefix_failpoint-1"};

    for (const auto & fp : failpoints)
    {
        auto config_str = fmt::format("[flash]\nrandom_fail_points = \"{}\"", fp);
        initRandomFailPoint(config_str);
        enablePipeline(true);
        // Expect this case throw failpoint instead of stuck.
        ASSERT_THROW(executeStreams(req, 1), Exception);
        disableRandomFailPoint(config_str);
    }
}
CATCH

TEST_F(PipelineExecutorContextTestRunner, waitTimeout)
try
{
    PipelineExecutorContext context;
    try
    {
        context.incActiveRefCount();
        std::chrono::milliseconds timeout(10);
        context.waitFor(timeout);
        GTEST_FAIL();
    }
    catch (DB::Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), PipelineExecutorContext::timeout_err_msg);
        auto err_msg = context.getExceptionMsg();
        ASSERT_EQ(err_msg, PipelineExecutorContext::timeout_err_msg);
    }
}
CATCH

TEST_F(PipelineExecutorContextTestRunner, run)
try
{
    PipelineExecutorContext context;
    context.incActiveRefCount();
    auto thread_manager = newThreadManager();
    thread_manager->schedule(false, "run", [&context]() mutable { context.decActiveRefCount(); });
    context.wait();
    auto exception_ptr = context.getExceptionPtr();
    auto err_msg = context.getExceptionMsg();
    ASSERT_TRUE(!exception_ptr) << err_msg;
    thread_manager->wait();
}
CATCH

TEST_F(PipelineExecutorContextTestRunner, toErr)
try
{
    auto test = [](std::string && err_msg) {
        auto expect_err_msg = err_msg;
        PipelineExecutorContext context;
        context.incActiveRefCount();
        auto thread_manager = newThreadManager();
        thread_manager->schedule(false, "err", [&context, &err_msg]() mutable {
            context.onErrorOccurred(err_msg);
            context.decActiveRefCount();
        });
        context.wait();
        context.onErrorOccurred("unexpect exception");
        ASSERT_TRUE(context.getExceptionPtr());
        auto actual_err_msg = context.getExceptionMsg();
        ASSERT_EQ(actual_err_msg, expect_err_msg);
        thread_manager->wait();
    };
    test("throw exception");
    test("");
}
CATCH

TEST_F(PipelineExecutorContextTestRunner, consumeThrowError)
try
{
    // case1
    {
        PipelineExecutorContext context;
        auto ret_queue = context.toConsumeMode(1);
        ret_queue->push(Block{});
        ResultHandler handler{[](const Block &) {
            throw Exception("for test");
        }};
        context.consume(handler);
        ASSERT_TRUE(context.getExceptionPtr());
        auto actual_err_msg = context.getExceptionMsg();
        ASSERT_EQ(actual_err_msg, "for test");
    }

    // case2
    {
        PipelineExecutorContext context;

        context.incActiveRefCount();
        auto thread_manager = newThreadManager();
        thread_manager->schedule(false, "exec", [&context]() mutable {
            while (!context.isCancelled())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            context.decActiveRefCount();
        });

        auto ret_queue = context.toConsumeMode(1);
        ret_queue->push(Block{});
        ResultHandler handler{[](const Block &) {
            throw Exception("for test");
        }};
        context.consume(handler);
        ASSERT_TRUE(context.getExceptionPtr());
        auto actual_err_msg = context.getExceptionMsg();
        ASSERT_EQ(actual_err_msg, "for test");

        thread_manager->wait();
    }
}
CATCH

} // namespace DB::tests
