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

#include <Common/Exception.h>
#include <Common/ThreadManager.h>
#include <Flash/Executor/PipelineExecutorStatus.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{
class PipelineExecutorStatusTestRunner : public ::testing::Test
{
};

TEST_F(PipelineExecutorStatusTestRunner, waitTimeout)
try
{
    PipelineExecutorStatus status;
    try
    {
        status.onEventSchedule();
        std::chrono::milliseconds timeout(10);
        status.waitFor(timeout);
        GTEST_FAIL();
    }
    catch (DB::Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), PipelineExecutorStatus::timeout_err_msg);
        auto err_msg = status.getExceptionMsg();
        ASSERT_EQ(err_msg, PipelineExecutorStatus::timeout_err_msg);
    }
}
CATCH

TEST_F(PipelineExecutorStatusTestRunner, popTimeout)
try
{
    PipelineExecutorStatus status;
    status.toConsumeMode(1);
    try
    {
        status.onEventSchedule();
        std::chrono::milliseconds timeout(10);
        ResultHandler result_handler{[](const Block &) {
        }};
        status.consumeFor(result_handler, timeout);
        GTEST_FAIL();
    }
    catch (DB::Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), PipelineExecutorStatus::timeout_err_msg);
        auto err_msg = status.getExceptionMsg();
        ASSERT_EQ(err_msg, PipelineExecutorStatus::timeout_err_msg);
    }
}
CATCH

TEST_F(PipelineExecutorStatusTestRunner, run)
try
{
    PipelineExecutorStatus status;
    status.onEventSchedule();
    auto thread_manager = newThreadManager();
    thread_manager->schedule(false, "run", [&status]() mutable { status.onEventFinish(); });
    status.wait();
    auto exception_ptr = status.getExceptionPtr();
    auto err_msg = status.getExceptionMsg();
    ASSERT_TRUE(!exception_ptr) << err_msg;
    thread_manager->wait();
}
CATCH

TEST_F(PipelineExecutorStatusTestRunner, toErr)
try
{
    auto test = [](std::string && err_msg) {
        auto expect_err_msg = err_msg;
        PipelineExecutorStatus status;
        status.onEventSchedule();
        auto thread_manager = newThreadManager();
        thread_manager->schedule(false, "err", [&status, &err_msg]() mutable {
            status.onErrorOccurred(err_msg);
            status.onEventFinish();
        });
        status.wait();
        status.onErrorOccurred("unexpect exception");
        ASSERT_TRUE(status.getExceptionPtr());
        auto actual_err_msg = status.getExceptionMsg();
        ASSERT_EQ(actual_err_msg, expect_err_msg);
        thread_manager->wait();
    };
    test("throw exception");
    test("");
}
CATCH

TEST_F(PipelineExecutorStatusTestRunner, consumeThrowError)
try
{
    PipelineExecutorStatus status;
    auto ret_queue = status.toConsumeMode(1);
    ret_queue->push(Block{});
    ResultHandler handler{[](const Block &) {
        throw Exception("for test");
    }};
    status.consume(handler);
    ASSERT_TRUE(status.getExceptionPtr());
    auto actual_err_msg = status.getExceptionMsg();
    ASSERT_EQ(actual_err_msg, "for test");
}
CATCH

} // namespace DB::tests
