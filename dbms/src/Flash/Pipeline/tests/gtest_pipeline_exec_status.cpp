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

#include <Common/ThreadManager.h>
#include <Flash/Pipeline/PipelineExecStatus.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{
class PipelineExecStatusTestRunner : public ::testing::Test
{
};

TEST_F(PipelineExecStatusTestRunner, timeout)
try
{
    PipelineExecStatus status;
    status.addActivePipeline();
    std::chrono::milliseconds timeout(10);
    status.waitFor(timeout);
    auto err_msg = status.getErrMsg();
    ASSERT_EQ(err_msg, PipelineExecStatus::timeout_err_msg);
}
CATCH

TEST_F(PipelineExecStatusTestRunner, run)
try
{
    PipelineExecStatus status;
    status.addActivePipeline();
    auto thread_manager = newThreadManager();
    thread_manager->schedule(false, "run", [&status]() mutable { status.completePipeline(); });
    status.wait();
    auto err_msg = status.getErrMsg();
    ASSERT_TRUE(err_msg.empty()) << err_msg;
    thread_manager->wait();
}
CATCH

TEST_F(PipelineExecStatusTestRunner, to_err)
try
{
    auto test = [](std::string && err_msg) {
        auto expect_err_msg = err_msg.empty() ? PipelineExecStatus::empty_err_msg : err_msg;
        PipelineExecStatus status;
        status.addActivePipeline();
        auto thread_manager = newThreadManager();
        thread_manager->schedule(false, "err", [&status, &err_msg]() mutable {
            status.toError(std::move(err_msg));
            status.completePipeline();
        });
        status.wait();
        status.toError("unexpect exception");
        auto actual_err_msg = status.getErrMsg();
        ASSERT_TRUE(!actual_err_msg.empty());
        ASSERT_EQ(actual_err_msg, expect_err_msg);
        thread_manager->wait();
    };
    test("throw exception");
    test("");
}
CATCH

} // namespace DB::tests
