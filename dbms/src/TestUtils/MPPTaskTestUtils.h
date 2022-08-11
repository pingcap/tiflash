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

#include <Debug/MockComputeServerManager.h>
#include <Debug/MockStorage.h>
#include <Server/FlashGrpcServerHolder.h>
#include <TestUtils/ExecutorTestUtils.h>

#include <memory>

namespace DB::tests
{
class MPPTaskTestUtils : public ExecutorTest
{
public:
    static void SetUpTestCase()
    {
        ExecutorTest::SetUpTestCase();
        log_ptr = Logger::get("compute_test");
        auto size = std::thread::hardware_concurrency();
        GRPCCompletionQueuePool::global_instance = std::make_unique<GRPCCompletionQueuePool>(size);
        MockComputeServerManager::instance();
    }

    static void TearDownTestCase() // NOLINT(readability-identifier-naming))
    {
        MockComputeServerManager::instance().reset();
    }

protected:
    static LoggerPtr log_ptr;
};

LoggerPtr MPPTaskTestUtils::log_ptr = nullptr;

#define ASSERT_MPPTASK_EQUAL(tasks, expect_cols)                                                                                \
    do                                                                                                                          \
    {                                                                                                                           \
        TiFlashTestEnv::getGlobalContext().setMPPTest();                                                                        \
        MockComputeServerManager::instance().setMockStorage(context.mockStorage());                                             \
        ASSERT_COLUMNS_EQ_UR(executeMPPTasks(tasks, MockComputeServerManager::instance().getServerConfigMap()), expected_cols); \
    } while (0)

} // namespace DB::tests
