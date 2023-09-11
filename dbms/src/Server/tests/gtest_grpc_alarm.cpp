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

/// Suppress gcc warning: ‘*((void*)&<anonymous> +4)’ may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <cpptoml.h>
#if !__clang__
#pragma GCC diagnostic pop
#endif

#include <Common/Config/ConfigProcessor.h>
#include <Interpreters/Quota.h>
#include <Poco/Logger.h>
#include <Server/StorageConfigParser.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/KVStore/MultiRaft/RegionManager.h>
#include <Storages/KVStore/MultiRaft/RegionPersister.h>
#include <Storages/KVStore/Region.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/PathCapacityMetrics.h>
#include <TestUtils/ConfigTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include "grpcpp/alarm.h"
#include "grpcpp/impl/codegen/completion_queue.h"

namespace DB
{
namespace tests
{
class GRPCAlarmTest : public ::testing::Test
{
public:
    GRPCAlarmTest() {}

protected:
};


TEST_F(GRPCAlarmTest, MeetDeadline)
try
{
    grpc::CompletionQueue cq;
    void * data = reinterpret_cast<void *>(&cq);
    grpc::Alarm alarm;
    alarm.Set(&cq, Clock::now() + std::chrono::seconds(1), data);
    void * output_tag;
    bool ok;
    const grpc::CompletionQueue::NextStatus status
        = cq.AsyncNext(&output_tag, &ok, Clock::now() + std::chrono::seconds(10));
    ASSERT_TRUE(status == grpc::CompletionQueue::NextStatus::GOT_EVENT);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(output_tag == data);
}
CATCH

TEST_F(GRPCAlarmTest, Cancelled)
try
{
    grpc::CompletionQueue cq;
    void * data = reinterpret_cast<void *>(&cq);
    grpc::Alarm alarm;
    alarm.Set(&cq, Clock::now() + std::chrono::seconds(600), data);
    void * output_tag;
    bool ok;
    alarm.Cancel();
    const grpc::CompletionQueue::NextStatus status
        = cq.AsyncNext(&output_tag, &ok, Clock::now() + std::chrono::seconds(1));
    ASSERT_TRUE(status == grpc::CompletionQueue::NextStatus::GOT_EVENT);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(output_tag == data);
}
CATCH
} // namespace tests
} // namespace DB
