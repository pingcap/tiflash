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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Interpreters/Context.h>
#include <Storages/KVStore/TMTContext.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <gtest/gtest.h>

#include "Server/RaftConfigParser.h"

namespace DB
{
namespace tests
{
class TestMPPTaskManager : public testing::Test
{
public:
    TestMPPTaskManager()
    {
        global_context = Context::createGlobal();
        global_context->mockConfigLoaded();
        TiFlashRaftConfig raft_config;
        global_context->createTMTContext(raft_config, pingcap::ClusterConfig());
    }
    static void fillTaskMeta(mpp::TaskMeta * task_meta, Int64 task_id, MPPGatherId & gather_id)
    {
        task_meta->set_gather_id(gather_id.gather_id);
        task_meta->set_task_id(task_id);
        task_meta->set_local_query_id(gather_id.query_id.local_query_id);
        task_meta->set_query_ts(gather_id.query_id.query_ts);
        task_meta->set_server_id(gather_id.query_id.server_id);
        task_meta->set_start_ts(gather_id.query_id.start_ts);
        task_meta->set_resource_group_name(gather_id.query_id.resource_group_name);
    }
    ContextPtr createContextForTest()
    {
        auto context = std::make_shared<Context>(*global_context);
        String query_id = "query_id_for_test";
        context->setCurrentQueryId(query_id);
        return context;
    }

protected:
    std::unique_ptr<Context> global_context;
};

TEST_F(TestMPPTaskManager, testUnregisterMPPTask)
try
{
    auto context = createContextForTest();

    /// find async tunnel create alarm if task is not visible
    EstablishCallData establish_call_data;
    mpp::EstablishMPPConnectionRequest establish_req;
    auto gather_id = MPPGatherId(1, MPPQueryId(1, 1, 1, 1, "", 1, ""));
    auto * receiver_meta = establish_req.mutable_receiver_meta();
    fillTaskMeta(receiver_meta, 2, gather_id);
    auto * sender_meta = establish_req.mutable_sender_meta();
    fillTaskMeta(sender_meta, 1, gather_id);
    auto mpp_task_manager = context->getTMTContext().getMPPTaskManager();
    auto find_tunnel_result
        = mpp_task_manager->findAsyncTunnel(&establish_req, &establish_call_data, nullptr, *context);
    ASSERT_TRUE(find_tunnel_result.first == nullptr && find_tunnel_result.second.empty());

    /// `findAsyncTunnel` will create GatherTaskSet
    auto gather_task_set = mpp_task_manager->getGatherTaskSet(MPPGatherId(1, MPPQueryId(1, 1, 1, 1, "", 1, "")));
    ASSERT_TRUE(gather_task_set.first != nullptr);
    ASSERT_TRUE(!gather_task_set.first->hasMPPTask());
    ASSERT_TRUE(gather_task_set.first->hasAlarm());
    ASSERT_TRUE(
        mpp_task_manager->getCurrentMinTSOQueryId(gather_id.query_id.resource_group_name) == MPPTaskId::Max_Query_Id);

    /// schedule task will put query_id to scheduler
    auto mpp_task_1 = MPPTask::newTaskForTest(*sender_meta, context);
    auto mpp_task_2 = MPPTask::newTaskForTest(*receiver_meta, context);
    mpp_task_manager->registerTask(mpp_task_1.get());
    mpp_task_manager->tryToScheduleTask(mpp_task_1->getScheduleEntry());
    mpp_task_manager->registerTask(mpp_task_2.get());
    mpp_task_manager->tryToScheduleTask(mpp_task_2->getScheduleEntry());
    ASSERT_TRUE(gather_task_set.first->hasMPPTask());
    ASSERT_TRUE(gather_task_set.first->hasAlarm());
    ASSERT_TRUE(
        mpp_task_manager->getCurrentMinTSOQueryId(gather_id.query_id.resource_group_name) == gather_id.query_id);

    /// unregister task should clean the related alarms
    mpp_task_manager->unregisterTask(mpp_task_1->getId(), "");
    gather_task_set = mpp_task_manager->getGatherTaskSet(MPPGatherId(1, MPPQueryId(1, 1, 1, 1, "", 1, "")));
    ASSERT_TRUE(gather_task_set.first->hasMPPTask());
    ASSERT_TRUE(!gather_task_set.first->hasAlarm());

    /// `findAsyncTunnel` should return error if target sender task is unregistered
    EstablishCallData establish_call_data_1;
    mpp::EstablishMPPConnectionRequest establish_req_1;
    receiver_meta = establish_req_1.mutable_receiver_meta();
    fillTaskMeta(receiver_meta, 3, gather_id);
    sender_meta = establish_req_1.mutable_sender_meta();
    fillTaskMeta(sender_meta, 1, gather_id);
    find_tunnel_result = mpp_task_manager->findAsyncTunnel(&establish_req, &establish_call_data, nullptr, *context);
    ASSERT_TRUE(find_tunnel_result.first == nullptr && !find_tunnel_result.second.empty());

    /// if all task is unregistered, min tso should be updated
    mpp_task_manager->unregisterTask(mpp_task_2->getId(), "");
    gather_task_set = mpp_task_manager->getGatherTaskSet(MPPGatherId(1, MPPQueryId(1, 1, 1, 1, "", 1, "")));
    ASSERT_TRUE(gather_task_set.first == nullptr);
    ASSERT_TRUE(
        mpp_task_manager->getCurrentMinTSOQueryId(gather_id.query_id.resource_group_name) == MPPTaskId::Max_Query_Id);
}
CATCH

TEST_F(TestMPPTaskManager, testDuplicateMPPTaskId)
try
{
    MPPTaskPtr original_task;
    auto context = createContextForTest();
    auto mpp_task_manager = context->getTMTContext().getMPPTaskManager();
    {
        mpp::EstablishMPPConnectionRequest establish_req;
        auto gather_id = MPPGatherId(1, MPPQueryId(1, 1, 1, 1, "", 1, ""));
        auto * sender_meta = establish_req.mutable_sender_meta();
        fillTaskMeta(sender_meta, 1, gather_id);
        original_task = MPPTask::newTaskForTest(*sender_meta, context);
        auto result = mpp_task_manager->registerTask(original_task.get());
        ASSERT_TRUE(result.first);
        auto second_task = MPPTask::newTaskForTest(*sender_meta, context);
        result = mpp_task_manager->registerTask(second_task.get());
        ASSERT_FALSE(result.first);
        second_task->handleError(result.second);
    }
    ASSERT_TRUE(mpp_task_manager->isTaskExists(original_task->getId()));
    ASSERT_TRUE(mpp_task_manager->getMPPTaskMonitor()->isInMonitor(original_task->getId().toString()));
}
CATCH

} // namespace tests
} // namespace DB
