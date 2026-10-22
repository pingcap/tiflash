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

#include <Common/SyncPoint/Ctl.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Interpreters/Context.h>
#include <Server/RaftConfigParser.h>
#include <Storages/KVStore/TMTContext.h>
#include <TestUtils/ConfigTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <ext/scope_guard.h>
#include <future>

namespace DB
{
namespace tests
{
class TestMPPTaskManager : public testing::Test
{
public:
    TestMPPTaskManager()
    {
        global_context = Context::createGlobal(Context::ApplicationType::LOCAL);
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
#ifdef FIU_ENABLE
    void checkProbeProtectionBeforeDrain(UInt64 probe_timeout, UInt64 drain_timeout, bool finish_task)
    {
        using namespace std::chrono_literals;
        // The probe timeout belongs to the main config; the existing drain timeout belongs to users config.
        global_context->setConfig(loadConfigFromString(fmt::format(
            "[flash]\ngraceful_shutdown_probe_timeout = {}\ngraceful_wait_shutdown_timeout = 30\n",
            probe_timeout)));
        global_context->setUsersConfig(loadConfigFromString(fmt::format(
            "[flash]\ngraceful_shutdown_probe_timeout = {}\ngraceful_wait_shutdown_timeout = {}\n",
            probe_timeout == 0 ? 60 : 0,
            drain_timeout)));
        const auto manager = global_context->getTMTContext().getMPPTaskManager();
        const auto monitor = manager->getMPPTaskMonitor();
        auto before_drain = SyncPointCtl::enableInScope("mpp_task_monitor_before_drain");
        auto at_timeout = SyncPointCtl::enableInScope("mpp_task_monitor_drain_timeout");
        const auto start = std::chrono::steady_clock::now();
        ASSERT_TRUE(manager->checkAvailable());
        manager->setUnavailable();
        ASSERT_FALSE(manager->checkAvailable());
        auto shutdown = std::async(std::launch::async, [&] { monitor->waitAllMPPTasksFinish(global_context); });
        auto drain_started = std::async(std::launch::async, [&] {
            before_drain.waitAndPause();
            return std::chrono::steady_clock::now();
        });
        auto drain_timed_out = std::async(std::launch::async, [&] { at_timeout.waitAndPause(); });
        // Release every sync point before waiting for futures, including on a fatal assertion.
        SCOPE_EXIT({
            before_drain.disable();
            at_timeout.disable();
            if (shutdown.valid())
                shutdown.wait();
            if (drain_started.valid())
                drain_started.wait();
            if (drain_timed_out.valid())
                drain_timed_out.wait();
        });

        if (probe_timeout != 0)
        {
            EXPECT_EQ(drain_started.wait_for(500ms), std::future_status::timeout);
            EXPECT_EQ(shutdown.wait_for(0ms), std::future_status::timeout);
        }

        // Construct a real task after shutdown began, as for a dispatch following the earlier successful probe.
        auto context = createContextForTest();
        auto gather_id = MPPGatherId(1, MPPQueryId(1, 1, 1, 1, "", 1, ""));
        mpp::TaskMeta meta;
        fillTaskMeta(&meta, 1, gather_id);
        auto task = MPPTask::newTaskForTest(meta, context);
        const auto task_id = task->getId().toString();
        ASSERT_TRUE(monitor->isInMonitor(task_id));
        if (probe_timeout != 0)
        {
            EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(probe_timeout));
            EXPECT_EQ(shutdown.wait_for(0ms), std::future_status::timeout);
        }

        // An ignored custom value (the default is 60s) or reading users config must fail this bounded wait.
        ASSERT_EQ(drain_started.wait_for(4s), std::future_status::ready);
        const auto protection_elapsed = drain_started.get() - start;
        EXPECT_GE(protection_elapsed, std::chrono::seconds(probe_timeout));
        EXPECT_LT(protection_elapsed, std::chrono::seconds(probe_timeout) + 2s);
        EXPECT_EQ(shutdown.wait_for(0ms), std::future_status::timeout);
        const auto drain_start = std::chrono::steady_clock::now();
        before_drain.next();

        if (finish_task)
        {
            // Beyond the original two-second dispatch grace, a live task must still prevent normal completion.
            EXPECT_EQ(shutdown.wait_for(2300ms), std::future_status::timeout);
            EXPECT_TRUE(monitor->isInMonitor(task_id));
            task.reset();
            EXPECT_FALSE(monitor->isInMonitor(task_id));
            ASSERT_EQ(shutdown.wait_for(2s), std::future_status::ready);
            EXPECT_EQ(drain_timed_out.wait_for(0ms), std::future_status::timeout);
        }
        else
        {
            // A zero drain timeout must take the timeout branch with the task still monitored.
            ASSERT_EQ(drain_timed_out.wait_for(4s), std::future_status::ready);
            EXPECT_GE(std::chrono::steady_clock::now() - drain_start, 2s);
            EXPECT_TRUE(monitor->isInMonitor(task_id));
            EXPECT_EQ(shutdown.wait_for(0ms), std::future_status::timeout);
            at_timeout.next();
            ASSERT_EQ(shutdown.wait_for(1s), std::future_status::ready);
            EXPECT_TRUE(monitor->isInMonitor(task_id));
        }
        shutdown.get();
    }
#endif

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

TEST_F(TestMPPTaskManager, testDuplicatePendingAsyncTunnel)
try
{
    auto context = createContextForTest();

    mpp::EstablishMPPConnectionRequest establish_req;
    auto gather_id = MPPGatherId(1, MPPQueryId(1, 1, 1, 1, "", 1, ""));
    auto * receiver_meta = establish_req.mutable_receiver_meta();
    fillTaskMeta(receiver_meta, 2, gather_id);
    auto * sender_meta = establish_req.mutable_sender_meta();
    fillTaskMeta(sender_meta, 1, gather_id);
    auto mpp_task_manager = context->getTMTContext().getMPPTaskManager();

    EstablishCallData first_call_data;
    auto find_tunnel_result = mpp_task_manager->findAsyncTunnel(&establish_req, &first_call_data, nullptr, *context);
    ASSERT_TRUE(find_tunnel_result.first == nullptr && find_tunnel_result.second.empty());
    ASSERT_TRUE(first_call_data.isWaitingTunnelState());

    EstablishCallData duplicate_call_data;
    find_tunnel_result = mpp_task_manager->findAsyncTunnel(&establish_req, &duplicate_call_data, nullptr, *context);
    ASSERT_TRUE(find_tunnel_result.first == nullptr);
    ASSERT_TRUE(!find_tunnel_result.second.empty());
    ASSERT_FALSE(duplicate_call_data.isWaitingTunnelState());
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

#ifdef FIU_ENABLE
TEST_F(TestMPPTaskManager, MainConfigProbeProtectionPrecedesTaskDrainTimeout)
try
{
    checkProbeProtectionBeforeDrain(2, 0, false);
}
CATCH

TEST_F(TestMPPTaskManager, MainConfigCanDisableProbeProtection)
try
{
    checkProbeProtectionBeforeDrain(0, 0, false);
}
CATCH

TEST_F(TestMPPTaskManager, ProtectedLateTaskFinishesDuringNormalDrain)
try
{
    checkProbeProtectionBeforeDrain(2, 10, true);
}
CATCH
#endif

} // namespace tests
} // namespace DB
