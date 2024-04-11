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

#include <Common/FailPoint.h>
#include <Common/setThreadName.h>
#include <Debug/MockKVStore/MockRaftStoreProxy.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/chrono.h>

#include <ext/scope_guard.h>

#pragma GCC diagnostic push
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
// include to suppress warnings on NO_THREAD_SAFETY_ANALYSIS. clang can't work without this include, don't know why
#include <grpcpp/security/credentials.h>
#pragma GCC diagnostic pop

namespace DB
{
namespace FailPoints
{
} // namespace FailPoints

namespace tests
{
class ReadIndexTest : public ::testing::Test
{
public:
    ReadIndexTest() = default;

    void SetUp() override {}

    static size_t computeCntUseHistoryTasks(ReadIndexWorkerManager & manager);
    static void testBasic();
    static void testBatch();
    static void testNormal();
    static void testError();
};

void ReadIndexTest::testError()
{
    // test error
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    KVStore kvs = KVStore{ctx};
    MockRaftStoreProxy proxy_instance;
    TiFlashRaftProxyHelper proxy_helper;
    {
        proxy_helper = MockRaftStoreProxy::setRaftStoreProxyFFIHelper(RaftStoreProxyPtr{&proxy_instance});
        proxy_instance.init(10);
    }
    auto manager = ReadIndexWorkerManager::newReadIndexWorkerManager(proxy_helper, kvs, 5, [&]() {
        return std::chrono::milliseconds(10);
    });
    {
        std::vector<kvrpcpb::ReadIndexRequest> reqs;
        std::vector<kvrpcpb::ReadIndexResponse> resps;
        std::list<ReadIndexFuturePtr> futures;
        {
            // Test running_tasks
            reqs = {make_read_index_reqs(2, 10), make_read_index_reqs(2, 12), make_read_index_reqs(2, 13)};
            for (const auto & req : reqs)
            {
                auto future = manager->genReadIndexFuture(req);
                auto resp = future->poll();
                ASSERT_FALSE(resp.has_value());
                futures.push_back(future);
            }
            manager->runOneRoundAll();
            for (auto & future : futures)
            {
                auto resp = future->poll();
                ASSERT_FALSE(resp.has_value());
            }
            ASSERT_EQ(proxy_instance.mock_read_index.read_index_tasks.size(), 1);
            ASSERT_EQ(proxy_instance.mock_read_index.read_index_tasks.front()->req.start_ts(), 13);
        }
        {
            // Force response region error `data_is_not_ready`
            proxy_instance.mock_read_index.read_index_tasks.front()->update(false, true);

            for (auto & future : futures)
            {
                auto resp = future->poll();
                ASSERT_FALSE(resp.has_value());
            }
            manager->runOneRoundAll();
            for (auto & future : futures)
            {
                auto resp = future->poll();
                ASSERT_TRUE(resp.has_value());
                ASSERT(resp->region_error().has_data_is_not_ready());
            }
            {
                // Poll another time
                auto resp = futures.front()->poll();
                ASSERT_TRUE(resp.has_value());
                // Still old value
                ASSERT(resp->region_error().has_data_is_not_ready());
            }
            futures.clear();
            proxy_instance.mock_read_index.runOneRound();
            ASSERT_FALSE(manager->getWorkerByRegion(2).data_map.getDataNode(2)->history_success_tasks);
        }

        {
            reqs = {make_read_index_reqs(2, 10), make_read_index_reqs(2, 12), make_read_index_reqs(2, 13)};
            for (const auto & req : reqs)
            {
                auto future = manager->genReadIndexFuture(req);
                auto resp = future->poll();
                ASSERT(!resp);
                futures.push_back(future);
            }
            manager->runOneRoundAll();
            for (auto & future : futures)
            {
                auto resp = future->poll();
                ASSERT(!resp);
            }
            ASSERT_EQ(proxy_instance.mock_read_index.read_index_tasks.size(), 1);

            // force response to have lock
            proxy_instance.mock_read_index.read_index_tasks.front()->update(true, false);

            proxy_instance.mock_read_index.runOneRound();
            for (auto & future : futures)
            {
                auto resp = future->poll();
                ASSERT(!resp);
            }
            manager->runOneRoundAll();
            for (auto & future : futures)
            {
                auto resp = future->poll();
                ASSERT(resp);
                resps.emplace_back(std::move(*resp));
            }

            // only the one with same ts will be set lock.
            // others will got region error and let upper layer retry.
            ASSERT(resps[0].region_error().has_epoch_not_match());
            ASSERT(resps[1].region_error().has_epoch_not_match());
            ASSERT(resps[2].has_locked());

            // `history_success_tasks` no update.
            ASSERT_FALSE(manager->getWorkerByRegion(2).data_map.getDataNode(2)->history_success_tasks);
        }
        {
            // test drop region when there are related tasks.
            std::vector<kvrpcpb::ReadIndexRequest> reqs;
            std::vector<kvrpcpb::ReadIndexResponse> resps;
            std::list<ReadIndexFuturePtr> futures;
            reqs = {
                make_read_index_reqs(2, 12),
                make_read_index_reqs(2, 13),
            };
            futures.clear();
            for (const auto & req : reqs)
            {
                auto future = manager->genReadIndexFuture(req);
                auto resp = future->poll();
                ASSERT(!resp);
                futures.push_back(future);
            }
            manager->runOneRoundAll();
            proxy_instance.mock_read_index.runOneRound();
            auto future = manager->genReadIndexFuture(make_read_index_reqs(2, 15));

            // drop region 2
            manager->getWorkerByRegion(2).removeRegion(2);

            for (auto & future : futures)
            {
                auto resp = future->poll();
                ASSERT(resp);
                resps.emplace_back(std::move(*resp));
            }
            ASSERT(resps[0].region_error().has_region_not_found());
            ASSERT(resps[1].region_error().has_region_not_found());
            {
                auto resp = future->poll();
                ASSERT(resp);
                ASSERT(resp->region_error().has_region_not_found());
            }
            ASSERT_FALSE(manager->getWorkerByRegion(2).data_map.getDataNode(2)->history_success_tasks);
        }
    }
    ASSERT(GCMonitor::instance().checkClean());
    ASSERT(!GCMonitor::instance().empty());
}

struct Helper
{
    size_t & counter;
    void operator()(std::unordered_map<RegionID, ReadIndexDataNodePtr> & d) NO_THREAD_SAFETY_ANALYSIS
    {
        for (auto & x : d)
        {
            auto _ = x.second->genLockGuard();
            counter += x.second->cnt_use_history_tasks;
        }
    }
};

size_t ReadIndexTest::computeCntUseHistoryTasks(ReadIndexWorkerManager & manager)
{
    size_t cnt_use_history_tasks = 0;
    for (auto & worker : manager.workers)
    {
        worker->data_map.invoke(Helper{.counter = cnt_use_history_tasks});
    }
    return cnt_use_history_tasks;
}

void ReadIndexTest::testBasic()
{
    {
        // codec
        kvrpcpb::ReadIndexResponse resp;
        {
            resp.mutable_locked()->set_key("123");
            resp.mutable_region_error();
            resp.set_read_index(12345);
        }
        std::string str = resp.SerializeAsString();

        kvrpcpb::ReadIndexResponse resp2;
        SetPBMsByBytes(MsgPBType::ReadIndexResponse, &resp2, BaseBuffView{str.data(), str.size()});
        std::string str2 = resp2.SerializeAsString();

        ASSERT_EQ(str2, str);
    }
    {
        // lock wrap
        struct TestMutexLockWrap : MutexLockWrap
        {
            void test() NO_THREAD_SAFETY_ANALYSIS
            {
                {
                    auto lock = genLockGuard();
                    auto ulock = tryToLock();
                    ASSERT_FALSE(ulock.operator bool());
                }
                {
                    auto ulock = genUniqueLock();
                    ASSERT(ulock.operator bool());
                }
            }
        } test;
        test.test();
    }
    {
        // future only can be updated once
        ReadIndexFuture f;
        kvrpcpb::ReadIndexResponse resp;
        resp.set_read_index(888);
        f.update(std::move(resp));
        ASSERT_EQ(f.resp.read_index(), 888);
        resp.set_read_index(999);
        f.update(std::move(resp));
        ASSERT_EQ(f.resp.read_index(), 888);
    }
}

void ReadIndexTest::testNormal()
{
    // test normal
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    KVStore kvs = KVStore{ctx};
    MockRaftStoreProxy proxy_instance;
    TiFlashRaftProxyHelper proxy_helper;
    {
        proxy_helper = MockRaftStoreProxy::setRaftStoreProxyFFIHelper(RaftStoreProxyPtr{&proxy_instance});
        proxy_instance.init(10);
    }
    auto manager = ReadIndexWorkerManager::newReadIndexWorkerManager(
        proxy_helper,
        kvs,
        5,
        [&]() { return std::chrono::milliseconds(10); },
        3);

    for (size_t id = 0; id < manager->workers.size(); ++id)
    {
        ASSERT_EQ(id, manager->workers[id]->getID());
    }

    // start async loop in other threads
    manager->asyncRun();

    std::atomic_bool over{false};

    // start mock proxy in other thread
    auto proxy_runner = std::thread([&]() {
        setThreadName("proxy-runner");
        proxy_instance.testRunReadIndex(over);
    });

    {
        std::vector<kvrpcpb::ReadIndexRequest> reqs;
        {
            // One request of start_ts = 10 for every region.
            reqs.reserve(proxy_instance.size());
            for (size_t i = 0; i < proxy_instance.size(); ++i)
            {
                reqs.emplace_back(make_read_index_reqs(i, 10));
            }
        }
        {
            auto resps = manager->batchReadIndex(reqs);
            ASSERT_EQ(resps.size(), reqs.size());
            for (size_t i = 0; i < reqs.size(); ++i)
            {
                auto & req = reqs[i];
                auto && [resp, id] = resps[i];
                ASSERT_EQ(req.context().region_id(), id);
                ASSERT_EQ(resp.read_index(), 5);
            }
        }
        {
            // update region commit index
            for (auto & r : proxy_instance.regions)
            {
                r.second->updateCommitIndex(668);
            }
        }

        size_t expect_cnt_use_history_tasks = proxy_instance.size();
        {
            // smaller ts, use history success record
            for (auto & r : reqs)
            {
                r.set_start_ts(9);
            }
            auto resps = manager->batchReadIndex(reqs);
            ASSERT_EQ(resps.size(), reqs.size());
            for (size_t i = 0; i < reqs.size(); ++i)
            {
                auto & req = reqs[i];
                auto && [resp, id] = resps[i];
                ASSERT_EQ(req.context().region_id(), id);
                ASSERT_EQ(resp.read_index(), 5);
            }
            ASSERT_EQ(computeCntUseHistoryTasks(*manager), expect_cnt_use_history_tasks);
        }
        {
            // bigger ts, fetch latest commit index
            reqs = {make_read_index_reqs(0, 11)};
            auto resps = manager->batchReadIndex(reqs);
            ASSERT_EQ(resps[0].first.read_index(), 668);
            ASSERT_EQ(computeCntUseHistoryTasks(*manager), expect_cnt_use_history_tasks);
        }
        {
            for (auto & r : proxy_instance.regions)
            {
                r.second->updateCommitIndex(669);
            }
        }
        {
            reqs = {make_read_index_reqs(0, 0)};
            auto resps = manager->batchReadIndex(reqs);
            ASSERT_EQ(resps[0].first.read_index(), 669); // tso 0 will not use history record
            ASSERT_EQ(computeCntUseHistoryTasks(*manager), expect_cnt_use_history_tasks);
        }
        {
            // smaller ts, use history success record.
            expect_cnt_use_history_tasks++;
            reqs = {make_read_index_reqs(0, 9)};
            auto resps = manager->batchReadIndex(reqs);
            ASSERT_EQ(resps[0].first.read_index(), 668); // history record has been updated
            ASSERT_EQ(computeCntUseHistoryTasks(*manager), expect_cnt_use_history_tasks);
        }
        {
            // Set region id to let mock proxy drop all related tasks.
            proxy_instance.unsafeInvokeForTest(
                [](MockRaftStoreProxy & proxy) { proxy.mock_read_index.region_id_to_drop.emplace(1); });
            std::vector<kvrpcpb::ReadIndexRequest> reqs;

            reqs = {make_read_index_reqs(5, 12), make_read_index_reqs(1, 12), make_read_index_reqs(2, 12)};
            auto start = std::chrono::steady_clock::now();
            auto resps = manager->batchReadIndex(reqs, 20);
            auto time_cost = std::chrono::steady_clock::now() - start;
            ASSERT_GE(time_cost, std::chrono::milliseconds{20}); // meet timeout
            ASSERT_EQ(resps[0].first.read_index(), 669);
            ASSERT_EQ(resps[1].first.region_error().has_region_not_found(), true); // timeout to region error not found
            ASSERT_EQ(resps[2].first.read_index(), 669);
            ASSERT(!GCMonitor::instance().checkClean());
            {
                // test timeout 0ms
                proxy_instance.unsafeInvokeForTest(
                    [](MockRaftStoreProxy & proxy) { proxy.mock_read_index.region_id_to_drop.emplace(9); });
                auto resps = manager->batchReadIndex({make_read_index_reqs(9, 12)}, 0);
                ASSERT_EQ(
                    resps[0].first.region_error().has_region_not_found(),
                    true); // timeout to region error not found
            }

            // disable drop region task
            proxy_instance.unsafeInvokeForTest(
                [](MockRaftStoreProxy & proxy) { proxy.mock_read_index.region_id_to_drop.clear(); });

            ReadIndexWorker::setMaxReadIndexTaskTimeout(
                std::chrono::milliseconds{10}); // set max task timeout in worker
            resps = manager->batchReadIndex(
                reqs,
                500); // the old task is still in worker, poll from mock proxy failed, check timeout and set region error `server_is_busy
            ASSERT_EQ(
                resps[1].first.region_error().has_server_is_busy(),
                true); // meet region error `server_is_busy` for task timeout

            ReadIndexWorker::setMaxReadIndexTaskTimeout(
                std::chrono::milliseconds{8 * 1000}); // set max task timeout in worker to previous.
            resps = manager->batchReadIndex(reqs, 500);
            ASSERT_EQ(resps[1].first.read_index(), 669);
        }
        {
            // test `batchReadIndex_v2`.
            // batchReadIndex_v2 is almost like v1.
            // v1 runs all jobs in proxy. v2 split into multi steps and runs them in tiflash side.
            reqs = {make_read_index_reqs(5, 12), make_read_index_reqs(1, 12), make_read_index_reqs(2, 12)};
            auto resps = proxy_helper.batchReadIndex_v2(reqs, 200);
            for (size_t i = 0; i < reqs.size(); ++i)
            {
                ASSERT_EQ(resps[i].first.read_index(), 669);
            }

            // set region id to let mock proxy drop all related tasks.
            proxy_instance.unsafeInvokeForTest(
                [](MockRaftStoreProxy & proxy) { proxy.mock_read_index.region_id_to_drop.emplace(1); });

            resps = proxy_helper.batchReadIndex_v2(reqs, 50);

            ASSERT_EQ(resps[0].first.read_index(), 669);
            ASSERT_EQ(resps[1].first.region_error().has_region_not_found(), true); // timeout to region error not found
            ASSERT_EQ(resps[2].first.read_index(), 669);

            proxy_instance.unsafeInvokeForTest(
                [](MockRaftStoreProxy & proxy) { proxy.mock_read_index.region_id_to_drop.clear(); });
        }
        {
            // test region not exists
            reqs = {make_read_index_reqs(8192, 5)};
            auto resps = proxy_helper.batchReadIndex_v2(reqs, 20);
            ASSERT(resps[0].first.has_region_error());
        }
    }
    over = true;
    proxy_instance.mock_read_index.wakeNotifier();
    proxy_runner.join();
    manager.reset();
    ASSERT(GCMonitor::instance().checkClean());
    ASSERT(!GCMonitor::instance().empty());
}
void ReadIndexTest::testBatch()
{
    // test batch
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    KVStore kvs = KVStore{ctx};
    MockRaftStoreProxy proxy_instance;
    TiFlashRaftProxyHelper proxy_helper;
    {
        proxy_helper = MockRaftStoreProxy::setRaftStoreProxyFFIHelper(RaftStoreProxyPtr{&proxy_instance});
        proxy_instance.init(10);
    }
    auto manager = ReadIndexWorkerManager::newReadIndexWorkerManager(proxy_helper, kvs, 5, [&]() {
        return std::chrono::milliseconds(10);
    });
    // DO NOT run manager and mock proxy in other threads.
    {
        {
            // run with empty `waiting_tasks`
            manager->getWorkerByRegion(0).data_map.getDataNode(0)->runOneRound(
                proxy_helper,
                manager->getWorkerByRegion(0).read_index_notify_ctrl);
        }
        {
            // test upsert region
            manager->workers[0]->data_map.upsertDataNode(0);
            auto ori_s = manager->workers[0]->data_map.region_map.size();
            manager->workers[0]->data_map.upsertDataNode(0);
            ASSERT_EQ(manager->workers[0]->data_map.region_map.size(), ori_s);
        }

        std::vector<kvrpcpb::ReadIndexRequest> reqs;
        std::deque<ReadIndexFuturePtr> futures;
        reqs
            = {make_read_index_reqs(0, 1),
               make_read_index_reqs(0, 2),
               make_read_index_reqs(0, 3),
               make_read_index_reqs(1, 2),
               make_read_index_reqs(1, 3)};
        // no waiting task
        ASSERT_EQ(manager->getWorkerByRegion(0).data_map.getDataNode(0)->waiting_tasks.size(), 0);
        // poll failed
        for (const auto & req : reqs)
        {
            auto future = manager->genReadIndexFuture(req);
            ASSERT_FALSE(future->poll());
            futures.emplace_back(future);
        }
        // worker 0 has 3 waiting tasks.
        ASSERT_EQ(manager->getWorkerByRegion(0).data_map.getDataNode(0)->waiting_tasks.size(), 3);
        // run worker, clean waiting task.
        manager->runOneRoundAll();
        ASSERT_EQ(manager->getWorkerByRegion(0).data_map.getDataNode(0)->waiting_tasks.size(), 0);

        ASSERT_EQ(
            1,
            manager->getWorkerByRegion(0)
                .data_map.getDataNode(0)
                ->running_tasks.size()); // worker 0 has 1 running task.
        ASSERT_EQ(
            1,
            manager->getWorkerByRegion(1)
                .data_map.getDataNode(1)
                ->running_tasks.size()); // worker 1 has 1 running task.
        {
            for (auto & r : proxy_instance.regions)
            {
                r.second->updateCommitIndex(667);
            }
        }
        {
            auto req = make_read_index_reqs(0, 5);
            auto future = manager->genReadIndexFuture(req);
            ASSERT_FALSE(future->poll());
            futures.emplace_back(future);
        }
        {
            // continuously run same worker, time duration must bigger than setting.
            ASSERT_FALSE(manager->getWorkerByRegion(0).lastRunTimeout(
                std::chrono::milliseconds(500))); // failed to check last run timeout
        }
        manager->runOneRoundAll();

        // bigger ts and make new running task
        ASSERT_EQ(2, manager->getWorkerByRegion(0).data_map.getDataNode(0)->running_tasks.size());

        // mock proxy update all read index requests.
        proxy_instance.mock_read_index.runOneRound();
        manager->runOneRoundAll();
        std::vector<kvrpcpb::ReadIndexResponse> resps;
        for (auto & future : futures)
        {
            auto resp = future->poll();
            ASSERT(resp);
            resps.emplace_back(std::move(*resp));
        }
        ASSERT_EQ(resps.size(), 6);
        for (auto & resp : resps)
            ASSERT_EQ(resp.read_index(), 667);
    }
    {
        auto req = make_read_index_reqs(8192, 5); // not exist

        auto future = manager->genReadIndexFuture(req);

        manager->runOneRoundAll(); // failed to execute `fn_make_read_index_task`.

        ASSERT_EQ(
            0,
            manager->getWorkerByRegion(8192).data_map.getDataNode(8192)->running_tasks.size()); // no running task

        auto resp = future->poll();
        ASSERT(resp);
        ASSERT(resp->has_region_error());
        proxy_instance.mock_read_index.runOneRound();
    }
    {
        // test history
        for (auto & r : proxy_instance.regions)
        {
            r.second->updateCommitIndex(670);
        }
        std::vector<kvrpcpb::ReadIndexRequest> reqs;
        std::list<ReadIndexFuturePtr> futures;
        std::vector<kvrpcpb::ReadIndexResponse> resps;

        reqs = {make_read_index_reqs(0, 5), make_read_index_reqs(0, 6)};
        for (const auto & req : reqs)
        {
            auto future = manager->genReadIndexFuture(req);
            futures.push_back(future);
        }
        manager->runOneRoundAll();

        ASSERT_EQ(1, manager->getWorkerByRegion(0).data_map.getDataNode(0)->running_tasks.size());

        {
            auto future = manager->genReadIndexFuture(make_read_index_reqs(0, 10));
            futures.push_back(future);
        }
        manager->runOneRoundAll();
        {
            auto & t = proxy_instance.mock_read_index.read_index_tasks.back();
            ASSERT_EQ(t->req.start_ts(), 10);
            t->update(); // only response ts `10`
        }

        ASSERT_EQ(2, manager->getWorkerByRegion(0).data_map.getDataNode(0)->running_tasks.size());

        manager->runOneRoundAll(); // history trigger all callback tasks.

        ASSERT_EQ(0, manager->getWorkerByRegion(0).data_map.getDataNode(0)->running_tasks.size());

        for (auto & future : futures)
        {
            auto resp = future->poll();
            ASSERT(resp);
            resps.emplace_back(std::move(*resp));
        }
        {
            ASSERT_EQ(resps[0].read_index(), 670);
            ASSERT_EQ(resps[1].read_index(), 670);
            ASSERT_EQ(resps[2].read_index(), 670);
        }
        proxy_instance.mock_read_index.runOneRound();
        manager->runOneRoundAll();

        futures.clear();
        reqs = {
            make_read_index_reqs(0, 12),
        };
        for (const auto & req : reqs)
        {
            auto future = manager->genReadIndexFuture(req);
            futures.push_back(future);
        }
        manager->runOneRoundAll();
        proxy_instance.mock_read_index.runOneRound();
        manager->runOneRoundAll();
        for (auto & future : futures)
        {
            auto resp = future->poll();
            ASSERT_EQ(resp->read_index(), 670);
        }

        ASSERT_EQ(
            manager->getWorkerByRegion(0).data_map.getDataNode(0)->history_success_tasks->second.read_index(),
            670);
    }
    {
        MockStressTestCfg::enable = true;
        auto region_id = 1 + MockStressTestCfg::RegionIdPrefix * (1 + 1);
        auto f = manager->genReadIndexFuture(make_read_index_reqs(region_id, 15));
        manager->runOneRoundAll();
        proxy_instance.regions[1]->updateCommitIndex(677);
        proxy_instance.mock_read_index.runOneRound();
        manager->runOneRoundAll();
        auto resp = f->poll();
        ASSERT_EQ(resp->read_index(), 677);
        MockStressTestCfg::enable = false;
    }
    ASSERT(GCMonitor::instance().checkClean());
    ASSERT(!GCMonitor::instance().empty());
}


TEST_F(ReadIndexTest, workers)
try
{
    testBasic();
    testNormal();
    testBatch();
    testError();
}
CATCH

} // namespace tests
} // namespace DB
