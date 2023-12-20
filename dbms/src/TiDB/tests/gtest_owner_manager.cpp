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
#include <Poco/Environment.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TiDB/Etcd/Client.h>
#include <TiDB/OwnerInfo.h>
#include <TiDB/OwnerManager.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>
#include <pingcap/Config.h>
#include <pingcap/pd/IClient.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <ext/scope_guard.h>
#include <future>
#include <magic_enum.hpp>
#include <memory>
#include <mutex>
#include <thread>

namespace DB
{
namespace Etcd
{
extern String getPrefix(String key);
} // namespace Etcd
namespace FailPoints
{
extern const char force_owner_mgr_state[];
extern const char force_owner_mgr_campaign_failed[];
extern const char force_fail_to_create_etcd_session[];
} // namespace FailPoints
} // namespace DB

namespace DB::tests
{

TEST(EtcdClientTest, ScanRangeEnd)
{
    const String inf("\x00", 1);
    EXPECT_EQ(DB::Etcd::getPrefix("\x01"), "\x02");
    EXPECT_EQ(DB::Etcd::getPrefix("aa"), "ab");
    EXPECT_EQ(DB::Etcd::getPrefix("a\xff"), "b");
    EXPECT_EQ(DB::Etcd::getPrefix("\xff\xff"), inf);
}

class OwnerManagerTest : public ::testing::Test
{
public:
    OwnerManagerTest()
        : log(Logger::get())
    {}

protected:
    static Etcd::LeaseID getLeaderLease(EtcdOwnerManager * o) { return o->leader.lease(); }

    static void changeState(EtcdOwnerManager * o, EtcdOwnerManager::State s) { o->tryChangeState(s); }

    Int64 test_ttl{10};
    Int64 test_pause{test_ttl * 6};

    LoggerPtr log;
};

TEST_F(OwnerManagerTest, SessionKeepaliveAfterCancelled)
{
    auto etcd_endpoint = Poco::Environment::get("ETCD_ENDPOINT", "");
    if (etcd_endpoint.empty())
    {
        const auto * t = ::testing::UnitTest::GetInstance()->current_test_info();
        LOG_INFO(
            log,
            "{}.{} is skipped because env ETCD_ENDPOINT not set. "
            "Run it with an etcd cluster using `ETCD_ENDPOINT=127.0.0.1:2379 ./dbms/gtests_dbms ...`",
            t->test_case_name(),
            t->name());
        return;
    }

    auto ctx = TiFlashTestEnv::getContext();
    pingcap::ClusterConfig config;
    pingcap::pd::ClientPtr pd_client = std::make_shared<pingcap::pd::Client>(Strings{etcd_endpoint}, config);
    auto etcd_client = DB::Etcd::Client::create(pd_client, config);

    auto keep_alive_ctx = std::make_unique<grpc::ClientContext>();
    auto session = etcd_client->createSession(keep_alive_ctx.get(), /*leader_ttl*/ 60);

    ASSERT_TRUE(session->keepAliveOne());

    keep_alive_ctx->TryCancel(); // mock that PD is down

    ASSERT_FALSE(session->keepAliveOne());
    ASSERT_FALSE(session->keepAliveOne()); // background task wake again, should return false instead of crashes
}

TEST_F(OwnerManagerTest, BeOwner)
try
{
    auto etcd_endpoint = Poco::Environment::get("ETCD_ENDPOINT", "");
    if (etcd_endpoint.empty())
    {
        const auto * t = ::testing::UnitTest::GetInstance()->current_test_info();
        LOG_INFO(
            log,
            "{}.{} is skipped because env ETCD_ENDPOINT not set. "
            "Run it with an etcd cluster using `ETCD_ENDPOINT=127.0.0.1:2379 ./dbms/gtests_dbms ...`",
            t->test_case_name(),
            t->name());
        return;
    }

    using namespace std::chrono_literals;

    auto ctx = TiFlashTestEnv::getContext();
    pingcap::ClusterConfig config;
    pingcap::pd::ClientPtr pd_client = std::make_shared<pingcap::pd::Client>(Strings{etcd_endpoint}, config);
    auto etcd_client = DB::Etcd::Client::create(pd_client, config);
    const String id = "owner_0";
    auto owner0 = OwnerManager::createS3GCOwner(*ctx, id, etcd_client, test_ttl);
    while (owner0->getOwnerID().status != OwnerType::NoLeader)
    {
        LOG_INFO(log, "wait for previous test end");
        std::this_thread::sleep_for(1s);
    }
    auto owner_info = owner0->getOwnerID();
    EXPECT_EQ(owner_info.status, OwnerType::NoLeader) << magic_enum::enum_name(owner_info.status);

    std::mutex mtx;
    std::condition_variable cv;
    bool become_owner = false;

    owner0->setBeOwnerHook([&] {
        {
            std::unique_lock lk(mtx);
            become_owner = true;
        }
        cv.notify_one();
    });

    owner0->campaignOwner();
    {
        std::unique_lock lk(mtx);
        cv.wait(lk, [&] { return become_owner; });
    }

    ASSERT_TRUE(owner0->isOwner());
    {
        auto owner_id = owner0->getOwnerID();
        EXPECT_EQ(owner_id.status, OwnerType::IsOwner);
        EXPECT_EQ(owner_id.owner_id, id);
    }

    LOG_INFO(log, "test wait for n*ttl");
    std::this_thread::sleep_for(std::chrono::seconds(test_pause));
    ASSERT_TRUE(owner0->isOwner());
    {
        auto owner_id = owner0->getOwnerID();
        EXPECT_EQ(owner_id.status, OwnerType::IsOwner);
        EXPECT_EQ(owner_id.owner_id, id);
    }
    LOG_INFO(log, "test wait for n*ttl passed");

    const String new_id = "owner_1";
    auto owner1 = OwnerManager::createS3GCOwner(*ctx, new_id, etcd_client);
    owner_info = owner1->getOwnerID();
    EXPECT_EQ(owner_info.status, OwnerType::NotOwner) << magic_enum::enum_name(owner_info.status);
    EXPECT_EQ(owner_info.owner_id, id);
    owner1->campaignOwner();

    {
        std::this_thread::sleep_for(5s);

        // owner is not transferred
        EXPECT_FALSE(owner1->isOwner());
        owner_info = owner1->getOwnerID();
        EXPECT_EQ(owner_info.status, OwnerType::NotOwner) << magic_enum::enum_name(owner_info.status);
        EXPECT_EQ(owner_info.owner_id, id);

        bool was_owner = owner1->resignOwner();
        EXPECT_FALSE(was_owner);
    }

    {
        bool was_owner = owner0->resignOwner();
        EXPECT_TRUE(was_owner);
    }

    {
        owner0->cancel();
        ASSERT_FALSE(owner0->isOwner());
    }

    {
        std::this_thread::sleep_for(5s);
        // owner is transferred to owner1
        EXPECT_TRUE(owner1->isOwner());
        owner_info = owner1->getOwnerID();
        EXPECT_EQ(owner_info.status, OwnerType::IsOwner) << magic_enum::enum_name(owner_info.status);
        EXPECT_EQ(owner_info.owner_id, new_id);
    }

    {
        owner1->cancel();
        ASSERT_FALSE(owner0->isOwner());
        ASSERT_FALSE(owner1->isOwner());
    }

    {
        owner_info = owner0->getOwnerID();
        EXPECT_EQ(owner_info.status, OwnerType::NoLeader) << magic_enum::enum_name(owner_info.status);
        EXPECT_EQ(owner_info.owner_id, "");
    }
}
CATCH

TEST_F(OwnerManagerTest, EtcdLeaseInvalid)
try
{
    auto etcd_endpoint = Poco::Environment::get("ETCD_ENDPOINT", "");
    if (etcd_endpoint.empty())
    {
        const auto * t = ::testing::UnitTest::GetInstance()->current_test_info();
        LOG_INFO(
            log,
            "{}.{} is skipped because env ETCD_ENDPOINT not set. "
            "Run it with an etcd cluster using `ETCD_ENDPOINT=127.0.0.1:2379 ./dbms/gtests_dbms ...`",
            t->test_case_name(),
            t->name());
        return;
    }
    using namespace std::chrono_literals;

    auto ctx = TiFlashTestEnv::getContext();
    pingcap::ClusterConfig config;
    pingcap::pd::ClientPtr pd_client = std::make_shared<pingcap::pd::Client>(Strings{etcd_endpoint}, config);
    auto etcd_client = DB::Etcd::Client::create(pd_client, config);
    const String id = "owner_0";
    auto owner0
        = std::static_pointer_cast<EtcdOwnerManager>(OwnerManager::createS3GCOwner(*ctx, id, etcd_client, test_ttl));
    auto owner_info = owner0->getOwnerID();
    EXPECT_EQ(owner_info.status, OwnerType::NoLeader) << magic_enum::enum_name(owner_info.status);

    std::mutex mtx;
    std::condition_variable cv;
    bool become_owner = false;

    owner0->setBeOwnerHook([&] {
        {
            std::unique_lock lk(mtx);
            become_owner = true;
        }
        cv.notify_one();
    });

    owner0->campaignOwner();
    {
        std::unique_lock lk(mtx);
        cv.wait(lk, [&] { return become_owner; });
    }

    ASSERT_TRUE(owner0->isOwner());
    {
        auto owner_id = owner0->getOwnerID();
        EXPECT_EQ(owner_id.status, OwnerType::IsOwner);
        EXPECT_EQ(owner_id.owner_id, id);
    }
    auto lease_0 = getLeaderLease(owner0.get());

    LOG_INFO(log, "test wait for n*ttl");
    FailPointHelper::enableFailPoint(FailPoints::force_owner_mgr_state, EtcdOwnerManager::State::CancelByLeaseInvalid);
    SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_owner_mgr_state); });
    changeState(owner0.get(), EtcdOwnerManager::State::CancelByLeaseInvalid);
    std::this_thread::sleep_for(std::chrono::seconds(test_pause));
    ASSERT_TRUE(owner0->isOwner());
    {
        auto owner_id = owner0->getOwnerID();
        EXPECT_EQ(owner_id.status, OwnerType::IsOwner);
        EXPECT_EQ(owner_id.owner_id, id);
    }
    auto lease_1 = getLeaderLease(owner0.get());
    EXPECT_NE(lease_0, lease_1) << "should use new etcd lease for elec";

    LOG_INFO(log, "test wait for n*ttl passed");
}
CATCH

TEST_F(OwnerManagerTest, KeyDeleted)
try
{
    auto etcd_endpoint = Poco::Environment::get("ETCD_ENDPOINT", "");
    if (etcd_endpoint.empty())
    {
        const auto * t = ::testing::UnitTest::GetInstance()->current_test_info();
        LOG_INFO(
            log,
            "{}.{} is skipped because env ETCD_ENDPOINT not set. "
            "Run it with an etcd cluster using `ETCD_ENDPOINT=127.0.0.1:2379 ./dbms/gtests_dbms ...`",
            t->test_case_name(),
            t->name());
        return;
    }

    using namespace std::chrono_literals;

    auto ctx = TiFlashTestEnv::getContext();
    pingcap::ClusterConfig config;
    pingcap::pd::ClientPtr pd_client = std::make_shared<pingcap::pd::Client>(Strings{etcd_endpoint}, config);
    auto etcd_client = DB::Etcd::Client::create(pd_client, config);
    const String id = "owner_0";
    auto owner0
        = std::static_pointer_cast<EtcdOwnerManager>(OwnerManager::createS3GCOwner(*ctx, id, etcd_client, test_ttl));
    auto owner_info = owner0->getOwnerID();
    EXPECT_EQ(owner_info.status, OwnerType::NoLeader) << magic_enum::enum_name(owner_info.status);

    std::mutex mtx;
    std::condition_variable cv;
    bool become_owner = false;

    owner0->setBeOwnerHook([&] {
        {
            std::unique_lock lk(mtx);
            become_owner = true;
        }
        cv.notify_one();
    });

    owner0->campaignOwner();
    {
        std::unique_lock lk(mtx);
        cv.wait(lk, [&] { return become_owner; });
    }

    ASSERT_TRUE(owner0->isOwner());
    {
        auto owner_id = owner0->getOwnerID();
        EXPECT_EQ(owner_id.status, OwnerType::IsOwner);
        EXPECT_EQ(owner_id.owner_id, id);
    }
    auto lease_0 = getLeaderLease(owner0.get());

    LOG_INFO(log, "test wait for n*ttl");
    FailPointHelper::enableFailPoint(FailPoints::force_owner_mgr_state, EtcdOwnerManager::State::CancelByKeyDeleted);
    SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_owner_mgr_state); });
    changeState(owner0.get(), EtcdOwnerManager::State::CancelByKeyDeleted);
    std::this_thread::sleep_for(std::chrono::seconds(test_pause));
    ASSERT_TRUE(owner0->isOwner());
    {
        auto owner_id = owner0->getOwnerID();
        EXPECT_EQ(owner_id.status, OwnerType::IsOwner);
        EXPECT_EQ(owner_id.owner_id, id);
    }
    auto lease_1 = getLeaderLease(owner0.get());
    EXPECT_EQ(lease_0, lease_1) << "should use same etcd lease for elec";

    LOG_INFO(log, "test wait for n*ttl passed");
}
CATCH

TEST_F(OwnerManagerTest, KeyDeletedWithSessionInvalid)
try
{
    auto etcd_endpoint = Poco::Environment::get("ETCD_ENDPOINT", "");
    if (etcd_endpoint.empty())
    {
        const auto * t = ::testing::UnitTest::GetInstance()->current_test_info();
        LOG_INFO(
            log,
            "{}.{} is skipped because env ETCD_ENDPOINT not set. "
            "Run it with an etcd cluster using `ETCD_ENDPOINT=127.0.0.1:2379 ./dbms/gtests_dbms ...`",
            t->test_case_name(),
            t->name());
        return;
    }

    using namespace std::chrono_literals;

    auto ctx = TiFlashTestEnv::getContext();
    pingcap::ClusterConfig config;
    pingcap::pd::ClientPtr pd_client = std::make_shared<pingcap::pd::Client>(Strings{etcd_endpoint}, config);
    auto etcd_client = DB::Etcd::Client::create(pd_client, config);
    const String id = "owner_0";
    auto owner0
        = std::static_pointer_cast<EtcdOwnerManager>(OwnerManager::createS3GCOwner(*ctx, id, etcd_client, test_ttl));
    auto owner_info = owner0->getOwnerID();
    EXPECT_EQ(owner_info.status, OwnerType::NoLeader) << magic_enum::enum_name(owner_info.status);

    std::mutex mtx;
    std::condition_variable cv;
    bool become_owner = false;

    owner0->setBeOwnerHook([&] {
        {
            std::unique_lock lk(mtx);
            become_owner = true;
        }
        cv.notify_one();
    });

    owner0->campaignOwner();
    {
        std::unique_lock lk(mtx);
        cv.wait(lk, [&] { return become_owner; });
    }

    ASSERT_TRUE(owner0->isOwner());
    {
        auto owner_id = owner0->getOwnerID();
        EXPECT_EQ(owner_id.status, OwnerType::IsOwner);
        EXPECT_EQ(owner_id.owner_id, id);
    }
    auto lease_0 = getLeaderLease(owner0.get());

    LOG_INFO(log, "test wait for n*ttl");
    FailPointHelper::enableFailPoint(FailPoints::force_owner_mgr_state, EtcdOwnerManager::State::CancelByKeyDeleted);
    FailPointHelper::enableFailPoint(FailPoints::force_owner_mgr_campaign_failed);
    SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_owner_mgr_state); });
    SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_owner_mgr_campaign_failed); });
    changeState(owner0.get(), EtcdOwnerManager::State::CancelByKeyDeleted);

    std::this_thread::sleep_for(std::chrono::seconds(test_pause));
    ASSERT_TRUE(owner0->isOwner());
    {
        auto owner_id = owner0->getOwnerID();
        EXPECT_EQ(owner_id.status, OwnerType::IsOwner);
        EXPECT_EQ(owner_id.owner_id, id);
    }
    auto lease_1 = getLeaderLease(owner0.get()); // should be a new lease
    EXPECT_NE(lease_0, lease_1) << "should use a new etcd lease for elec";

    LOG_INFO(log, "test wait for n*ttl passed");
}
CATCH

TEST_F(OwnerManagerTest, CreateEtcdSessionFail)
try
{
    auto etcd_endpoint = Poco::Environment::get("ETCD_ENDPOINT", "");
    if (etcd_endpoint.empty())
    {
        const auto * t = ::testing::UnitTest::GetInstance()->current_test_info();
        LOG_INFO(
            log,
            "{}.{} is skipped because env ETCD_ENDPOINT not set. "
            "Run it with an etcd cluster using `ETCD_ENDPOINT=127.0.0.1:2379 ./dbms/gtests_dbms ...`",
            t->test_case_name(),
            t->name());
        return;
    }

    using namespace std::chrono_literals;

    auto ctx = TiFlashTestEnv::getContext();
    pingcap::ClusterConfig config;
    pingcap::pd::ClientPtr pd_client = std::make_shared<pingcap::pd::Client>(Strings{etcd_endpoint}, config);
    auto etcd_client = DB::Etcd::Client::create(pd_client, config);
    const String id = "owner_0";
    auto owner0
        = std::static_pointer_cast<EtcdOwnerManager>(OwnerManager::createS3GCOwner(*ctx, id, etcd_client, test_ttl));
    auto owner_info = owner0->getOwnerID();
    EXPECT_EQ(owner_info.status, OwnerType::NoLeader) << magic_enum::enum_name(owner_info.status);

    FailPointHelper::enableFailPoint(FailPoints::force_fail_to_create_etcd_session);

    owner0->campaignOwner();

    std::this_thread::sleep_for(5s); // wait bg task wake
    owner0->cancel();
}
CATCH

TEST_F(OwnerManagerTest, CancelNonOwner)
try
{
    auto etcd_endpoint = Poco::Environment::get("ETCD_ENDPOINT", "");
    if (etcd_endpoint.empty())
    {
        const auto * t = ::testing::UnitTest::GetInstance()->current_test_info();
        LOG_INFO(
            log,
            "{}.{} is skipped because env ETCD_ENDPOINT not set. "
            "Run it with an etcd cluster using `ETCD_ENDPOINT=127.0.0.1:2379 ./dbms/gtests_dbms ...`",
            t->test_case_name(),
            t->name());
        return;
    }

    using namespace std::chrono_literals;

    auto ctx = TiFlashTestEnv::getContext();
    pingcap::ClusterConfig config;
    pingcap::pd::ClientPtr pd_client = std::make_shared<pingcap::pd::Client>(Strings{etcd_endpoint}, config);
    auto etcd_client = DB::Etcd::Client::create(pd_client, config);

    std::atomic<bool> owner0_elected = false;
    std::shared_ptr<EtcdOwnerManager> owner0;
    std::shared_ptr<EtcdOwnerManager> owner1;
    auto th_owner = std::async([&]() {
        const String id = "owner_0";
        owner0 = std::static_pointer_cast<EtcdOwnerManager>(
            OwnerManager::createS3GCOwner(*ctx, id, etcd_client, test_ttl));
        auto owner_info = owner0->getOwnerID();
        EXPECT_EQ(owner_info.status, OwnerType::NoLeader) << magic_enum::enum_name(owner_info.status);

        owner0->setBeOwnerHook([&] { owner0_elected = true; });
        owner0->campaignOwner();

        while (!owner0_elected)
            ;

        owner_info = owner0->getOwnerID();
        EXPECT_EQ(owner_info.status, OwnerType::IsOwner) << magic_enum::enum_name(owner_info.status);
    });

    auto th_non_owner = std::async([&] {
        const String id = "owner_1";

        LOG_INFO(log, "waiting for owner0 elected");
        while (!owner0_elected)
            ;

        owner1 = std::static_pointer_cast<EtcdOwnerManager>(
            OwnerManager::createS3GCOwner(*ctx, id, etcd_client, test_ttl));
        owner1->campaignOwner(); // this will block
    });

    auto th_cancel_non_owner = std::async([&] {
        while (!owner0_elected)
            ;

        LOG_INFO(log, "waiting for owner1 start campaign");
        std::this_thread::sleep_for(3s);
        LOG_INFO(log, "cancel owner1 start");
        owner1->cancel(); // cancel should finished th_non_owner
        LOG_INFO(log, "cancel owner1 done");
    });

    th_cancel_non_owner.wait();
    th_non_owner.wait();
    th_owner.wait();
}
CATCH

} // namespace DB::tests
