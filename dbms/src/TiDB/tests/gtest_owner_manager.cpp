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

#include <Poco/Environment.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TiDB/Etcd/Client.h>
#include <TiDB/OwnerInfo.h>
#include <TiDB/OwnerManager.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>
#include <pingcap/Config.h>
#include <pingcap/pd/IClient.h>
#include <unistd.h>

#include <condition_variable>
#include <magic_enum.hpp>
#include <mutex>

namespace DB::tests
{

TEST(OwnerManagerTest, BeOwner)
{
    auto etcd_endpoint = Poco::Environment::get("ETCD_ENDPOINT");
    if (etcd_endpoint.empty())
    {
        ::testing::UnitTest::GetInstance()->current_test_info();
        LOG_INFO(Logger::get(), "{}.{} is skipped because env ETCD_ENDPOINT not set");
        return;
    }

    auto ctx = TiFlashTestEnv::getContext();
    pingcap::ClusterConfig config;
    pingcap::pd::ClientPtr pd_client = std::make_shared<pingcap::pd::Client>(Strings{etcd_endpoint}, config);
    auto etcd_client = DB::Etcd::Client::create(pd_client, config);
    const String id = "OwnerManagerTest";
    auto owner = OwnerManager::createS3GCOwner(ctx, id, etcd_client);
    auto owner_id = owner->getOwnerID();
    EXPECT_EQ(owner_id.status, OwnerType::NoLeader) << magic_enum::enum_name(owner_id.status);

    std::mutex mtx;
    std::condition_variable cv;
    bool become_owner = false;

    owner->campaignOwner();
    owner->setBeOwnerHook([&] {
        {
            std::unique_lock lk(mtx);
            become_owner = true;
        }
        cv.notify_one();
    });

    {
        std::unique_lock lk(mtx);
        cv.wait(lk, [&] { return become_owner; });
    }

    ASSERT_TRUE(owner->isOwner());
    {
        auto owner_id = owner->getOwnerID();
        EXPECT_EQ(owner_id.status, OwnerType::IsOwner);
        EXPECT_EQ(owner_id.owner_id, id);
    }

    {
        bool was_owner = owner->resignOwner();
        EXPECT_TRUE(was_owner);
    }

    {
        owner->cancel();
        ASSERT_FALSE(owner->isOwner());
    }

    sleep(10);
}

} // namespace DB::tests
