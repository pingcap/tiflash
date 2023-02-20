#include <TestUtils/TiFlashTestEnv.h>
#include <TiDB/Etcd/Client.h>
#include <TiDB/OwnerInfo.h>
#include <TiDB/OwnerManager.h>
#include <gtest/gtest.h>
#include <pingcap/Config.h>
#include <pingcap/pd/IClient.h>
#include <unistd.h>

#include <condition_variable>
#include <mutex>

namespace DB::tests
{

TEST(OwnerManagerTest, BeOwner)
{
    auto ctx = TiFlashTestEnv::getContext();
    pingcap::ClusterConfig config;
    pingcap::pd::ClientPtr pd_client = std::make_shared<pingcap::pd::Client>(Strings{"172.16.5.85:6520"}, config);
    auto etcd_client = DB::Etcd::Client::create(pd_client, config);
    const String id = "OwnerManagerTest";
    auto owner = OwnerManager::createS3GCOwner(ctx, id, etcd_client);
    EXPECT_EQ(owner->getOwnerID().status, OwnerType::NoLeader);

    std::mutex mtx;
    std::condition_variable cv;
    bool become_owner = false;

    owner->campaignOwner();
    owner->setBeOwnerHook([&] {
        std::unique_lock lk(mtx);
        become_owner = true;
        cv.notify_one();
    });

    std::unique_lock lk(mtx);
    cv.wait(lk, [&] { return become_owner; });

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
        owner->campaignCancel();
        ASSERT_FALSE(owner->isOwner());
    }

    sleep(10);
}

} // namespace DB::tests
