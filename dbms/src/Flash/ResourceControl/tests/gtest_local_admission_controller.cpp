// Copyright 2026 PingCAP, Inc.
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

#include <Flash/ResourceControl/LocalAdmissionController.h>
#include <Storages/KVStore/Types.h>
#include <gtest/gtest.h>
#include <pingcap/kv/Cluster.h>

namespace DB::tests
{
namespace
{
class TestPDClient final : public pingcap::pd::MockPDClient
{
public:
    explicit TestPDClient(
        std::function<resource_manager::GetResourceGroupResponse(const resource_manager::GetResourceGroupRequest &)>
            get_resource_group_)
        : get_resource_group(std::move(get_resource_group_))
    {}

    resource_manager::GetResourceGroupResponse getResourceGroup(
        const resource_manager::GetResourceGroupRequest & req) override
    {
        return get_resource_group(req);
    }

    resource_manager::TokenBucketsResponse acquireTokenBuckets(const resource_manager::TokenBucketsRequest &) override
    {
        return {};
    }

private:
    std::function<resource_manager::GetResourceGroupResponse(const resource_manager::GetResourceGroupRequest &)>
        get_resource_group;
};
} // namespace

TEST(LocalAdmissionControllerTest, ReservedDefaultGroupFallsBackToLocalCacheWhenPDDoesNotPersistIt)
{
    constexpr KeyspaceID keyspace_id = 7;
    pingcap::kv::Cluster cluster;
    cluster.pd_client = std::make_shared<TestPDClient>([](const resource_manager::GetResourceGroupRequest &) {
        resource_manager::GetResourceGroupResponse resp;
        resp.mutable_error()->set_message("resource group default not found");
        return resp;
    });

    LocalAdmissionController lac(&cluster, nullptr, false, false);

    ASSERT_NO_THROW(lac.warmupResourceGroupInfoCache(keyspace_id, "default"));
    ASSERT_EQ(lac.cachedResourceGroupCount(), 1);

    auto priority = lac.getPriority(keyspace_id, "default");
    ASSERT_TRUE(priority.has_value());

    auto group = lac.getCachedResourceGroupForTest(keyspace_id, "default");
    ASSERT_NE(group, nullptr);
    EXPECT_FALSE(group->enable_gac);
    EXPECT_EQ(group->user_ru_per_sec, LocalAdmissionController::RESERVED_DEFAULT_RESOURCE_GROUP_RU_PER_SEC);
    EXPECT_EQ(group->group_pb.priority(), ResourceGroup::UserMediumPriority);
    EXPECT_TRUE(group->group_pb.has_keyspace_id());
    EXPECT_EQ(group->group_pb.keyspace_id().value(), keyspace_id);
    EXPECT_FALSE(group->buildRequestInfoIfNecessary(SteadyClock::now()).has_value());
}

TEST(LocalAdmissionControllerTest, ReservedDefaultGroupUsesPDConfigWhenPresent)
{
    constexpr KeyspaceID keyspace_id = 9;
    pingcap::kv::Cluster cluster;
    cluster.pd_client = std::make_shared<TestPDClient>([](const resource_manager::GetResourceGroupRequest & req) {
        resource_manager::GetResourceGroupResponse resp;
        auto * group = resp.mutable_group();
        group->set_name(req.resource_group_name());
        group->set_mode(resource_manager::GroupMode::RUMode);
        group->set_priority(ResourceGroup::UserLowPriority);
        group->mutable_keyspace_id()->set_value(req.keyspace_id().value());
        auto * settings = group->mutable_r_u_settings()->mutable_r_u()->mutable_settings();
        settings->set_fill_rate(1024);
        settings->set_burst_limit(1024);
        return resp;
    });

    LocalAdmissionController lac(&cluster, nullptr, false, false);

    ASSERT_NO_THROW(lac.warmupResourceGroupInfoCache(keyspace_id, "default"));

    auto group = lac.getCachedResourceGroupForTest(keyspace_id, "default");
    ASSERT_NE(group, nullptr);
    EXPECT_TRUE(group->enable_gac);
    EXPECT_EQ(group->user_ru_per_sec, 1024);
    EXPECT_EQ(group->group_pb.priority(), ResourceGroup::UserLowPriority);

    auto priority = lac.getPriority(keyspace_id, "default");
    ASSERT_TRUE(priority.has_value());
}

TEST(LocalAdmissionControllerTest, KeyspaceLookupFallsBackToLegacyCachedGroup)
{
    constexpr KeyspaceID keyspace_id = 11;
    pingcap::kv::Cluster cluster;
    cluster.pd_client = std::make_shared<TestPDClient>([](const resource_manager::GetResourceGroupRequest & req) {
        resource_manager::GetResourceGroupResponse resp;
        if (req.has_keyspace_id())
        {
            resp.mutable_error()->set_message("resource group default not found");
            return resp;
        }

        auto * group = resp.mutable_group();
        group->set_name(req.resource_group_name());
        group->set_mode(resource_manager::GroupMode::RUMode);
        group->set_priority(ResourceGroup::UserHighPriority);
        auto * settings = group->mutable_r_u_settings()->mutable_r_u()->mutable_settings();
        settings->set_fill_rate(2048);
        settings->set_burst_limit(2048);
        return resp;
    });

    LocalAdmissionController lac(&cluster, nullptr, false, false);

    ASSERT_NO_THROW(lac.warmupResourceGroupInfoCache(keyspace_id, "default"));
    ASSERT_EQ(lac.cachedResourceGroupCount(), 1);
    ASSERT_EQ(lac.getCachedResourceGroupForTest(keyspace_id, "default"), nullptr);

    auto legacy_group = lac.getCachedResourceGroupForTest(NullspaceID, "default");
    ASSERT_NE(legacy_group, nullptr);
    EXPECT_TRUE(legacy_group->enable_gac);
    EXPECT_EQ(legacy_group->user_ru_per_sec, 2048);
    EXPECT_EQ(legacy_group->group_pb.priority(), ResourceGroup::UserHighPriority);

    auto priority = lac.getPriority(keyspace_id, "default");
    ASSERT_TRUE(priority.has_value());
    EXPECT_EQ(lac.cachedResourceGroupCount(), 1);
}
} // namespace DB::tests
