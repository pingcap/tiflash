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
#include <pingcap/Exception.h>
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

TEST(LocalAdmissionControllerTest, KeyspaceWatchUpdatePromotesLegacyFallbackToExactGroup)
{
    constexpr KeyspaceID keyspace_id = 23;
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
        group->set_priority(ResourceGroup::UserLowPriority);
        auto * settings = group->mutable_r_u_settings()->mutable_r_u()->mutable_settings();
        settings->set_fill_rate(1024);
        settings->set_burst_limit(1024);
        return resp;
    });

    LocalAdmissionController lac(&cluster, nullptr, false, false);
    ASSERT_NO_THROW(lac.warmupResourceGroupInfoCache(keyspace_id, "default"));

    ASSERT_EQ(lac.getCachedResourceGroupForTest(keyspace_id, "default"), nullptr);
    ASSERT_NE(lac.getCachedResourceGroupForTest(NullspaceID, "default"), nullptr);

    mvccpb::KeyValue kv;
    kv.set_key("resource_group/keyspace/settings/23/default");
    resource_manager::ResourceGroup updated_group;
    updated_group.set_name("default");
    updated_group.set_mode(resource_manager::GroupMode::RUMode);
    updated_group.set_priority(ResourceGroup::UserHighPriority);
    updated_group.mutable_keyspace_id()->set_value(keyspace_id);
    auto * settings = updated_group.mutable_r_u_settings()->mutable_r_u()->mutable_settings();
    settings->set_fill_rate(4096);
    settings->set_burst_limit(4096);
    kv.set_value(updated_group.SerializeAsString());

    std::string err_msg;
    ASSERT_TRUE(lac.handlePutEvent(LocalAdmissionController::GAC_KEYSPACE_RESOURCE_GROUP_ETCD_PATH, kv, err_msg))
        << err_msg;

    auto exact_group = lac.getCachedResourceGroupForTest(keyspace_id, "default");
    ASSERT_NE(exact_group, nullptr);
    EXPECT_TRUE(exact_group->enable_gac);
    EXPECT_EQ(exact_group->user_ru_per_sec, 4096);
    EXPECT_EQ(exact_group->group_pb.priority(), ResourceGroup::UserHighPriority);

    auto request_info = exact_group->buildRequestInfoIfNecessary(SteadyClock::now());
    ASSERT_TRUE(request_info.has_value());
    EXPECT_EQ(request_info->keyspace_id, keyspace_id);
}

TEST(LocalAdmissionControllerTest, KeyspaceLookupDoesNotFallbackForNonNotFoundError)
{
    constexpr KeyspaceID keyspace_id = 29;
    pingcap::kv::Cluster cluster;
    cluster.pd_client = std::make_shared<TestPDClient>([](const resource_manager::GetResourceGroupRequest & req) {
        resource_manager::GetResourceGroupResponse resp;
        if (req.has_keyspace_id())
        {
            resp.mutable_error()->set_message("keyspace metadata is unavailable");
            return resp;
        }

        auto * group = resp.mutable_group();
        group->set_name(req.resource_group_name());
        group->set_mode(resource_manager::GroupMode::RUMode);
        group->set_priority(ResourceGroup::UserMediumPriority);
        auto * settings = group->mutable_r_u_settings()->mutable_r_u()->mutable_settings();
        settings->set_fill_rate(1024);
        settings->set_burst_limit(1024);
        return resp;
    });

    LocalAdmissionController lac(&cluster, nullptr, false, false);

    EXPECT_THROW(lac.warmupResourceGroupInfoCache(keyspace_id, "default"), DB::Exception);
    EXPECT_EQ(lac.cachedResourceGroupCount(), 0);
}

TEST(LocalAdmissionControllerTest, ReservedDefaultGroupStillFallbacksWhenLegacyLookupThrows)
{
    constexpr KeyspaceID keyspace_id = 31;
    pingcap::kv::Cluster cluster;
    cluster.pd_client = std::make_shared<TestPDClient>([](const resource_manager::GetResourceGroupRequest & req) {
        if (!req.has_keyspace_id())
            throw pingcap::Exception("legacy lookup failed", pingcap::ErrorCodes::UnknownError);

        resource_manager::GetResourceGroupResponse resp;
        resp.mutable_error()->set_message("resource group default not found");
        return resp;
    });

    LocalAdmissionController lac(&cluster, nullptr, false, false);

    ASSERT_NO_THROW(lac.warmupResourceGroupInfoCache(keyspace_id, "default"));
    auto group = lac.getCachedResourceGroupForTest(NullspaceID, "default");
    ASSERT_NE(group, nullptr);
    EXPECT_FALSE(group->enable_gac);
    EXPECT_EQ(group->user_ru_per_sec, LocalAdmissionController::RESERVED_DEFAULT_RESOURCE_GROUP_RU_PER_SEC);
}

} // namespace DB::tests
