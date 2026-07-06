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
#include <gtest/gtest.h>
#include <pingcap/kv/Cluster.h>

#include <functional>
#include <vector>

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

resource_manager::GetResourceGroupResponse buildResourceGroupResp(
    const resource_manager::GetResourceGroupRequest & req,
    uint64_t fill_rate,
    bool with_keyspace_id)
{
    resource_manager::GetResourceGroupResponse resp;
    auto * group = resp.mutable_group();
    group->set_name(req.resource_group_name());
    group->set_mode(resource_manager::GroupMode::RUMode);
    group->set_priority(ResourceGroup::UserLowPriority);
    if (with_keyspace_id)
        group->mutable_keyspace_id()->set_value(req.keyspace_id().value());
    auto * settings = group->mutable_r_u_settings()->mutable_r_u()->mutable_settings();
    settings->set_fill_rate(fill_rate);
    settings->set_burst_limit(fill_rate);
    return resp;
}
} // namespace

TEST(LocalAdmissionControllerTest, LegacyBackendCachesSharedGroupAndOmitsKeyspaceAfterDetection)
{
    constexpr KeyspaceID keyspace_id = 23;
    std::vector<bool> request_has_keyspace;
    pingcap::kv::Cluster cluster;
    cluster.pd_client = std::make_shared<TestPDClient>(
        [&request_has_keyspace](const resource_manager::GetResourceGroupRequest & req) {
            request_has_keyspace.push_back(req.has_keyspace_id());
            const auto fill_rate = req.resource_group_name() == "default" ? 1024 : 2048;
            return buildResourceGroupResp(req, fill_rate, false);
        });

    LocalAdmissionController lac(&cluster, nullptr, true, false);

    ASSERT_NO_THROW(lac.warmupResourceGroupInfoCache(keyspace_id, "default"));
    ASSERT_NO_THROW(lac.warmupResourceGroupInfoCache(keyspace_id + 1, "analytics"));

    ASSERT_EQ(request_has_keyspace.size(), 2);
    EXPECT_TRUE(request_has_keyspace[0]);
    EXPECT_FALSE(request_has_keyspace[1]);

    EXPECT_EQ(lac.getCachedResourceGroupForTest(keyspace_id, "default"), nullptr);
    auto default_group = lac.getCachedResourceGroupForTest(NullspaceID, "default");
    ASSERT_NE(default_group, nullptr);
    EXPECT_TRUE(lac.getPriority(keyspace_id, "default").has_value());

    auto request_info = default_group->buildRequestInfoIfNecessary(SteadyClock::now());
    ASSERT_TRUE(request_info.has_value());
    EXPECT_EQ(request_info->keyspace_id, NullspaceID);

    auto analytics_group = lac.getCachedResourceGroupForTest(NullspaceID, "analytics");
    ASSERT_NE(analytics_group, nullptr);
    EXPECT_EQ(analytics_group->user_ru_per_sec, 2048);
}

TEST(LocalAdmissionControllerTest, KeyspaceScopedBackendCachesExactGroupAndKeepsKeyspaceRequests)
{
    constexpr KeyspaceID keyspace_id = 29;
    std::vector<bool> request_has_keyspace;
    pingcap::kv::Cluster cluster;
    cluster.pd_client = std::make_shared<TestPDClient>(
        [&request_has_keyspace](const resource_manager::GetResourceGroupRequest & req) {
            request_has_keyspace.push_back(req.has_keyspace_id());
            return buildResourceGroupResp(req, 4096, true);
        });

    LocalAdmissionController lac(&cluster, nullptr, true, false);

    ASSERT_NO_THROW(lac.warmupResourceGroupInfoCache(keyspace_id, "default"));
    ASSERT_NO_THROW(lac.warmupResourceGroupInfoCache(keyspace_id + 1, "analytics"));

    ASSERT_EQ(request_has_keyspace.size(), 2);
    EXPECT_TRUE(request_has_keyspace[0]);
    EXPECT_TRUE(request_has_keyspace[1]);

    EXPECT_EQ(lac.getCachedResourceGroupForTest(NullspaceID, "default"), nullptr);
    auto default_group = lac.getCachedResourceGroupForTest(keyspace_id, "default");
    ASSERT_NE(default_group, nullptr);
    auto request_info = default_group->buildRequestInfoIfNecessary(SteadyClock::now());
    ASSERT_TRUE(request_info.has_value());
    EXPECT_EQ(request_info->keyspace_id, keyspace_id);

    auto analytics_group = lac.getCachedResourceGroupForTest(keyspace_id + 1, "analytics");
    ASSERT_NE(analytics_group, nullptr);
    EXPECT_EQ(analytics_group->group_pb.keyspace_id().value(), keyspace_id + 1);
}

TEST(LocalAdmissionControllerTest, WarmupErrorPropagatesWithoutCompatibilityFallback)
{
    constexpr KeyspaceID keyspace_id = 31;
    size_t request_count = 0;
    pingcap::kv::Cluster cluster;
    cluster.pd_client
        = std::make_shared<TestPDClient>([&request_count](const resource_manager::GetResourceGroupRequest &) {
              ++request_count;
              resource_manager::GetResourceGroupResponse resp;
              resp.mutable_error()->set_message("resource group default not found");
              return resp;
          });

    LocalAdmissionController lac(&cluster, nullptr, true, false);

    EXPECT_THROW(lac.warmupResourceGroupInfoCache(keyspace_id, "default"), DB::Exception);
    EXPECT_EQ(request_count, 1);
    EXPECT_EQ(lac.cachedResourceGroupCount(), 0);
}

} // namespace DB::tests
