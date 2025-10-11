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
#include <Common/Logger.h>
#include <Debug/TiFlashTestEnv.h>
#include <Storages/S3/S3Common.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <aws/core/AmazonWebServiceRequest.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutBucketLifecycleConfigurationRequest.h>
#include <gtest/gtest.h>

#include <ext/scope_guard.h>

namespace DB::FailPoints
{
extern const char force_set_lifecycle_resp[];
} // namespace DB::FailPoints

namespace DB::S3
{
Aws::S3::Model::BucketLifecycleConfiguration genNewLifecycleConfig(
    const Aws::Vector<Aws::S3::Model::LifecycleRule> & existing_rules,
    Int32 expire_days,
    bool use_ali_oss_format);
}

namespace DB::S3::tests
{

class S3ClientTest : public ::testing::Test
{
public:
    void SetUp() override
    {
        client = ClientFactory::instance().sharedTiFlashClient();
        ::DB::tests::TiFlashTestEnv::deleteBucket(*client);
        ::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*client);
    }

    std::shared_ptr<TiFlashS3Client> client;
};

TEST_F(S3ClientTest, LifecycleSerialize)
{
    {
        // aws default format
        // serialize
        auto cfg = genNewLifecycleConfig({}, 1, false);
        Aws::S3::Model::PutBucketLifecycleConfigurationRequest req;
        req.WithLifecycleConfiguration(cfg);
        auto text = req.SerializePayload();
        LOG_INFO(Logger::get(), "lifecycle config: {}", text);
        ASSERT_EQ(
            text,
            "<?xml version=\"1.0\"?>\n<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n    "
            "<Rule>\n        <Expiration>\n            <Days>1</Days>\n        </Expiration>\n        "
            "<ID>tiflashgc</ID>\n        <Filter>\n            <And>\n                <Prefix></Prefix>\n              "
            "  <Tag>\n                    <Key>tiflash_deleted</Key>\n                    <Value>true</Value>\n        "
            "        </Tag>\n            </And>\n        </Filter>\n        <Status>Enabled</Status>\n    "
            "</Rule>\n</LifecycleConfiguration>\n");

        // deserialize
        Aws::Utils::Xml::XmlDocument doc = Aws::Utils::Xml::XmlDocument::CreateFromXmlString(text);
        Aws::S3::Model::GetBucketLifecycleConfigurationResult result(
            Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>(std::move(doc), {}));
        const auto & rules = result.GetRules();
        ASSERT_EQ(rules.size(), 1);
        const auto & rule = rules[0];
        ASSERT_EQ(rule.GetAliOssFormat(), false);
        ASSERT_EQ(rule.GetExpiration().GetDays(), 1);
        ASSERT_EQ(rule.GetStatus(), Aws::S3::Model::ExpirationStatus::Enabled);
        ASSERT_EQ(rule.GetID(), "tiflashgc");
        const auto & tags = rule.GetFilter().GetAnd().GetTags();
        ASSERT_EQ(tags.size(), 1);
        const auto & tag = tags[0];
        ASSERT_EQ(tag.GetKey(), "tiflash_deleted");
        ASSERT_EQ(tag.GetValue(), "true");
    }

    {
        // alibaba cloud oss format
        // serialize
        auto cfg = genNewLifecycleConfig({}, 1, true);
        Aws::S3::Model::PutBucketLifecycleConfigurationRequest req;
        req.WithLifecycleConfiguration(cfg);
        auto text = req.SerializePayload();
        LOG_INFO(Logger::get(), "lifecycle config: {}", text);
        ASSERT_EQ(
            text,
            "<?xml version=\"1.0\"?>\n<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n    "
            "<Rule>\n        <Expiration>\n            <Days>1</Days>\n        </Expiration>\n        "
            "<ID>tiflashgc</ID>\n        <Prefix></Prefix>\n        <Tag>\n            <Key>tiflash_deleted</Key>\n    "
            "        <Value>true</Value>\n        </Tag>\n        <Status>Enabled</Status>\n    "
            "</Rule>\n</LifecycleConfiguration>\n");

        // deserialize
        Aws::Utils::Xml::XmlDocument doc = Aws::Utils::Xml::XmlDocument::CreateFromXmlString(text);
        Aws::S3::Model::GetBucketLifecycleConfigurationResult result(
            Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>(std::move(doc), {}));
        const auto & rules = result.GetRules();
        ASSERT_EQ(rules.size(), 1);
        const auto & rule = rules[0];
        ASSERT_EQ(rule.GetAliOssFormat(), true);
        ASSERT_EQ(rule.GetExpiration().GetDays(), 1);
        ASSERT_EQ(rule.GetStatus(), Aws::S3::Model::ExpirationStatus::Enabled);
        ASSERT_EQ(rule.GetID(), "tiflashgc");
        const auto & tag = rule.GetFilter().GetTag();
        ASSERT_EQ(tag.GetKey(), "tiflash_deleted");
        ASSERT_EQ(tag.GetValue(), "true");
    }
}

TEST_F(S3ClientTest, LifecycleRule)
try
{
    ASSERT_TRUE(ensureLifecycleRuleExist(*client, 1));

    // Only run following failpoint test for mock client
    if (!DB::tests::TiFlashTestEnv::isMockedS3Client())
        return;
    auto log = Logger::get();
    {
        LOG_INFO(log, "checking with mock result 1");
        std::vector<Aws::S3::Model::Tag> filter_tags{
            Aws::S3::Model::Tag().WithKey("tiflash_deleted").WithValue("true"),
        };
        std::vector<Aws::S3::Model::LifecycleRule> rules{
            Aws::S3::Model::LifecycleRule()
                .WithStatus(Aws::S3::Model::ExpirationStatus::Enabled)
                .WithFilter(Aws::S3::Model::LifecycleRuleFilter().WithAnd(
                    Aws::S3::Model::LifecycleRuleAndOperator().WithPrefix("").WithTags(filter_tags)))
                .WithExpiration(Aws::S3::Model::LifecycleExpiration().WithDays(1))
                .WithID("tiflashgc"),
        };
        FailPointHelper::enableFailPoint(FailPoints::force_set_lifecycle_resp, rules);
        SCOPE_EXIT(FailPointHelper::disableFailPoint(FailPoints::force_set_lifecycle_resp));

        ASSERT_TRUE(ensureLifecycleRuleExist(*client, 1));
    }
    {
        LOG_INFO(log, "checking with mock result 2");
        std::vector<Aws::S3::Model::LifecycleRule> rules{
            Aws::S3::Model::LifecycleRule()
                .WithStatus(Aws::S3::Model::ExpirationStatus::Enabled)
                .WithFilter(Aws::S3::Model::LifecycleRuleFilter().WithTag(
                    Aws::S3::Model::Tag().WithKey("tiflash_deleted").WithValue("true")))
                .WithExpiration(Aws::S3::Model::LifecycleExpiration().WithDays(1))
                .WithID("tiflashgc"),
        };
        FailPointHelper::enableFailPoint(FailPoints::force_set_lifecycle_resp, rules);
        SCOPE_EXIT(FailPointHelper::disableFailPoint(FailPoints::force_set_lifecycle_resp));

        ASSERT_TRUE(ensureLifecycleRuleExist(*client, 1));
    }
    {
        LOG_INFO(log, "checking with mock result 3");
        std::vector<Aws::S3::Model::LifecycleRule> rules{
            Aws::S3::Model::LifecycleRule()
                // disabled
                .WithStatus(Aws::S3::Model::ExpirationStatus::Disabled)
                .WithFilter(Aws::S3::Model::LifecycleRuleFilter().WithTag(
                    Aws::S3::Model::Tag().WithKey("tiflash_deleted").WithValue("true")))
                .WithExpiration(Aws::S3::Model::LifecycleExpiration().WithDays(1))
                .WithID("tiflashgc"),
        };
        FailPointHelper::enableFailPoint(FailPoints::force_set_lifecycle_resp, rules);
        SCOPE_EXIT(FailPointHelper::disableFailPoint(FailPoints::force_set_lifecycle_resp));

        ASSERT_TRUE(ensureLifecycleRuleExist(*client, 1));
    }
    {
        LOG_INFO(log, "checking with mock result 4");
        std::vector<Aws::S3::Model::LifecycleRule> rules{
            Aws::S3::Model::LifecycleRule()
                .WithStatus(Aws::S3::Model::ExpirationStatus::Disabled)
                .WithFilter(Aws::S3::Model::LifecycleRuleFilter().WithTag(
                    // not the tiflash_deleted tag
                    Aws::S3::Model::Tag().WithKey("tikv").WithValue("true")))
                .WithExpiration(Aws::S3::Model::LifecycleExpiration().WithDays(1))
                .WithID("tiflashgc"),
        };
        FailPointHelper::enableFailPoint(FailPoints::force_set_lifecycle_resp, rules);
        SCOPE_EXIT(FailPointHelper::disableFailPoint(FailPoints::force_set_lifecycle_resp));

        ASSERT_TRUE(ensureLifecycleRuleExist(*client, 1));
    }
}
CATCH

TEST_F(S3ClientTest, UploadRead)
try
{
    deleteObject(*client, "s999/manifest/mf_1");
    ASSERT_FALSE(objectExists(*client, "s999/manifest/mf_1"));
    uploadEmptyFile(*client, "s999/manifest/mf_1");
    ASSERT_TRUE(objectExists(*client, "s999/manifest/mf_1"));

    uploadEmptyFile(*client, "s999/manifest/mf_2");
    uploadEmptyFile(*client, "s999/manifest/mf_789");

    uploadEmptyFile(*client, "s999/data/dat_789_0");
    uploadEmptyFile(*client, "s999/data/dat_790_0");

    uploadEmptyFile(*client, "s999/abcd");

    {
        Strings prefixes;
        listPrefixWithDelimiter(*client, "s999/", "/", [&](const Aws::S3::Model::CommonPrefix & p) {
            prefixes.emplace_back(p.GetPrefix());
            return PageResult{.num_keys = 1, .more = true};
        });
        ASSERT_EQ(prefixes.size(), 2) << fmt::format("{}", prefixes);
        EXPECT_EQ(prefixes[0], "s999/data/");
        EXPECT_EQ(prefixes[1], "s999/manifest/");
    }

    // check the keys with raw `LIST` request
    {
        Strings prefixes;
        rawListPrefix(
            *client,
            client->bucket(),
            client->root() + "s999/",
            "/",
            [&](const Aws::S3::Model::ListObjectsV2Result & result) {
                const auto & ps = result.GetCommonPrefixes();
                for (const auto & p : ps)
                {
                    prefixes.emplace_back(p.GetPrefix());
                }
                return PageResult{.num_keys = ps.size(), .more = true};
            });
        ASSERT_EQ(prefixes.size(), 2) << fmt::format("{}", prefixes);
        EXPECT_EQ(prefixes[0], client->root() + "s999/data/");
        EXPECT_EQ(prefixes[1], client->root() + "s999/manifest/");
    }
}
CATCH

} // namespace DB::S3::tests
