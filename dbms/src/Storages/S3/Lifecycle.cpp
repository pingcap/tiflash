// Copyright 2025 PingCAP, Inc.
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
#include <Storages/S3/Lifecycle.h>
#include <Storages/S3/S3Common.h>
#include <aws/s3/model/ExpirationStatus.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/LifecycleConfiguration.h>
#include <aws/s3/model/LifecycleExpiration.h>
#include <aws/s3/model/LifecycleRule.h>
#include <aws/s3/model/LifecycleRuleAndOperator.h>
#include <aws/s3/model/PutBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/Rule.h>
#include <aws/s3/model/Tag.h>

namespace DB::FailPoints
{
extern const char force_set_lifecycle_resp[];
} // namespace DB::FailPoints

namespace DB::S3
{

Aws::S3::Model::BucketLifecycleConfiguration genNewLifecycleConfig(
    const Aws::Vector<Aws::S3::Model::LifecycleRule> & existing_rules,
    Int32 expire_days,
    bool use_ali_oss_format)
{
    static_assert(TaggingObjectIsDeleted == "tiflash_deleted=true");
    std::vector<Aws::S3::Model::Tag> filter_tags{
        Aws::S3::Model::Tag().WithKey("tiflash_deleted").WithValue("true"),
    };
    Aws::S3::Model::LifecycleRule rule;
    rule.WithStatus(Aws::S3::Model::ExpirationStatus::Enabled)
        .WithExpiration(Aws::S3::Model::LifecycleExpiration().WithDays(expire_days))
        .WithID("tiflashgc");
    if (!use_ali_oss_format)
    {
        // Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3OutpostsLifecycleCLIJava.html
        rule.WithFilter(Aws::S3::Model::LifecycleRuleFilter().WithAnd(
            Aws::S3::Model::LifecycleRuleAndOperator().WithPrefix("").WithTags(filter_tags)));
    }
    else
    {
        // Alibaba cloud oss format
        // Note that "Prefix" field is required
        // Reference: https://github.com/aliyun/aliyun-oss-cpp-sdk/blob/c42600fb0b2057494ae3b77b93afeff42dfba0a4/sdk/src/model/SetBucketLifecycleRequest.cc#L40-L44
        rule.WithFilter(Aws::S3::Model::LifecycleRuleFilter().WithTag(filter_tags[0]).WithPrefix("")) //
            .SetAliOssFormat(true);
    }

    auto new_rules = existing_rules;
    new_rules.emplace_back(rule);
    Aws::S3::Model::BucketLifecycleConfiguration lifecycle_config;
    lifecycle_config.WithRules(new_rules);

    return lifecycle_config;
}

struct RuleInfo
{
    bool check_success = false;
    bool rule_has_been_set = false;
    Aws::Vector<Aws::S3::Model::LifecycleRule> rules;
};

RuleInfo checkLifecycleRuleExist(const TiFlashS3Client & client)
{
    Aws::S3::Model::GetBucketLifecycleConfigurationRequest req;
    req.SetBucket(client.bucket());
    auto outcome = client.GetBucketLifecycleConfiguration(req);
    if (!outcome.IsSuccess())
    {
        const auto & error = outcome.GetError();
        // The life cycle is not added at all
        if (error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND
            || error.GetExceptionName() == "NoSuchLifecycleConfiguration")
        {
            return RuleInfo{.check_success = true, .rule_has_been_set = false, .rules = {}};
        }

        LOG_ERROR(
            client.log,
            "GetBucketLifecycle fail, please check the bucket lifecycle configuration or create the lifecycle rule"
            " manually, bucket={} {}",
            client.bucket(),
            S3ErrorMessage(error));
        return RuleInfo{.check_success = false, .rule_has_been_set = false, .rules = {}};
    }

    auto res = outcome.GetResultWithOwnership();
    auto old_rules = res.GetRules();
    fiu_do_on(FailPoints::force_set_lifecycle_resp, {
        if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_set_lifecycle_resp); v)
        {
            auto rules = std::any_cast<std::vector<Aws::S3::Model::LifecycleRule>>(*v);
            old_rules = rules;
        }
    });

    bool lifecycle_rule_has_been_set = false;
    static_assert(TaggingObjectIsDeleted == "tiflash_deleted=true");
    for (const auto & rule : old_rules)
    {
        if (rule.GetAliOssFormat())
            LOG_INFO(client.log, "Found existing lifecycle rule in AliOSS format, rule_id={}", rule.GetID());

        const auto & filt = rule.GetFilter();

        std::optional<Aws::S3::Model::Tag> tag;
        if (!filt.AndHasBeenSet())
        {
            // For AWS S3, filt.AndHasBeenSet() == false
            tag = filt.GetTag();
        }
        else
        {
            // For minio filt.AndHasBeenSet() == true
            const auto & and_op = filt.GetAnd();
            const auto & tags = and_op.GetTags();
            if (tags.size() != 1 || !and_op.PrefixHasBeenSet() || !and_op.GetPrefix().empty())
            {
                continue;
            }
            tag = tags[0];
        }
        if (!tag)
            continue;
        if (tag->GetKey() == "tiflash_deleted" && tag->GetValue() == "true")
        {
            if (rule.GetStatus() == Aws::S3::Model::ExpirationStatus::Enabled)
            {
                lifecycle_rule_has_been_set = true;
            }
            else
            {
                LOG_ERROR(
                    client.log,
                    "The lifecycle rule is added but not enabled, please check the bucket lifecycle "
                    "configuration or create the lifecycle rule manually, "
                    "rule_id={} rule_status={} tag.key={} tag.value={}",
                    rule.GetID(),
                    Aws::S3::Model::ExpirationStatusMapper::GetNameForExpirationStatus(rule.GetStatus()),
                    tag->GetKey(),
                    tag->GetValue());
            }
            break;
        }
    }

    return RuleInfo{
        .check_success = true,
        .rule_has_been_set = lifecycle_rule_has_been_set,
        .rules = std::move(old_rules),
    };
}

// Ensure the lifecycle rule with filter `TaggingObjectIsDeleted` has been added.
// The lifecycle rule is required when using S3GCMethod::Lifecycle to expire the
// deleted objects automatically using the cloud platform lifecycle mechanism.
// If the lifecycle rule not exist, try to add the rule with `expire_days` days
// to expire the objects.
// Return true if the rule has been added or already exists.
bool ensureLifecycleRuleExist(const TiFlashS3Client & client, Int32 expire_days)
{
    auto rule_info = checkLifecycleRuleExist(client);
    if (!rule_info.check_success)
    {
        // Failed to check the lifecycle rule existence, return false
        return false;
    }

    bool rule_has_been_set = rule_info.rule_has_been_set;
    auto & old_rules = rule_info.rules;
    if (rule_has_been_set)
    {
        LOG_INFO(
            client.log,
            "The lifecycle rule has been set, n_rules={} filter={}",
            old_rules.size(),
            TaggingObjectIsDeleted);
        return true;
    }

    LOG_INFO(
        client.log,
        "The lifecycle rule with filter \"{}\" has not been added, n_rules={}",
        TaggingObjectIsDeleted,
        old_rules.size());

    // Try add the tiflash rule to lifecycle
    bool use_ali_oss_format = false;
    auto lifecycle_config = genNewLifecycleConfig(old_rules, expire_days, use_ali_oss_format);
    Aws::S3::Model::PutBucketLifecycleConfigurationRequest request;
    request.WithBucket(client.bucket()).WithLifecycleConfiguration(lifecycle_config);
    auto outcome = client.PutBucketLifecycleConfiguration(request);
    if (outcome.IsSuccess())
    {
        LOG_INFO(
            client.log,
            "The lifecycle rule has been added, new_n_rules={} tag={} use_ali_oss_format={}",
            old_rules.size(),
            TaggingObjectIsDeleted,
            use_ali_oss_format);
        return true;
    }
    const auto & error = outcome.GetError();
    LOG_WARNING(
        client.log,
        "Create lifecycle rule with tag filter \"{}\" failed, retrying with another format, bucket={} "
        "use_ali_oss_format={} {}",
        TaggingObjectIsDeleted,
        client.bucket(),
        use_ali_oss_format,
        S3ErrorMessage(error));

    // Retry with another format
    use_ali_oss_format = true;
    lifecycle_config = genNewLifecycleConfig(old_rules, expire_days, use_ali_oss_format);
    request.WithBucket(client.bucket()).WithLifecycleConfiguration(lifecycle_config);
    outcome = client.PutBucketLifecycleConfiguration(request);
    if (outcome.IsSuccess())
    {
        LOG_INFO(
            client.log,
            "The lifecycle rule has been added, new_n_rules={} tag={} use_ali_oss_format={}",
            old_rules.size(),
            TaggingObjectIsDeleted,
            use_ali_oss_format);
        return true;
    }

    // Still failed to create lifecycle rule, log an error
    LOG_ERROR(
        client.log,
        "Create lifecycle rule with tag filter \"{}\" failed, please check the bucket lifecycle configuration or "
        "create the lifecycle rule manually, bucket={} use_ali_oss_format={} {}",
        TaggingObjectIsDeleted,
        client.bucket(),
        use_ali_oss_format,
        S3ErrorMessage(error));
    return false;
}

} // namespace DB::S3
