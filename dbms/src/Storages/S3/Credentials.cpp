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

#include <Common/Logger.h>
#include <Storages/S3/Credentials.h>
#include <Storages/S3/CredentialsAliCloud.h>
#include <Storages/S3/PocoHTTPClientFactory.h>
#include <aws/core/Version.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/SpecifiedRetryableErrorsRetryStrategy.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/platform/OSVersionInfo.h>
#include <aws/core/utils/UUID.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <common/logger_useful.h>

#include <fstream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#pragma GCC diagnostic pop

namespace DB::S3
{


static const char STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG[] = "STSAssumeRoleWebIdentityCredentialsProvider";

// Override Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider for better logging and metrics
class STSAssumeRoleWebIdentityCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    static std::shared_ptr<Aws::Auth::AWSCredentialsProvider> build();

    STSAssumeRoleWebIdentityCredentialsProvider(String role_arn, String token_file, String region, String session_name);

    /**
     * Retrieves the credentials if found, otherwise returns empty credential set.
     */
    Aws::Auth::AWSCredentials GetAWSCredentials() override;

protected:
    void Reload() override;

private:
    void refreshIfExpired();
    Aws::String calculateQueryString() const;
    bool expiresSoon() const;

    Aws::UniquePtr<Aws::Internal::STSCredentialsClient> m_client;
    Aws::Auth::AWSCredentials m_credentials;
    Aws::String m_role_arn;
    Aws::String m_token_file;
    Aws::String m_session_name;
    Aws::String m_token;
    bool m_initialized;
    LoggerPtr log;
};

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> STSAssumeRoleWebIdentityCredentialsProvider::build()
{
    auto log = Logger::get();

    // check environment variables
    Aws::String tmp_region = Aws::Environment::GetEnv("AWS_DEFAULT_REGION");
    String token_file = Aws::Environment::GetEnv("AWS_WEB_IDENTITY_TOKEN_FILE");
    String role_arn = Aws::Environment::GetEnv("AWS_ROLE_ARN");
    String session_name = Aws::Environment::GetEnv("AWS_ROLE_SESSION_NAME");

    // check profile_config if either m_role_arn or m_token_file is not loaded from environment variable
    // region source is not enforced, but we need it to construct sts endpoint, if we can't find from environment, we should check if it's set in config file.
    if (role_arn.empty() || token_file.empty() || tmp_region.empty())
    {
        auto profile = Aws::Config::GetCachedConfigProfile(Aws::Auth::GetConfigProfileName());
        if (tmp_region.empty())
        {
            tmp_region = profile.GetRegion();
        }
        // If either of these two were not found from environment, use whatever found for all three in config file
        if (role_arn.empty() || token_file.empty())
        {
            role_arn = profile.GetRoleArn();
            token_file = profile.GetValue("web_identity_token_file");
            session_name = profile.GetValue("role_session_name");
        }
    }

    if (token_file.empty())
    {
        LOG_INFO(log, "Token file must be specified to use STS AssumeRole web identity creds provider.");
        return nullptr;
    }
    if (role_arn.empty())
    {
        LOG_INFO(log, "RoleArn must be specified to use STS AssumeRole web identity creds provider.");
        return nullptr;
    }

    return std::make_shared<STSAssumeRoleWebIdentityCredentialsProvider>(
        role_arn,
        token_file,
        tmp_region,
        session_name);
}

STSAssumeRoleWebIdentityCredentialsProvider::STSAssumeRoleWebIdentityCredentialsProvider(
    String role_arn,
    String token_file,
    String tmp_region,
    String session_name)
    : m_role_arn(std::move(role_arn))
    , m_token_file(std::move(token_file))
    , m_session_name(std::move(session_name))
    , m_initialized(false)
    , log(Logger::get())
{
    assert(!m_role_arn.empty());
    assert(!m_token_file.empty());

    if (tmp_region.empty())
        tmp_region = Aws::Region::US_EAST_1;

    if (m_session_name.empty())
        m_session_name = Aws::Utils::UUID::RandomUUID();

    Aws::Client::ClientConfiguration aws_client_configuration;
    aws_client_configuration.scheme = Aws::Http::Scheme::HTTPS;
    aws_client_configuration.region = tmp_region;

    Aws::Vector<Aws::String> retryable_errors{
        "IDPCommunicationError",
        "InvalidIdentityToken",
    };

    aws_client_configuration.retryStrategy = Aws::MakeShared<Aws::Client::SpecifiedRetryableErrorsRetryStrategy>(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        retryable_errors,
        3 /*maxRetries*/);

    m_client = Aws::MakeUnique<Aws::Internal::STSCredentialsClient>(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        aws_client_configuration);
    m_initialized = true;
    LOG_DEBUG(
        log,
        "Created STS AssumeRole with web identity creds provider, role_arn={} token_file={} region={} session_name={}",
        m_role_arn,
        m_token_file,
        tmp_region,
        m_session_name);
    LOG_INFO(log, "Creating STS AssumeRole with web identity creds provider");
}

Aws::Auth::AWSCredentials STSAssumeRoleWebIdentityCredentialsProvider::GetAWSCredentials()
{
    // A valid client means required information like role arn and token file were constructed correctly.
    // We can use this provider to load creds, otherwise, we can just return empty creds.
    if (!m_initialized)
    {
        return Aws::Auth::AWSCredentials();
    }
    refreshIfExpired();
    Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
    return m_credentials;
}

void STSAssumeRoleWebIdentityCredentialsProvider::Reload()
{
    LOG_INFO(
        log,
        "Credentials have expired, attempting to renew from STS, role_arn={} role_session_name={}.",
        m_role_arn,
        m_session_name);

    std::ifstream token_file(m_token_file.c_str());
    if (token_file)
    {
        Aws::String token((std::istreambuf_iterator<char>(token_file)), std::istreambuf_iterator<char>());
        m_token = token;
    }
    else
    {
        LOG_ERROR(log, "Can't open token file: {}", m_token_file);
        return;
    }
    Aws::Internal::STSCredentialsClient::STSAssumeRoleWithWebIdentityRequest request{
        m_session_name,
        m_role_arn,
        m_token};

    const auto result = m_client->GetAssumeRoleWithWebIdentityCredentials(request);
    LOG_TRACE(log, "Successfully retrieved credentials with AWS_ACCESS_KEY: {}", result.creds.GetAWSAccessKeyId());
    m_credentials = result.creds;
}

static const int STS_CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD = 5 * 1000; // 5 seconds

bool STSAssumeRoleWebIdentityCredentialsProvider::expiresSoon() const
{
    return (
        (m_credentials.GetExpiration() - Aws::Utils::DateTime::Now()).count()
        < STS_CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD);
}

void STSAssumeRoleWebIdentityCredentialsProvider::refreshIfExpired()
{
    Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
    if (!m_credentials.IsEmpty() && !expiresSoon())
    {
        return;
    }

    guard.UpgradeToWriterLock();
    if (!m_credentials.IsExpiredOrEmpty() && !expiresSoon()) // double-checked lock to avoid refreshing twice
    {
        return;
    }

    Reload();
}

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> buildECSCredentialsProvider(const LoggerPtr & log)
{
    //ECS TaskRole Credentials only available when ENVIRONMENT VARIABLE is set
    static const char AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI[] = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
    auto const relative_uri = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI);
    LOG_DEBUG(log, "The environment variable value {} is {}", AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI, relative_uri);

    static const char AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI[] = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
    auto const absolute_uri = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI);
    LOG_DEBUG(log, "The environment variable value {} is {}", AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI, absolute_uri);

    static const char AWS_EC2_METADATA_DISABLED[] = "AWS_EC2_METADATA_DISABLED";
    auto const ec2_metadata_disabled = Aws::Environment::GetEnv(AWS_EC2_METADATA_DISABLED);
    LOG_DEBUG(log, "The environment variable value {} is {}", AWS_EC2_METADATA_DISABLED, ec2_metadata_disabled);

    if (!relative_uri.empty())
    {
        LOG_INFO(
            log,
            "Added ECS metadata service credentials provider with relative path: [{}] to the provider chain",
            relative_uri);
        return std::make_shared<Aws::Auth::TaskRoleCredentialsProvider>(relative_uri.c_str());
    }
    else if (!absolute_uri.empty())
    {
        static const char AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN[] = "AWS_CONTAINER_AUTHORIZATION_TOKEN";
        const auto token = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN);
        // DO NOT log the value of the authorization token for security purposes.
        LOG_INFO(
            log,
            "Added ECS credentials provider with URI: [{}] to the provider chain with {} authorization token",
            absolute_uri,
            (token.empty() ? "an empty" : "a non-empty"));
        return std::make_shared<Aws::Auth::TaskRoleCredentialsProvider>(absolute_uri.c_str(), token.c_str());
    }
    else if (Aws::Utils::StringUtils::ToLower(ec2_metadata_disabled.c_str()) != "true")
    {
        LOG_INFO(log, "Added EC2 metadata service credentials provider to the provider chain");
        return std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>();
    }

    // EC2 metadata service is disabled, do not add the provider
    LOG_INFO(log, "IMDS is disabled for AWS ECS/EC2 creds provider");
    return nullptr;
}

class TiFlashEnvironmentCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    /**
     * Reads credentials from the Environment variables ACCESS_KEY_ID and SECRET_ACCESS_KEY and SESSION_TOKEN if they exist.
     * If they are not found, empty credentials are returned.
    */
    Aws::Auth::AWSCredentials GetAWSCredentials() override
    {
        auto log = Logger::get();
        Aws::Auth::AWSCredentials credentials;

        if (auto access_key = Aws::Environment::GetEnv("ACCESS_KEY_ID"); !access_key.empty())
        {
            credentials.SetAWSAccessKeyId(access_key);

            auto secret_key = Aws::Environment::GetEnv("SECRET_ACCESS_KEY");
            if (!secret_key.empty())
            {
                credentials.SetAWSSecretKey(secret_key);
            }

            auto session_token = Aws::Environment::GetEnv("SESSION_TOKEN");
            if (!session_token.empty())
            {
                credentials.SetSessionToken(session_token);
            }

            LOG_INFO(
                log,
                "Creating TiFlashEnvironmentCredentialsProvider with ACCESS_KEY_ID, "
                "access_key_id_size={} secret_key_size={} session_token_size={}",
                access_key.size(),
                secret_key.size(),
                session_token.size());
        }

        return credentials;
    }
};

/// S3CredentialsProviderChain ///

S3CredentialsProviderChain::S3CredentialsProviderChain(const Aws::Client::ClientConfiguration & cfg, CloudVendor vendor)
    : log(Logger::get())
{
    /// AWS API tries credentials providers one by one. Some of providers (like ProfileConfigFileAWSCredentialsProvider) can be
    /// quite verbose even if nobody configured them. So tiflash use our provider first and only after it use default providers.
    /// And ProcessCredentialsProvider is useless in tiflash deployment cases, removed.

    switch (vendor)
    {
    case CloudVendor::AWS:
    {
        if (auto provider = DB::S3::STSAssumeRoleWebIdentityCredentialsProvider::build(); provider != nullptr)
            AddProvider(provider);
        // AWS environment variable credentials provider always added
        AddProvider(std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>());

        // AWS ECS credentials provider
        if (auto provider = buildECSCredentialsProvider(log); provider != nullptr)
            AddProvider(provider);

        /// Quite verbose provider (argues if file with credentials doesn't exist) so it's the last one
        /// in chain.
        AddProvider(std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>());
        break;
    }
    case CloudVendor::AlibabaCloud:
    {
        // Alibaba Cloud credentials providers
        if (auto provider = DB::S3::AlibabaCloud::ECSRAMRoleCredentialsProvider::build(cfg); provider != nullptr)
        {
            AddProvider(provider);
        }
        if (auto provider = DB::S3::AlibabaCloud::OIDCCredentialsProvider::build(cfg); provider != nullptr)
        {
            AddProvider(provider);
        }
        break;
    }
    case CloudVendor::Unknown:
    {
        AddProvider(std::make_shared<TiFlashEnvironmentCredentialsProvider>());
        // Add AWS environment variable credentials provider as a default fallback
        AddProvider(std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>());
        break;
    }
    }
}

} // namespace DB::S3
