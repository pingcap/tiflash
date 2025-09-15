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
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#pragma GCC diagnostic pop
#include <Storages/S3/Credentials.h>
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

namespace DB::S3
{


static const char STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG[] = "STSAssumeRoleWithWebIdentityCredentialsProvider";
static const int STS_CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD = 5 * 1000;

// Override Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider for better logging and metrics
class STSAssumeRoleWebIdentityCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    STSAssumeRoleWebIdentityCredentialsProvider();

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


STSAssumeRoleWebIdentityCredentialsProvider::STSAssumeRoleWebIdentityCredentialsProvider()
    : m_initialized(false)
    , log(Logger::get())
{
    // check environment variables
    Aws::String tmp_region = Aws::Environment::GetEnv("AWS_DEFAULT_REGION");
    m_role_arn = Aws::Environment::GetEnv("AWS_ROLE_ARN");
    m_token_file = Aws::Environment::GetEnv("AWS_WEB_IDENTITY_TOKEN_FILE");
    m_session_name = Aws::Environment::GetEnv("AWS_ROLE_SESSION_NAME");

    // check profile_config if either m_role_arn or m_token_file is not loaded from environment variable
    // region source is not enforced, but we need it to construct sts endpoint, if we can't find from environment, we should check if it's set in config file.
    if (m_role_arn.empty() || m_token_file.empty() || tmp_region.empty())
    {
        auto profile = Aws::Config::GetCachedConfigProfile(Aws::Auth::GetConfigProfileName());
        if (tmp_region.empty())
        {
            tmp_region = profile.GetRegion();
        }
        // If either of these two were not found from environment, use whatever found for all three in config file
        if (m_role_arn.empty() || m_token_file.empty())
        {
            m_role_arn = profile.GetRoleArn();
            m_token_file = profile.GetValue("web_identity_token_file");
            m_session_name = profile.GetValue("role_session_name");
        }
    }

    if (m_token_file.empty())
    {
        LOG_WARNING(log, "Token file must be specified to use STS AssumeRole web identity creds provider.");
        return; // No need to do further constructing
    }
    else
    {
        LOG_DEBUG(log, "Resolved token_file from profile_config or environment variable to be {}", m_token_file);
    }

    if (m_role_arn.empty())
    {
        LOG_WARNING(log, "RoleArn must be specified to use STS AssumeRole web identity creds provider.");
        return; // No need to do further constructing
    }
    else
    {
        LOG_DEBUG(log, "Resolved role_arn from profile_config or environment variable to be {}", m_role_arn);
    }

    if (tmp_region.empty())
    {
        tmp_region = Aws::Region::US_EAST_1;
    }
    else
    {
        LOG_DEBUG(log, "Resolved region from profile_config or environment variable to be {}", tmp_region);
    }

    if (m_session_name.empty())
    {
        m_session_name = Aws::Utils::UUID::RandomUUID();
    }
    else
    {
        LOG_DEBUG(log, "Resolved session_name from profile_config or environment variable to be {}", m_session_name);
    }

    Aws::Client::ClientConfiguration aws_client_configuration;
    aws_client_configuration.scheme = Aws::Http::Scheme::HTTPS;
    aws_client_configuration.region = tmp_region;

    Aws::Vector<Aws::String> retryable_errors;
    retryable_errors.push_back("IDPCommunicationError");
    retryable_errors.push_back("InvalidIdentityToken");

    aws_client_configuration.retryStrategy = Aws::MakeShared<Aws::Client::SpecifiedRetryableErrorsRetryStrategy>(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        retryable_errors,
        3 /*maxRetries*/);

    m_client = Aws::MakeUnique<Aws::Internal::STSCredentialsClient>(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        aws_client_configuration);
    m_initialized = true;
    LOG_INFO(log, "Creating STS AssumeRole with web identity creds provider.");
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

//  AWSCredentialsProvider for Alibaba Cloud
class AlibabaCloudSTSAssumeRoleWebIdentityCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    AlibabaCloudSTSAssumeRoleWebIdentityCredentialsProvider();

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

    Aws::Auth::AWSCredentials m_credentials;
    Aws::String m_oidc_provider_arn;
    Aws::String m_region_id;
    Aws::String m_role_arn;
    Aws::String m_token_file;
    Aws::String m_session_name;
    Aws::String m_token;
    Aws::String m_endpoint;
    bool m_initialized;
    std::shared_ptr<Aws::Http::HttpClientFactory> m_http_client_factory;
    std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> m_limiter;
    LoggerPtr log;
};


AlibabaCloudSTSAssumeRoleWebIdentityCredentialsProvider::AlibabaCloudSTSAssumeRoleWebIdentityCredentialsProvider()
    : m_initialized(false)
    , log(Logger::get())
{
    // check environment variables
    m_region_id = Aws::Environment::GetEnv("ALIBABA_CLOUD_REGION_ID");
    m_role_arn = Aws::Environment::GetEnv("ALIBABA_CLOUD_ROLE_ARN");
    m_oidc_provider_arn = Aws::Environment::GetEnv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN");
    m_token_file = Aws::Environment::GetEnv("ALIBABA_CLOUD_OIDC_TOKEN_FILE");

    // only support environment variable
    if (m_role_arn.empty() || m_token_file.empty() || m_region_id.empty() || m_oidc_provider_arn.empty())
    {
        LOG_WARNING(
            log,
            "Environment variables must be specified to use Alibaba Cloud STS AssumeRole web identity creds provider.");
        return;
    }

    if (m_session_name.empty())
    {
        m_session_name = Aws::Utils::UUID::RandomUUID();
    }
    else
    {
        LOG_DEBUG(log, "Resolved session_name from profile_config or environment variable to be {}", m_session_name);
    }

    m_endpoint = fmt::format("https://sts.{}.aliyuncs.com", m_region_id);
    auto poco_cfg = PocoHTTPClientConfiguration(std::make_shared<RemoteHostFilter>(), 1, false, true);
    // use default retry strategy in poco http client
    m_http_client_factory = std::make_shared<PocoHTTPClientFactory>(poco_cfg);
    m_limiter = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>(
        "AlibabaCloudSTSAssumeRoleWebIdentityCredentialsProvider",
        200000);
    m_initialized = true;
    LOG_INFO(log, "Creating Alibaba Cloud STS AssumeRole with web identity creds provider.");
}

Aws::String AlibabaCloudSTSAssumeRoleWebIdentityCredentialsProvider::calculateQueryString() const
{
    Aws::Map<Aws::String, Aws::String> params;
    params["Action"] = "AssumeRoleWithOIDC";
    params["Format"] = "JSON";
    params["Version"] = "2015-04-01";
    params["RoleSessionName"] = m_session_name;
    params["OIDCProviderArn"] = m_oidc_provider_arn;
    params["RoleArn"] = m_role_arn;
    params["OIDCToken"] = m_token;
    params["Timestamp"] = Aws::Utils::DateTime::Now().ToGmtString(Aws::Utils::DateFormat::ISO_8601);

    // build http query_string from params
    FmtBuffer buff;
    buff.joinStr(
        params.begin(),
        params.end(),
        [](const auto & iter, FmtBuffer & fb) { fb.fmtAppend("{}={}", iter.first, iter.second); },
        "&");
    Aws::String query_string = buff.toString();

    return query_string;
}

Aws::Auth::AWSCredentials AlibabaCloudSTSAssumeRoleWebIdentityCredentialsProvider::GetAWSCredentials()
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

void AlibabaCloudSTSAssumeRoleWebIdentityCredentialsProvider::Reload()
{
    LOG_INFO(
        log,
        "Credentials have expired, attempting to renew from Alibaba Cloud STS, role_arn={} role_session_name={}.",
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
    Aws::String query_string = calculateQueryString();

    // create http request
    auto http_client = m_http_client_factory->CreateHttpClient(Aws::Client::ClientConfiguration());
    auto http_request = m_http_client_factory->CreateHttpRequest(
        m_endpoint + "?" + query_string,
        Aws::Http::HttpMethod::HTTP_POST,
        Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
    // send http request
    auto response = http_client->MakeRequest(http_request, m_limiter.get(), m_limiter.get());
    if (response->GetResponseCode() != Aws::Http::HttpResponseCode::OK)
    {
        LOG_ERROR(log, "Failed to send request to Alibaba Cloud STS AssumeRole with web identity creds provider.");
        return;
    }

    // parse http response
    auto body = Aws::String(Aws::IStreamBufIterator(response->GetResponseBody()), Aws::IStreamBufIterator());
    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(body);
        const auto & obj = result.extract<Poco::JSON::Object::Ptr>();
        if (!obj->has("Credentials"))
        {
            LOG_ERROR(
                log,
                "Failed to parse response from Alibaba Cloud STS AssumeRole with web identity creds provider.");
            return;
        }
        auto credentials = obj->getObject("Credentials");
        auto access_key_id = credentials->getValue<Aws::String>("AccessKeyId");
        auto secret_access_key = credentials->getValue<Aws::String>("AccessKeySecret");
        auto security_token = credentials->getValue<Aws::String>("SecurityToken");
        auto expiration = credentials->getValue<Aws::String>("Expiration");
        Aws::Utils::DateTime expiration_time(expiration, Aws::Utils::DateFormat::ISO_8601);

        LOG_TRACE(log, "Successfully retrieved credentials with AWS_ACCESS_KEY: {}", access_key_id);
        m_credentials = Aws::Auth::AWSCredentials(access_key_id, secret_access_key, security_token, expiration_time);
    }
    catch (const Poco::Exception & e)
    {
        LOG_ERROR(log, "Failed to parse response from Alibaba Cloud STS, {}", e.displayText());
        return;
    }
}


bool AlibabaCloudSTSAssumeRoleWebIdentityCredentialsProvider::expiresSoon() const
{
    return (
        (m_credentials.GetExpiration() - Aws::Utils::DateTime::Now()).count()
        < STS_CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD);
}

void AlibabaCloudSTSAssumeRoleWebIdentityCredentialsProvider::refreshIfExpired()
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

/// S3CredentialsProviderChain ///

static const char S3CredentialsProviderChainTag[] = "S3CredentialsProviderChain";

S3CredentialsProviderChain::S3CredentialsProviderChain()
    : log(Logger::get())
{
    static const char AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI[] = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
    static const char AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI[] = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
    static const char AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN[] = "AWS_CONTAINER_AUTHORIZATION_TOKEN";
    static const char AWS_EC2_METADATA_DISABLED[] = "AWS_EC2_METADATA_DISABLED";

    /// AWS API tries credentials providers one by one. Some of providers (like ProfileConfigFileAWSCredentialsProvider) can be
    /// quite verbose even if nobody configured them. So we use our provider first and only after it use default providers.
    /// And ProcessCredentialsProvider is useless in our cases, removed.

    AddProvider(std::make_shared<DB::S3::STSAssumeRoleWebIdentityCredentialsProvider>());
    AddProvider(std::make_shared<DB::S3::AlibabaCloudSTSAssumeRoleWebIdentityCredentialsProvider>());
    AddProvider(Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>(S3CredentialsProviderChainTag));

    //ECS TaskRole Credentials only available when ENVIRONMENT VARIABLE is set
    auto const relative_uri = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI);
    LOG_DEBUG(log, "The environment variable value {} is {}", AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI, relative_uri);

    auto const absolute_uri = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI);
    LOG_DEBUG(log, "The environment variable value {} is {}", AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI, absolute_uri);

    auto const ec2_metadata_disabled = Aws::Environment::GetEnv(AWS_EC2_METADATA_DISABLED);
    LOG_DEBUG(log, "The environment variable value {} is {}", AWS_EC2_METADATA_DISABLED, ec2_metadata_disabled);

    if (!relative_uri.empty())
    {
        AddProvider(Aws::MakeShared<Aws::Auth::TaskRoleCredentialsProvider>(
            S3CredentialsProviderChainTag,
            relative_uri.c_str()));
        LOG_INFO(
            log,
            "Added ECS metadata service credentials provider with relative path: [{}] to the provider chain.",
            relative_uri);
    }
    else if (!absolute_uri.empty())
    {
        const auto token = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN);
        AddProvider(Aws::MakeShared<Aws::Auth::TaskRoleCredentialsProvider>(
            S3CredentialsProviderChainTag,
            absolute_uri.c_str(),
            token.c_str()));

        //DO NOT log the value of the authorization token for security purposes.
        LOG_INFO(
            log,
            "Added ECS credentials provider with URI: [{}] to the provider chain with {} authorization token.",
            absolute_uri,
            (token.empty() ? "an empty" : "a non-empty"));
    }
    else if (Aws::Utils::StringUtils::ToLower(ec2_metadata_disabled.c_str()) != "true")
    {
        AddProvider(Aws::MakeShared<Aws::Auth::InstanceProfileCredentialsProvider>(S3CredentialsProviderChainTag));
        LOG_INFO(log, "Added EC2 metadata service credentials provider to the provider chain.");
    }

    /// Quite verbose provider (argues if file with credentials doesn't exist) so iut's the last one
    /// in chain.
    AddProvider(std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>());
}

} // namespace DB::S3
