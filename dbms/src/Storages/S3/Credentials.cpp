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

#include <Common/Logger.h>
#include <Storages/S3/Credentials.h>
#include <aws/core/Version.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/core/client/SpecifiedRetryableErrorsRetryStrategy.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/platform/OSVersionInfo.h>
#include <aws/core/utils/UUID.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <common/logger_useful.h>

#include <fstream>

namespace DB::S3
{


static const char STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG[] = "STSAssumeRoleWithWebIdentityCredentialsProvider";
static const int STS_CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD = 5 * 1000;

// Override Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider by http
class STSAssumeRoleWebIdentityCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    STSAssumeRoleWebIdentityCredentialsProvider(DB::S3::PocoHTTPClientConfiguration & aws_client_configuration);

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


STSAssumeRoleWebIdentityCredentialsProvider::STSAssumeRoleWebIdentityCredentialsProvider(DB::S3::PocoHTTPClientConfiguration & aws_client_configuration)
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

    aws_client_configuration.scheme = Aws::Http::Scheme::HTTPS;
    aws_client_configuration.region = tmp_region;

    Aws::Vector<Aws::String> retryable_errors;
    retryable_errors.push_back("IDPCommunicationError");
    retryable_errors.push_back("InvalidIdentityToken");

    aws_client_configuration.retryStrategy = Aws::MakeShared<Aws::Client::SpecifiedRetryableErrorsRetryStrategy>(STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG, retryable_errors, 3 /*maxRetries*/);

    m_client = Aws::MakeUnique<Aws::Internal::STSCredentialsClient>(STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG, aws_client_configuration);
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
    LOG_INFO(log, "Credentials have expired, attempting to renew from STS.");

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
    Aws::Internal::STSCredentialsClient::STSAssumeRoleWithWebIdentityRequest request{m_session_name, m_role_arn, m_token};

    auto result = m_client->GetAssumeRoleWithWebIdentityCredentials(request);
    LOG_TRACE(log, "Successfully retrieved credentials with AWS_ACCESS_KEY: {}", result.creds.GetAWSAccessKeyId());
    m_credentials = result.creds;
}

bool STSAssumeRoleWebIdentityCredentialsProvider::expiresSoon() const
{
    return ((m_credentials.GetExpiration() - Aws::Utils::DateTime::Now()).count() < STS_CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD);
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


class AWSEC2MetadataClient : public Aws::Internal::AWSHttpResourceClient
{
    static constexpr char EC2_SECURITY_CREDENTIALS_RESOURCE[] = "/latest/meta-data/iam/security-credentials";
    static constexpr char EC2_IMDS_TOKEN_RESOURCE[] = "/latest/api/token";
    static constexpr char EC2_IMDS_TOKEN_HEADER[] = "x-aws-ec2-metadata-token";
    static constexpr char EC2_IMDS_TOKEN_TTL_DEFAULT_VALUE[] = "21600";
    static constexpr char EC2_IMDS_TOKEN_TTL_HEADER[] = "x-aws-ec2-metadata-token-ttl-seconds";

public:
    /// See EC2MetadataClient.

    explicit AWSEC2MetadataClient(const Aws::Client::ClientConfiguration & client_configuration, const char * endpoint_);

    AWSEC2MetadataClient & operator=(const AWSEC2MetadataClient & rhs) = delete;
    AWSEC2MetadataClient(const AWSEC2MetadataClient & rhs) = delete;
    AWSEC2MetadataClient & operator=(const AWSEC2MetadataClient && rhs) = delete;
    AWSEC2MetadataClient(const AWSEC2MetadataClient && rhs) = delete;

    ~AWSEC2MetadataClient() override = default;

    using Aws::Internal::AWSHttpResourceClient::GetResource;

    virtual Aws::String GetResource(const char * resource_path) const;
    virtual Aws::String getDefaultCredentials() const;

    static Aws::String awsComputeUserAgentString();

    virtual Aws::String getDefaultCredentialsSecurely() const;

    virtual Aws::String getCurrentRegion() const;

private:
    const Aws::String endpoint;
    mutable std::recursive_mutex token_mutex;
    mutable Aws::String token;
    Poco::Logger * logger;
};

std::shared_ptr<AWSEC2MetadataClient> InitEC2MetadataClient(const Aws::Client::ClientConfiguration & client_configuration);

class AWSEC2InstanceProfileConfigLoader : public Aws::Config::AWSProfileConfigLoader
{
public:
    explicit AWSEC2InstanceProfileConfigLoader(const std::shared_ptr<AWSEC2MetadataClient> & client_, bool use_secure_pull_);

    ~AWSEC2InstanceProfileConfigLoader() override = default;

protected:
    bool LoadInternal() override;

private:
    std::shared_ptr<AWSEC2MetadataClient> client;
    bool use_secure_pull;
    Poco::Logger * logger;
};

class AWSInstanceProfileCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    /// See InstanceProfileCredentialsProvider.

    explicit AWSInstanceProfileCredentialsProvider(const std::shared_ptr<AWSEC2InstanceProfileConfigLoader> & config_loader);

    Aws::Auth::AWSCredentials GetAWSCredentials() override;

protected:
    void Reload() override;

private:
    void refreshIfExpired();

    std::shared_ptr<AWSEC2InstanceProfileConfigLoader> ec2_metadata_config_loader;
    Int64 load_frequency_ms;
    Poco::Logger * logger;
};

AWSEC2MetadataClient::AWSEC2MetadataClient(const Aws::Client::ClientConfiguration & client_configuration, const char * endpoint_)
    : Aws::Internal::AWSHttpResourceClient(client_configuration)
    , endpoint(endpoint_)
    , logger(&Poco::Logger::get("AWSEC2InstanceProfileConfigLoader"))
{
}

Aws::String AWSEC2MetadataClient::GetResource(const char * resource_path) const
{
    return GetResource(endpoint.c_str(), resource_path, nullptr /*authToken*/);
}

Aws::String AWSEC2MetadataClient::getDefaultCredentials() const
{
    String credentials_string;
    {
        std::lock_guard locker(token_mutex);

        LOG_TRACE(logger, "Getting default credentials for ec2 instance from {}", endpoint);
        auto result = GetResourceWithAWSWebServiceResult(endpoint.c_str(), EC2_SECURITY_CREDENTIALS_RESOURCE, nullptr);
        credentials_string = result.GetPayload();
        if (result.GetResponseCode() == Aws::Http::HttpResponseCode::UNAUTHORIZED)
        {
            return {};
        }
    }

    String trimmed_credentials_string = Aws::Utils::StringUtils::Trim(credentials_string.c_str());
    if (trimmed_credentials_string.empty())
        return {};

    std::vector<String> security_credentials = Aws::Utils::StringUtils::Split(trimmed_credentials_string, '\n');

    LOG_DEBUG(logger, "Calling EC2MetadataService resource, {} returned credential string {}.", EC2_SECURITY_CREDENTIALS_RESOURCE, trimmed_credentials_string);

    if (security_credentials.empty())
    {
        LOG_WARNING(logger, "Initial call to EC2MetadataService to get credentials failed.");
        return {};
    }

    Aws::StringStream ss;
    ss << EC2_SECURITY_CREDENTIALS_RESOURCE << "/" << security_credentials[0];
    LOG_DEBUG(logger, "Calling EC2MetadataService resource {}.", ss.str());
    return GetResource(ss.str().c_str());
}

Aws::String AWSEC2MetadataClient::awsComputeUserAgentString()
{
    Aws::StringStream ss;
    ss << "aws-sdk-cpp/" << Aws::Version::GetVersionString() << " " << Aws::OSVersionInfo::ComputeOSVersionString()
       << " " << Aws::Version::GetCompilerVersionString();
    return ss.str();
}

Aws::String AWSEC2MetadataClient::getDefaultCredentialsSecurely() const
{
    String user_agent_string = awsComputeUserAgentString();
    String new_token;

    {
        std::lock_guard locker(token_mutex);

        Aws::StringStream ss;
        ss << endpoint << EC2_IMDS_TOKEN_RESOURCE;
        std::shared_ptr<Aws::Http::HttpRequest> token_request(Aws::Http::CreateHttpRequest(ss.str(), Aws::Http::HttpMethod::HTTP_PUT, Aws::Utils::Stream::DefaultResponseStreamFactoryMethod));
        token_request->SetHeaderValue(EC2_IMDS_TOKEN_TTL_HEADER, EC2_IMDS_TOKEN_TTL_DEFAULT_VALUE);
        token_request->SetUserAgent(user_agent_string);
        LOG_TRACE(logger, "Calling EC2MetadataService to get token.");
        auto result = GetResourceWithAWSWebServiceResult(token_request);
        const String & token_string = result.GetPayload();
        new_token = Aws::Utils::StringUtils::Trim(token_string.c_str());

        if (result.GetResponseCode() == Aws::Http::HttpResponseCode::BAD_REQUEST)
        {
            return {};
        }
        else if (result.GetResponseCode() != Aws::Http::HttpResponseCode::OK || new_token.empty())
        {
            LOG_TRACE(logger, "Calling EC2MetadataService to get token failed, falling back to less secure way.");
            return getDefaultCredentials();
        }
        token = new_token;
    }

    String url = endpoint + EC2_SECURITY_CREDENTIALS_RESOURCE;
    std::shared_ptr<Aws::Http::HttpRequest> profile_request(Aws::Http::CreateHttpRequest(url,
                                                                                         Aws::Http::HttpMethod::HTTP_GET,
                                                                                         Aws::Utils::Stream::DefaultResponseStreamFactoryMethod));
    profile_request->SetHeaderValue(EC2_IMDS_TOKEN_HEADER, new_token);
    profile_request->SetUserAgent(user_agent_string);
    String profile_string = GetResourceWithAWSWebServiceResult(profile_request).GetPayload();

    String trimmed_profile_string = Aws::Utils::StringUtils::Trim(profile_string.c_str());
    std::vector<String> security_credentials = Aws::Utils::StringUtils::Split(trimmed_profile_string, '\n');

    LOG_DEBUG(logger, "Calling EC2MetadataService resource, {} with token returned profile string {}.", EC2_SECURITY_CREDENTIALS_RESOURCE, trimmed_profile_string);

    if (security_credentials.empty())
    {
        LOG_WARNING(logger, "Calling EC2Metadataservice to get profiles failed.");
        return {};
    }

    Aws::StringStream ss;
    ss << endpoint << EC2_SECURITY_CREDENTIALS_RESOURCE << "/" << security_credentials[0];
    std::shared_ptr<Aws::Http::HttpRequest> credentials_request(Aws::Http::CreateHttpRequest(ss.str(),
                                                                                             Aws::Http::HttpMethod::HTTP_GET,
                                                                                             Aws::Utils::Stream::DefaultResponseStreamFactoryMethod));
    credentials_request->SetHeaderValue(EC2_IMDS_TOKEN_HEADER, new_token);
    credentials_request->SetUserAgent(user_agent_string);
    LOG_DEBUG(logger, "Calling EC2MetadataService resource {} with token.", ss.str());
    return GetResourceWithAWSWebServiceResult(credentials_request).GetPayload();
}

Aws::String AWSEC2MetadataClient::getCurrentRegion() const
{
    return Aws::Region::AWS_GLOBAL;
}

std::shared_ptr<AWSEC2MetadataClient> InitEC2MetadataClient(const Aws::Client::ClientConfiguration & client_configuration)
{
    Aws::String ec2_metadata_service_endpoint = Aws::Environment::GetEnv("AWS_EC2_METADATA_SERVICE_ENDPOINT");
    auto * logger = &Poco::Logger::get("AWSEC2InstanceProfileConfigLoader");
    if (ec2_metadata_service_endpoint.empty())
    {
        Aws::String ec2_metadata_service_endpoint_mode = Aws::Environment::GetEnv("AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE");
        if (ec2_metadata_service_endpoint_mode.length() == 0)
        {
            ec2_metadata_service_endpoint = "http://169.254.169.254"; //default to IPv4 default endpoint
        }
        else
        {
            if (ec2_metadata_service_endpoint_mode.length() == 4)
            {
                if (Aws::Utils::StringUtils::CaselessCompare(ec2_metadata_service_endpoint_mode.c_str(), "ipv4"))
                {
                    ec2_metadata_service_endpoint = "http://169.254.169.254"; //default to IPv4 default endpoint
                }
                else if (Aws::Utils::StringUtils::CaselessCompare(ec2_metadata_service_endpoint_mode.c_str(), "ipv6"))
                {
                    ec2_metadata_service_endpoint = "http://[fd00:ec2::254]";
                }
                else
                {
                    LOG_ERROR(logger, "AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE can only be set to ipv4 or ipv6, received: {}", ec2_metadata_service_endpoint_mode);
                }
            }
            else
            {
                LOG_ERROR(logger, "AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE can only be set to ipv4 or ipv6, received: {}", ec2_metadata_service_endpoint_mode);
            }
        }
    }
    LOG_INFO(logger, "Using IMDS endpoint: {}", ec2_metadata_service_endpoint);
    return std::make_shared<AWSEC2MetadataClient>(client_configuration, ec2_metadata_service_endpoint.c_str());
}

AWSEC2InstanceProfileConfigLoader::AWSEC2InstanceProfileConfigLoader(const std::shared_ptr<AWSEC2MetadataClient> & client_, bool use_secure_pull_)
    : client(client_)
    , use_secure_pull(use_secure_pull_)
    , logger(&Poco::Logger::get("AWSEC2InstanceProfileConfigLoader"))
{
}

bool AWSEC2InstanceProfileConfigLoader::LoadInternal()
{
    auto credentials_str = use_secure_pull ? client->getDefaultCredentialsSecurely() : client->getDefaultCredentials();

    /// See EC2InstanceProfileConfigLoader.
    if (credentials_str.empty())
        return false;

    Aws::Utils::Json::JsonValue credentials_doc(credentials_str);
    if (!credentials_doc.WasParseSuccessful())
    {
        LOG_ERROR(logger, "Failed to parse output from EC2MetadataService.");
        return false;
    }
    String access_key, secret_key, token;

    auto credentials_view = credentials_doc.View();
    access_key = credentials_view.GetString("AccessKeyId");
    LOG_TRACE(logger, "Successfully pulled credentials from EC2MetadataService with access key.");

    secret_key = credentials_view.GetString("SecretAccessKey");
    token = credentials_view.GetString("Token");

    auto region = client->getCurrentRegion();

    Aws::Config::Profile profile;
    profile.SetCredentials(Aws::Auth::AWSCredentials(access_key, secret_key, token));
    profile.SetRegion(region);
    profile.SetName(Aws::Config::INSTANCE_PROFILE_KEY);

    m_profiles[Aws::Config::INSTANCE_PROFILE_KEY] = profile;

    return true;
}

AWSInstanceProfileCredentialsProvider::AWSInstanceProfileCredentialsProvider(const std::shared_ptr<AWSEC2InstanceProfileConfigLoader> & config_loader)
    : ec2_metadata_config_loader(config_loader)
    , load_frequency_ms(Aws::Auth::REFRESH_THRESHOLD)
    , logger(&Poco::Logger::get("AWSInstanceProfileCredentialsProvider"))
{
    LOG_INFO(logger, "Creating Instance with injected EC2MetadataClient and refresh rate.");
}

Aws::Auth::AWSCredentials AWSInstanceProfileCredentialsProvider::GetAWSCredentials()
{
    refreshIfExpired();
    Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
    auto profile_it = ec2_metadata_config_loader->GetProfiles().find(Aws::Config::INSTANCE_PROFILE_KEY);

    if (profile_it != ec2_metadata_config_loader->GetProfiles().end())
    {
        return profile_it->second.GetCredentials();
    }

    return Aws::Auth::AWSCredentials();
}

void AWSInstanceProfileCredentialsProvider::Reload()
{
    LOG_INFO(logger, "Credentials have expired attempting to repull from EC2 Metadata Service.");
    ec2_metadata_config_loader->Load();
    AWSCredentialsProvider::Reload();
}

void AWSInstanceProfileCredentialsProvider::refreshIfExpired()
{
    LOG_DEBUG(logger, "Checking if latest credential pull has expired.");
    Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
    if (!IsTimeToRefresh(load_frequency_ms))
    {
        return;
    }

    guard.UpgradeToWriterLock();
    if (!IsTimeToRefresh(load_frequency_ms)) // double-checked lock to avoid refreshing twice
    {
        return;
    }
    Reload();
}

/// S3CredentialsProviderChain ///

static const char S3CredentialsProviderChainTag[] = "S3CredentialsProviderChain";

S3CredentialsProviderChain::S3CredentialsProviderChain(const PocoHTTPClientConfiguration & configuration)
    : log(Logger::get())
{
    static const char AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI[] = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
    static const char AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI[] = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
    static const char AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN[] = "AWS_CONTAINER_AUTHORIZATION_TOKEN";
    static const char AWS_EC2_METADATA_DISABLED[] = "AWS_EC2_METADATA_DISABLED";

    /// AWS API tries credentials providers one by one. Some of providers (like ProfileConfigFileAWSCredentialsProvider) can be
    /// quite verbose even if nobody configured them. So we use our provider first and only after it use default providers.
    /// And ProcessCredentialsProvider is useless in our cases, removed.

    {
        DB::S3::PocoHTTPClientConfiguration aws_client_configuration = configuration; // copy
        AddProvider(std::make_shared<DB::S3::STSAssumeRoleWebIdentityCredentialsProvider>(aws_client_configuration));
    }
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
        AddProvider(Aws::MakeShared<Aws::Auth::TaskRoleCredentialsProvider>(S3CredentialsProviderChainTag, relative_uri.c_str()));
        LOG_INFO(log, "Added ECS metadata service credentials provider with relative path: [{}] to the provider chain.", relative_uri);
    }
    else if (!absolute_uri.empty())
    {
        const auto token = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN);
        AddProvider(Aws::MakeShared<Aws::Auth::TaskRoleCredentialsProvider>(
            S3CredentialsProviderChainTag,
            absolute_uri.c_str(),
            token.c_str()));

        //DO NOT log the value of the authorization token for security purposes.
        LOG_INFO(log, "Added ECS credentials provider with URI: [{}] to the provider chain with {} authorization token.", absolute_uri, (token.empty() ? "an empty" : "a non-empty"));
    }
    else if (Aws::Utils::StringUtils::ToLower(ec2_metadata_disabled.c_str()) != "true")
    {
        DB::S3::PocoHTTPClientConfiguration aws_client_configuration = configuration;
        /// See MakeDefaultHttpResourceClientConfiguration().
        /// This is part of EC2 metadata client, but unfortunately it can't be accessed from outside
        /// of contrib/aws/aws-cpp-sdk-core/source/internal/AWSHttpResourceClient.cpp
        aws_client_configuration.maxConnections = 2;
        aws_client_configuration.scheme = Aws::Http::Scheme::HTTP;

        /// Explicitly set the proxy settings to empty/zero to avoid relying on defaults that could potentially change
        /// in the future.
        aws_client_configuration.proxyHost = "";
        aws_client_configuration.proxyUserName = "";
        aws_client_configuration.proxyPassword = "";
        aws_client_configuration.proxyPort = 0;

        /// EC2MetadataService throttles by delaying the response so the service client should set a large read timeout.
        /// EC2MetadataService delay is in order of seconds so it only make sense to retry after a couple of seconds.
        aws_client_configuration.connectTimeoutMs = 1000;
        aws_client_configuration.requestTimeoutMs = 1000;

        aws_client_configuration.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(1, 1000);

        auto ec2_metadata_client = InitEC2MetadataClient(aws_client_configuration);
        auto config_loader = std::make_shared<AWSEC2InstanceProfileConfigLoader>(ec2_metadata_client, true);

        AddProvider(std::make_shared<AWSInstanceProfileCredentialsProvider>(config_loader));
        LOG_INFO(log, "Added EC2 metadata service credentials provider to the provider chain.");

        // AddProvider(Aws::MakeShared<Aws::Auth::InstanceProfileCredentialsProvider>(S3CredentialsProviderChainTag));
        // LOG_INFO(log, "Added EC2 metadata service credentials provider to the provider chain.");
    }

    /// Quite verbose provider (argues if file with credentials doesn't exist) so iut's the last one
    /// in chain.
    AddProvider(std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>());
}

} // namespace DB::S3
