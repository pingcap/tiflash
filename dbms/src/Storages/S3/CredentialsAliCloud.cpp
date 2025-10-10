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

#include <Storages/S3/Credentials.h>
#include <Storages/S3/CredentialsAliCloud.h>
#include <Storages/S3/PocoHTTPClientFactory.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/UUID.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <common/logger_useful.h>

#include <magic_enum.hpp>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#pragma GCC diagnostic pop

namespace DB::S3::AlibabaCloud
{
static constexpr int CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD = 180 * 1000; // 180 seconds

/// OIDCCredentialsProvider ///
std::shared_ptr<Aws::Auth::AWSCredentialsProvider> OIDCCredentialsProvider::build()
{
    // check environment variables
    auto region_id = Aws::Environment::GetEnv("ALIBABA_CLOUD_REGION_ID");
    auto role_arn = Aws::Environment::GetEnv("ALIBABA_CLOUD_ROLE_ARN");
    auto oidc_provider_arn = Aws::Environment::GetEnv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN");
    auto token_file = Aws::Environment::GetEnv("ALIBABA_CLOUD_OIDC_TOKEN_FILE");

    // only support environment variable
    if (role_arn.empty() || token_file.empty() || region_id.empty() || oidc_provider_arn.empty())
    {
        LOG_WARNING(Logger::get(), "Environment variables must be specified to use Alibaba Cloud OIDC creds provider");
        return nullptr;
    }
    return std::make_shared<OIDCCredentialsProvider>(region_id, role_arn, oidc_provider_arn, token_file);
}

OIDCCredentialsProvider::OIDCCredentialsProvider(
    const String & region_id,
    const String & role_arn,
    const String & oidc_provider_arn,
    const String & token_file)
    : m_oidc_provider_arn(oidc_provider_arn)
    , m_region_id(region_id)
    , m_role_arn(role_arn)
    , m_token_file(token_file)
    , m_initialized(false)
    , log(Logger::get())
{
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
    m_limiter = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>("OIDCCredentialsProvider", 200000);
    m_initialized = true;
    LOG_INFO(log, "Creating Alibaba Cloud OIDC creds provider");
}

Aws::Auth::AWSCredentials OIDCCredentialsProvider::GetAWSCredentials()
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

Aws::String OIDCCredentialsProvider::calculateQueryString() const
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

void OIDCCredentialsProvider::Reload()
{
    LOG_INFO(
        log,
        "Credentials have expired, attempting to renew from Alibaba Cloud OIDC, role_arn={} role_session_name={}",
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
        LOG_ERROR(
            log,
            "Failed to send request to Alibaba Cloud OIDC creds provider, role_arn={} role_session_name={} code={}",
            m_role_arn,
            m_session_name,
            magic_enum::enum_name(response->GetResponseCode()));
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
            LOG_ERROR(log, "Failed to parse response from Alibaba Cloud OIDC creds provider");
            return;
        }
        auto credentials = obj->getObject("Credentials");
        auto access_key_id = credentials->getValue<Aws::String>("AccessKeyId");
        auto secret_access_key = credentials->getValue<Aws::String>("AccessKeySecret");
        auto security_token = credentials->getValue<Aws::String>("SecurityToken");
        auto expiration = credentials->getValue<Aws::String>("Expiration");
        Aws::Utils::DateTime expiration_time(expiration, Aws::Utils::DateFormat::ISO_8601);

        LOG_INFO(
            log,
            "Successfully retrieved credentials from Alibaba Cloud OIDC, role_arn={} role_session_name={}",
            m_role_arn,
            m_session_name);
        m_credentials = Aws::Auth::AWSCredentials(access_key_id, secret_access_key, security_token, expiration_time);
    }
    catch (const Poco::Exception & e)
    {
        LOG_ERROR(log, "Failed to parse response from Alibaba Cloud OIDC, {}", e.displayText());
        return;
    }
}

bool OIDCCredentialsProvider::expiresSoon() const
{
    return (
        (m_credentials.GetExpiration() - Aws::Utils::DateTime::Now()).count()
        < CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD);
}

void OIDCCredentialsProvider::refreshIfExpired()
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

/// ECSRAMRoleCredentialsProvider ///
namespace details
{
bool isTruthy(const String & str)
{
    auto normalized_str = Poco::toLower(str);
    return str == "1" || str == "true" || str == "yes" || str == "on";
}

static const std::string_view IMDS_BASE_URL("http://100.100.100.200");
} // namespace details

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> ECSRAMRoleCredentialsProvider::build()
{
    String tmp_ecs_metadata_disabled = Aws::Environment::GetEnv("ALIBABA_CLOUD_ECS_METADATA_DISABLED");
    if (!tmp_ecs_metadata_disabled.empty() && details::isTruthy(tmp_ecs_metadata_disabled))
    {
        LOG_WARNING(Logger::get(), "IMDS is disabled for Alibaba Cloud IMDS creds provider");
        return nullptr;
    }
    return std::make_shared<ECSRAMRoleCredentialsProvider>();
}

ECSRAMRoleCredentialsProvider::ECSRAMRoleCredentialsProvider()
    : m_disable_imdsv1(false)
    , m_initialized(false)
    , log(Logger::get())
{
    String tmp_imdsv1_disabled = Aws::Environment::GetEnv("ALIBABA_CLOUD_IMDSV1_DISABLED");
    if (!tmp_imdsv1_disabled.empty() && details::isTruthy(tmp_imdsv1_disabled))
    {
        m_disable_imdsv1 = true;
        LOG_INFO(log, "IMDSv1 is disabled for Alibaba Cloud IMDS creds provider");
    }

    m_role_name = Aws::Environment::GetEnv("ALIBABA_CLOUD_ECS_METADATA");

    auto poco_cfg = PocoHTTPClientConfiguration(std::make_shared<RemoteHostFilter>(), 1, false, true);
    m_http_client_factory = std::make_shared<PocoHTTPClientFactory>(poco_cfg);
    m_limiter = std::make_shared<Aws::Utils::RateLimits::DefaultRateLimiter<>>(200000);
    m_initialized = true;
    LOG_INFO(log, "Creating Alibaba Cloud IMDS creds provider");
}

Aws::Auth::AWSCredentials ECSRAMRoleCredentialsProvider::GetAWSCredentials()
{
    if (!m_initialized)
    {
        return Aws::Auth::AWSCredentials();
    }
    refreshIfExpired();
    Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
    return m_credentials;
}

bool ECSRAMRoleCredentialsProvider::expiresSoon() const
{
    return (
        (m_credentials.GetExpiration() - Aws::Utils::DateTime::Now()).count()
        < CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD);
}

void ECSRAMRoleCredentialsProvider::refreshIfExpired()
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

std::optional<Aws::String> ECSRAMRoleCredentialsProvider::getMetadataToken()
{
    // PUT http://100.100.100.200/latest/api/token
    String url = fmt::format("{}/latest/api/token", details::IMDS_BASE_URL);

    auto http_client = m_http_client_factory->CreateHttpClient(Aws::Client::ClientConfiguration());
    auto http_request = m_http_client_factory->CreateHttpRequest(
        url,
        Aws::Http::HttpMethod::HTTP_PUT,
        Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
    static const Int64 DEFAULT_METADATA_TOKEN_TTL_SECS = 21600; // 6 hours
    http_request->SetHeaderValue(
        "X-aliyun-ecs-metadata-token-ttl-seconds",
        fmt::format("{}", DEFAULT_METADATA_TOKEN_TTL_SECS));

    // Try IMDSv2 first; if it fails, fall back to IMDSv1 unless explicitly disabled.
    auto response = http_client->MakeRequest(http_request, m_limiter.get(), m_limiter.get());
    if (response->GetResponseCode() == Aws::Http::HttpResponseCode::OK)
    {
        auto body = Aws::String(Aws::IStreamBufIterator(response->GetResponseBody()), Aws::IStreamBufIterator());
        return Poco::trim(body);
    }
    // IMDSv2 failed
    assert(response->GetResponseCode() != Aws::Http::HttpResponseCode::OK);
    if (m_disable_imdsv1)
    {
        auto body = Aws::String(Aws::IStreamBufIterator(response->GetResponseBody()), Aws::IStreamBufIterator());
        LOG_ERROR(
            log,
            "Failed to get token from IMDS, code={} body={}",
            magic_enum::enum_name(response->GetResponseCode()),
            body);
        return std::nullopt;
    }
    // else return an empty token for fall back to IMDSv1
    return "";
}

std::optional<Aws::String> ECSRAMRoleCredentialsProvider::getRoleName()
{
    if (likely(!m_role_name.empty()))
    {
        return m_role_name;
    }

    String url = fmt::format("{}/latest/meta-data/ram/security-credentials/", details::IMDS_BASE_URL);
    auto http_client = m_http_client_factory->CreateHttpClient(Aws::Client::ClientConfiguration());
    auto http_request = m_http_client_factory->CreateHttpRequest(
        url,
        Aws::Http::HttpMethod::HTTP_GET,
        Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
    std::optional<String> token = getMetadataToken();
    if (!token.has_value())
    {
        return std::nullopt;
    }
    else if (!token->empty())
    {
        // If token is not empty, it means IMDSv2 is used.
        // Else IMDSv1 is used.
        http_request->SetHeaderValue("X-aliyun-ecs-metadata-token", fmt::format("{}", token.value()));
    }

    auto response = http_client->MakeRequest(http_request, m_limiter.get(), m_limiter.get());
    if (response->GetResponseCode() != Aws::Http::HttpResponseCode::OK)
    {
        LOG_ERROR(
            log,
            "Failed to get IMDSv2 token from IMDS, token={} code={}",
            token,
            magic_enum::enum_name(response->GetResponseCode()));
        return std::nullopt;
    }
    auto body = Aws::String(Aws::IStreamBufIterator(response->GetResponseBody()), Aws::IStreamBufIterator());
    return Poco::trim(body);
}

void ECSRAMRoleCredentialsProvider::Reload()
{
    LOG_INFO(
        log,
        "Credentials have expired, attempting to renew from Alibaba Cloud IMDS, role_name={} disable_imdsv1={}",
        m_role_name,
        m_disable_imdsv1);

    std::optional<String> opt_role_name = getRoleName();
    if (!opt_role_name.has_value() || opt_role_name->empty())
    {
        LOG_ERROR(log, "Failed to get role_name from Alibaba Cloud IMDS");
        return;
    }

    String role_name = opt_role_name.value(); // extract the role name from optional
    String url = fmt::format("{}/latest/meta-data/ram/security-credentials/{}", details::IMDS_BASE_URL, role_name);
    auto http_client = m_http_client_factory->CreateHttpClient(Aws::Client::ClientConfiguration());
    auto http_request = m_http_client_factory->CreateHttpRequest(
        url,
        Aws::Http::HttpMethod::HTTP_GET,
        Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
    std::optional<String> token = getMetadataToken();
    if (!token.has_value())
    {
        return;
    }
    else if (!token->empty())
    {
        // If token is not empty, it means IMDSv2 is used.
        // Else IMDSv1 is used.
        http_request->SetHeaderValue("X-aliyun-ecs-metadata-token", fmt::format("{}", token.value()));
    }

    auto response = http_client->MakeRequest(http_request, m_limiter.get(), m_limiter.get());
    if (response->GetResponseCode() != Aws::Http::HttpResponseCode::OK)
    {
        LOG_ERROR(
            log,
            "Failed to send request to Alibaba Cloud IMDS, role_name={} code={}",
            role_name,
            magic_enum::enum_name(response->GetResponseCode()));
        return;
    }

    // parse http response
    auto body = Aws::String(Aws::IStreamBufIterator(response->GetResponseBody()), Aws::IStreamBufIterator());
    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(body);
        const auto & obj = result.extract<Poco::JSON::Object::Ptr>();
        if (!obj->has("Code"))
        {
            LOG_ERROR(log, "Failed to parse response from Alibaba Cloud IMDS, resp_body={}", body);
            return;
        }
        auto code = obj->getValue<Aws::String>("Code");
        if (code != "Success")
        {
            LOG_ERROR(log, "Failed to get valid credentials from Alibaba Cloud IMDS, code={} resp_body={}", code, body);
            return;
        }

        if (!obj->has("AccessKeyId") || !obj->has("AccessKeySecret") || !obj->has("SecurityToken")
            || !obj->has("Expiration"))
        {
            LOG_ERROR(log, "Failed to get valid credentials from Alibaba Cloud IMDS, resp_body={}", body);
            return;
        }
        auto access_key_id = obj->getValue<Aws::String>("AccessKeyId");
        auto secret_access_key = obj->getValue<Aws::String>("AccessKeySecret");
        auto security_token = obj->getValue<Aws::String>("SecurityToken");
        auto expiration = obj->getValue<Aws::String>("Expiration");
        Aws::Utils::DateTime expiration_time(expiration, Aws::Utils::DateFormat::ISO_8601);

        LOG_INFO(log, "Successfully retrieved credentials from Alibaba Cloud IMDS, role_name={}", role_name);
        m_credentials = Aws::Auth::AWSCredentials(access_key_id, secret_access_key, security_token, expiration_time);
    }
    catch (const Poco::Exception & e)
    {
        LOG_ERROR(log, "Failed to parse response from Alibaba Cloud IMDS, {}", e.displayText());
        return;
    }
}

} // namespace DB::S3::AlibabaCloud
