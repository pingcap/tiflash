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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#pragma GCC diagnostic pop

#include <Storages/S3/Credentials.h>
#include <Storages/S3/CredentialsAliCloud.h>
#include <Storages/S3/PocoHTTPClientFactory.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/UUID.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <common/logger_useful.h>

namespace DB::S3::AlibabaCloud
{

OIDCCredentialsProvider::OIDCCredentialsProvider()
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
    m_limiter = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>("OIDCCredentialsProvider", 200000);
    m_initialized = true;
    LOG_INFO(log, "Creating Alibaba Cloud STS AssumeRole with web identity creds provider.");
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

bool OIDCCredentialsProvider::expiresSoon() const
{
    return (
        (m_credentials.GetExpiration() - Aws::Utils::DateTime::Now()).count()
        < STS_CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD);
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

} // namespace DB::S3::AlibabaCloud
