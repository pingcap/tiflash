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

#pragma once

#include <Common/Logger.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/http/HttpClientFactory.h>

namespace DB::S3::AlibabaCloud
{
//  AWSCredentialsProvider for Alibaba Cloud using OIDC
class OIDCCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    OIDCCredentialsProvider();

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

private:
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

// AWSCredentialsProvider for Alibaba Cloud using ECS RAM Role
class ECSRAMRoleCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    ECSRAMRoleCredentialsProvider();

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

    std::optional<Aws::String> getMetadataToken();
    std::optional<Aws::String> getRoleName();

private:
    Aws::Auth::AWSCredentials m_credentials;
    Aws::String m_role_name;
    bool m_disable_imdsv1;
    bool m_initialized;
    std::shared_ptr<Aws::Http::HttpClientFactory> m_http_client_factory;
    std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> m_limiter;
    LoggerPtr log;
};
} // namespace DB::S3::AlibabaCloud
