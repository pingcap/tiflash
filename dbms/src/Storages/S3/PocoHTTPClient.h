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

#pragma once

#include <Common/RemoteHostFilter.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/HTTPHeaderEntries.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Storages/S3/SessionAwareIOStream.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/standard/StandardHttpResponse.h>

#include <string>
#include <vector>

namespace Aws::Http::Standard
{
class StandardHttpResponse;
}

namespace DB::S3
{
class ClientFactory;

struct ClientConfigurationPerRequest
{
    Aws::Http::Scheme proxy_scheme = Aws::Http::Scheme::HTTPS;
    String proxy_host;
    unsigned proxy_port = 0;
};

struct PocoHTTPClientConfiguration
{
    std::function<ClientConfigurationPerRequest(const Aws::Http::HttpRequest &)> per_request_configuration
        = [](const Aws::Http::HttpRequest &) {
              return ClientConfigurationPerRequest();
          };
    std::shared_ptr<RemoteHostFilter> remote_host_filter;
    UInt32 s3_max_redirects;
    bool enable_s3_requests_logging;
    bool enable_session_pool = true;
    HTTPHeaderEntries extra_headers;

    std::function<void(const ClientConfigurationPerRequest &)> error_report;

    PocoHTTPClientConfiguration(
        const std::shared_ptr<RemoteHostFilter> & remote_host_filter_,
        UInt32 s3_max_redirects_,
        bool enable_s3_requests_logging_,
        bool enable_session_pool_);

    /// Constructor of Aws::Client::ClientConfiguration must be called after AWS SDK initialization.
    friend ClientFactory;
};

class PocoHTTPResponse : public Aws::Http::Standard::StandardHttpResponse
{
public:
    using SessionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

    explicit PocoHTTPResponse(const std::shared_ptr<const Aws::Http::HttpRequest> request)
        : Aws::Http::Standard::StandardHttpResponse(request)
        , body_stream(request->GetResponseStreamFactory())
    {}

    void SetResponseBody(Aws::IStream & incoming_stream, SessionPtr & session_) /// NOLINT
    {
        body_stream = Aws::Utils::Stream::ResponseStream(
            Aws::New<SessionAwareIOStream<SessionPtr>>("http result streambuf", session_, incoming_stream.rdbuf()));
    }

    void SetResponseBody(Aws::IStream & incoming_stream, PooledHTTPSessionPtr & session_) /// NOLINT
    {
        body_stream = Aws::Utils::Stream::ResponseStream(Aws::New<SessionAwareIOStream<PooledHTTPSessionPtr>>(
            "http result streambuf",
            session_,
            incoming_stream.rdbuf()));
    }

    void SetResponseBody(std::string & response_body) /// NOLINT
    {
        auto * stream
            = Aws::New<std::stringstream>("http result buf", response_body); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        stream->exceptions(std::ios::failbit);
        body_stream = Aws::Utils::Stream::ResponseStream(std::move(stream));
    }

    Aws::IOStream & GetResponseBody() const override { return body_stream.GetUnderlyingStream(); }

    Aws::Utils::Stream::ResponseStream && SwapResponseStreamOwnership() override { return std::move(body_stream); }

private:
    Aws::Utils::Stream::ResponseStream body_stream;
};

// A HTTP client base on Poco
// TODO: Support Throttler
class PocoHTTPClient : public Aws::Http::HttpClient
{
public:
    explicit PocoHTTPClient(
        const Aws::Client::ClientConfiguration & client_configuration,
        const PocoHTTPClientConfiguration & poco_configuration);
    ~PocoHTTPClient() override = default;

    std::shared_ptr<Aws::Http::HttpResponse> MakeRequest(
        const std::shared_ptr<Aws::Http::HttpRequest> & request,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const override;

private:
    void makeRequestInternal(
        Aws::Http::HttpRequest & request,
        std::shared_ptr<PocoHTTPResponse> & response,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const;

    template <typename Session>
    std::optional<String> makeRequestOnce(
        const Poco::URI & target_uri,
        Aws::Http::HttpRequest & request,
        const ClientConfigurationPerRequest & request_configuration,
        Session session,
        std::shared_ptr<PocoHTTPResponse> & response,
        const LoggerPtr & tracing_logger) const;

    enum class S3MetricType
    {
        Microseconds,
        Count,
        Errors,
        Throttling,
        Redirects,

        EnumSize,
    };

    enum class S3MetricKind
    {
        Read,
        Write,

        EnumSize,
    };

    static S3MetricKind getMetricKind(const Aws::Http::HttpRequest & request);
    static void addMetric(const Aws::Http::HttpRequest & request, S3MetricType type, ProfileEvents::Count amount = 1);

    std::function<ClientConfigurationPerRequest(const Aws::Http::HttpRequest &)> per_request_configuration;
    std::function<void(const ClientConfigurationPerRequest &)> error_report;
    ConnectionTimeouts timeouts;
    const std::shared_ptr<RemoteHostFilter> remote_host_filter;
    const HTTPHeaderEntries extra_headers;
    UInt32 s3_max_redirects;
    bool enable_s3_requests_logging;
    bool enable_session_pool;
};

} // namespace DB::S3
