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

#pragma once

#include <Common/RemoteHostFilter.h>
#include <Poco/Net/HTTPClientSession.h>
// #include <Common/Throttler_fwd.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/HTTPHeaderEntries.h>
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

namespace DB
{

class Context;
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

using PocoHTTPClientConfiguration = Aws::Client::ClientConfiguration;

class PocoHTTPResponse : public Aws::Http::Standard::StandardHttpResponse
{
public:
    using SessionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

    explicit PocoHTTPResponse(const std::shared_ptr<const Aws::Http::HttpRequest> request)
        : Aws::Http::Standard::StandardHttpResponse(request)
        , body_stream(request->GetResponseStreamFactory())
    {
    }

    void SetResponseBody(Aws::IStream & incoming_stream, SessionPtr & session_) /// NOLINT
    {
        body_stream = Aws::Utils::Stream::ResponseStream(
            Aws::New<SessionAwareIOStream<SessionPtr>>("http result streambuf", session_, incoming_stream.rdbuf()));
    }

    void SetResponseBody(std::string & response_body) /// NOLINT
    {
        auto * stream = Aws::New<std::stringstream>("http result buf", response_body); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        stream->exceptions(std::ios::failbit);
        body_stream = Aws::Utils::Stream::ResponseStream(std::move(stream));
    }

    Aws::IOStream & GetResponseBody() const override
    {
        return body_stream.GetUnderlyingStream();
    }

    Aws::Utils::Stream::ResponseStream && SwapResponseStreamOwnership() override
    {
        return std::move(body_stream);
    }

private:
    Aws::Utils::Stream::ResponseStream body_stream;
};

class PocoHTTPClient : public Aws::Http::HttpClient
{
public:
    explicit PocoHTTPClient(const Aws::Client::ClientConfiguration & client_configuration);
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
    void addMetric(const Aws::Http::HttpRequest & request, S3MetricType type, ProfileEvents::Count amount = 1) const;

    std::function<ClientConfigurationPerRequest(const Aws::Http::HttpRequest &)> per_request_configuration;
    std::function<void(const ClientConfigurationPerRequest &)> error_report;
    ConnectionTimeouts timeouts;
    //const RemoteHostFilter & remote_host_filter;
    static constexpr unsigned int s3_max_redirects = 3;
    //bool enable_s3_requests_logging;
    //bool for_disk_s3;

    /// Limits get request per second rate for GET, SELECT and all other requests, excluding throttled by put throttler
    /// (i.e. throttles GetObject, HeadObject)
    // ThrottlerPtr get_request_throttler;

    /// Limits put request per second rate for PUT, COPY, POST, LIST requests
    /// (i.e. throttles PutObject, CopyObject, ListObjects, CreateMultipartUpload, UploadPartCopy, UploadPart, CompleteMultipartUpload)
    /// NOTE: DELETE and CANCEL requests are not throttled by either put or get throttler
    // ThrottlerPtr put_request_throttler;

    const HTTPHeaderEntries extra_headers;
};

} // namespace DB::S3
