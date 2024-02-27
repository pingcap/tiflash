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

#include <Common/CurrentMetrics.h>
#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/Throttler.h>
#include <Common/TiFlashMetrics.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <Storages/S3/PocoHTTPClient.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/Scheme.h>
#include <aws/core/monitoring/HttpClientMetrics.h>
#include <aws/core/utils/ratelimiter/RateLimiterInterface.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <boost_wrapper/string.h>
#include <common/logger_useful.h>
#include <re2/re2.h>

#include <algorithm>
#include <functional>
#include <utility>

static const int SUCCESS_RESPONSE_MIN = 200;
static const int SUCCESS_RESPONSE_MAX = 299;

namespace ProfileEvents
{
extern const Event S3ReadMicroseconds;
extern const Event S3ReadRequestsCount;
extern const Event S3ReadRequestsErrors;
extern const Event S3ReadRequestsThrottling;
extern const Event S3ReadRequestsRedirects;

extern const Event S3WriteMicroseconds;
extern const Event S3WriteRequestsCount;
extern const Event S3WriteRequestsErrors;
extern const Event S3WriteRequestsThrottling;
extern const Event S3WriteRequestsRedirects;

extern const Event S3GetRequestThrottlerCount;
extern const Event S3GetRequestThrottlerSleepMicroseconds;
extern const Event S3PutRequestThrottlerCount;
extern const Event S3PutRequestThrottlerSleepMicroseconds;
} // namespace ProfileEvents

namespace CurrentMetrics
{
extern const Metric S3Requests;
}

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int TOO_MANY_REDIRECTS;
} // namespace DB::ErrorCodes

namespace DB::S3
{

PocoHTTPClientConfiguration::PocoHTTPClientConfiguration(
    const std::shared_ptr<RemoteHostFilter> & remote_host_filter_,
    UInt32 s3_max_redirects_,
    bool enable_s3_requests_logging_,
    bool enable_session_pool_)
    : remote_host_filter(remote_host_filter_)
    , s3_max_redirects(s3_max_redirects_)
    , enable_s3_requests_logging(enable_s3_requests_logging_)
    , enable_session_pool(enable_session_pool_)
    , error_report(nullptr)
{}

PocoHTTPClient::PocoHTTPClient(
    const Aws::Client::ClientConfiguration & client_configuration,
    const PocoHTTPClientConfiguration & poco_configuration)
    : per_request_configuration(poco_configuration.per_request_configuration)
    , error_report(poco_configuration.error_report)
    , timeouts(ConnectionTimeouts(
          Poco::Timespan(client_configuration.connectTimeoutMs * 1000), /// connection timeout.
          Poco::Timespan(client_configuration.requestTimeoutMs * 1000), /// send timeout.
          Poco::Timespan(client_configuration.requestTimeoutMs * 1000) /// receive timeout.
          ))
    , remote_host_filter(poco_configuration.remote_host_filter)
    , extra_headers(poco_configuration.extra_headers)
    , s3_max_redirects(poco_configuration.s3_max_redirects)
    , enable_s3_requests_logging(poco_configuration.enable_s3_requests_logging)
    , enable_session_pool(poco_configuration.enable_session_pool)
{}

std::shared_ptr<Aws::Http::HttpResponse> PocoHTTPClient::MakeRequest(
    const std::shared_ptr<Aws::Http::HttpRequest> & request,
    Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const
{
    try
    {
        auto response = Aws::MakeShared<PocoHTTPResponse>("PocoHTTPClient", request);
        makeRequestInternal(*request, response, readLimiter, writeLimiter);
        return response;
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }
}

namespace
{
/// No comments:
/// 1) https://aws.amazon.com/premiumsupport/knowledge-center/s3-resolve-200-internalerror/
/// 2) https://github.com/aws/aws-sdk-cpp/issues/658
bool checkRequestCanReturn2xxAndErrorInBody(Aws::Http::HttpRequest & request)
{
    auto query_params = request.GetQueryStringParameters();
    if (request.HasHeader("x-amz-copy-source"))
    {
        /// CopyObject https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
        if (query_params.empty())
            return true;

        /// UploadPartCopy https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
        if (query_params.contains("partNumber") && query_params.contains("uploadId"))
            return true;
    }
    else
    {
        /// CompleteMultipartUpload https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
        if (query_params.size() == 1 && query_params.contains("uploadId"))
            return true;
    }

    return false;
}
} // namespace

PocoHTTPClient::S3MetricKind PocoHTTPClient::getMetricKind(const Aws::Http::HttpRequest & request)
{
    switch (request.GetMethod())
    {
    case Aws::Http::HttpMethod::HTTP_GET:
    case Aws::Http::HttpMethod::HTTP_HEAD:
        return S3MetricKind::Read;
    case Aws::Http::HttpMethod::HTTP_POST:
    case Aws::Http::HttpMethod::HTTP_DELETE:
    case Aws::Http::HttpMethod::HTTP_PUT:
    case Aws::Http::HttpMethod::HTTP_PATCH:
        return S3MetricKind::Write;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported request method");
}

void PocoHTTPClient::addMetric(const Aws::Http::HttpRequest & request, S3MetricType type, ProfileEvents::Count amount)
{
    const ProfileEvents::Event events_map[static_cast<size_t>(S3MetricType::EnumSize)]
                                         [static_cast<size_t>(S3MetricKind::EnumSize)]
        = {
            {ProfileEvents::S3ReadMicroseconds, ProfileEvents::S3WriteMicroseconds},
            {ProfileEvents::S3ReadRequestsCount, ProfileEvents::S3WriteRequestsCount},
            {ProfileEvents::S3ReadRequestsErrors, ProfileEvents::S3WriteRequestsErrors},
            {ProfileEvents::S3ReadRequestsThrottling, ProfileEvents::S3WriteRequestsThrottling},
            {ProfileEvents::S3ReadRequestsRedirects, ProfileEvents::S3WriteRequestsRedirects},
        };

    S3MetricKind kind = getMetricKind(request);

    ProfileEvents::increment(events_map[static_cast<unsigned int>(type)][static_cast<unsigned int>(kind)], amount);
}

void PocoHTTPClient::makeRequestInternal(
    Aws::Http::HttpRequest & request,
    std::shared_ptr<PocoHTTPResponse> & response,
    Aws::Utils::RateLimits::RateLimiterInterface *,
    Aws::Utils::RateLimits::RateLimiterInterface *) const
{
    auto uri = request.GetUri().GetURIString();
    LoggerPtr tracing_logger = Logger::get(fmt::format("URI={} pool={}", uri, enable_session_pool));
    if (enable_s3_requests_logging)
        LOG_DEBUG(tracing_logger, "Make request");

    // TODO: Support Throttler

    addMetric(request, S3MetricType::Count);
    CurrentMetrics::Increment metric_increment{CurrentMetrics::S3Requests};

    try
    {
        for (UInt32 attempt = 0; attempt <= s3_max_redirects; ++attempt)
        {
            Poco::URI target_uri(uri);
            auto request_configuration = per_request_configuration(request);
            RUNTIME_CHECK_MSG(
                request_configuration.proxy_host.empty(),
                "http proxy is not supported, proxy_host={}",
                request_configuration.proxy_host);

            std::optional<String> redirect_uri;
            if (enable_session_pool)
            {
                PooledHTTPSessionPtr pooled_session = makePooledHTTPSession(
                    target_uri,
                    timeouts,
                    /* per_endpoint_pool_size = */ 1024,
                    /* resolve_host = */ true);
                redirect_uri = makeRequestOnce(
                    target_uri,
                    request,
                    request_configuration,
                    pooled_session,
                    response,
                    tracing_logger);
            }
            else
            {
                HTTPSessionPtr session = makeHTTPSession(target_uri, timeouts, /* resolve_host = */ true);
                redirect_uri
                    = makeRequestOnce(target_uri, request, request_configuration, session, response, tracing_logger);
            }

            if (redirect_uri)
            {
                // redirect happens, update the uri and try again
                uri = redirect_uri.value();
                continue;
            }
            else
            {
                // request finish normally
                return;
            }
        }
        // too may redirects
        throw Exception(
            ErrorCodes::TOO_MANY_REDIRECTS,
            "Too many redirects while trying to access {}",
            request.GetUri().GetURIString());
    }
    catch (...)
    {
        tryLogCurrentException(tracing_logger, fmt::format("Failed to make request to: {}", uri));
        response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
        response->SetClientErrorMessage(getCurrentExceptionMessage(false));

        addMetric(request, S3MetricType::Errors);

        /// Probably this is socket timeout or something more or less related to DNS
        /// Let's just remove this host from DNS cache to be more safe
        DNSResolver::instance().removeHostFromCache(Poco::URI(uri).getHost());
    }
}

template <typename Session>
std::optional<String> PocoHTTPClient::makeRequestOnce(
    const Poco::URI & target_uri,
    Aws::Http::HttpRequest & request,
    const ClientConfigurationPerRequest & request_configuration,
    Session session,
    std::shared_ptr<PocoHTTPResponse> & response,
    const LoggerPtr & tracing_logger) const
{
    /// In case of error this address will be written to logs
    request.SetResolvedRemoteHost(session->getResolvedAddress());

    Poco::Net::HTTPRequest poco_request(Poco::Net::HTTPRequest::HTTP_1_1);

    /** According to RFC-2616, Request-URI is allowed to be encoded.
     * However, there is no clear agreement on which exact symbols must be encoded.
     * Effectively, `Poco::URI` chooses smaller subset of characters to encode,
     * whereas Amazon S3 and Google Cloud Storage expects another one.
     * In order to successfully execute a request, a path must be exact representation
     * of decoded path used by `AWSAuthSigner`.
     * Therefore we shall encode some symbols "manually" to fit the signatures.
     */

    std::string path_and_query;
    const std::string & query = target_uri.getRawQuery();
    const std::string reserved = "?#:;+@&=%"; /// Poco::URI::RESERVED_QUERY_PARAM without '/' plus percent sign.
    Poco::URI::encode(target_uri.getPath(), reserved, path_and_query);

    /// `target_uri.getPath()` could return an empty string, but a proper HTTP request must
    /// always contain a non-empty URI in its first line (e.g. "POST / HTTP/1.1" or "POST /?list-type=2 HTTP/1.1").
    if (path_and_query.empty())
        path_and_query = "/";

    /// Append the query param to URI
    if (!query.empty())
    {
        path_and_query += '?';
        path_and_query += query;
    }

    poco_request.setURI(path_and_query);

    switch (request.GetMethod())
    {
    case Aws::Http::HttpMethod::HTTP_GET:
        poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_GET);
        break;
    case Aws::Http::HttpMethod::HTTP_POST:
        poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_POST);
        break;
    case Aws::Http::HttpMethod::HTTP_DELETE:
        poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_DELETE);
        break;
    case Aws::Http::HttpMethod::HTTP_PUT:
        poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_PUT);
        break;
    case Aws::Http::HttpMethod::HTTP_HEAD:
        poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_HEAD);
        break;
    case Aws::Http::HttpMethod::HTTP_PATCH:
        poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_PATCH);
        break;
    }

    /// Headers coming from SDK are lower-cased.
    for (const auto & [header_name, header_value] : request.GetHeaders())
        poco_request.set(header_name, header_value);
    for (const auto & [header_name, header_value] : extra_headers)
        poco_request.set(boost::algorithm::to_lower_copy(header_name), header_value);

    Poco::Net::HTTPResponse poco_response;

    Stopwatch watch;

    Poco::Net::HTTPSendMetrics metrics;
    auto & request_body_stream = session->sendRequest(poco_request, &metrics);
    GET_METRIC(tiflash_storage_s3_http_request_seconds, type_connect).Observe(metrics.connect_ms / 1000.0);

    if (request.GetContentBody())
    {
        if (enable_s3_requests_logging)
            LOG_DEBUG(tracing_logger, "Writing request body.");

        /// Rewind content body buffer.
        /// NOTE: we should do that always (even if `attempt == 0`) because the same request can be retried also by AWS,
        /// see retryStrategy in Aws::Client::ClientConfiguration.
        request.GetContentBody()->clear();
        request.GetContentBody()->seekg(0);

        auto size = Poco::StreamCopier::copyStream(*request.GetContentBody(), request_body_stream);
        if (enable_s3_requests_logging)
            LOG_DEBUG(tracing_logger, "Written {} bytes to request body", size);
    }

    const auto elapsed_ms_connect_and_send = watch.elapsedMilliseconds();
    GET_METRIC(tiflash_storage_s3_http_request_seconds, type_request)
        .Observe((elapsed_ms_connect_and_send - metrics.connect_ms) / 1000.0);

    if (enable_s3_requests_logging)
        LOG_DEBUG(tracing_logger, "Receiving response...");
    auto & response_body_stream = session->receiveResponse(poco_response);
    GET_METRIC(tiflash_storage_s3_http_request_seconds, type_response)
        .Observe(watch.elapsedMillisecondsFromLastTime() / 1000.0);

    int status_code = static_cast<int>(poco_response.getStatus());
    if (status_code >= SUCCESS_RESPONSE_MIN && status_code <= SUCCESS_RESPONSE_MAX)
    {
        if (enable_s3_requests_logging)
            LOG_DEBUG(tracing_logger, "Response status: {}, {}", status_code, poco_response.getReason());
    }
    else
    {
        /// Error statuses are more important so we show them even if `enable_s3_requests_logging == false`.
        LOG_INFO(tracing_logger, "Response status: {}, {}", status_code, poco_response.getReason());
    }
    if (poco_response.getStatus() == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT
        || poco_response.getStatus() == Poco::Net::HTTPResponse::HTTP_FOUND)
    {
        auto location = poco_response.get("location");
        remote_host_filter->checkURL(Poco::URI(location));
        if (enable_s3_requests_logging)
            LOG_DEBUG(tracing_logger, "Redirecting request to new location: {}", location);

        addMetric(request, S3MetricType::Redirects);

        // Return the redirection location, upper should retry the request with `location`
        return location;
    }

    response->SetResponseCode(static_cast<Aws::Http::HttpResponseCode>(status_code));
    response->SetContentType(poco_response.getContentType());

    if (enable_s3_requests_logging)
    {
        WriteBufferFromOwnString headers_ss;
        for (const auto & [header_name, header_value] : poco_response)
        {
            response->AddHeader(header_name, header_value);
            headers_ss << header_name << ": " << header_value << "; ";
        }
        LOG_DEBUG(tracing_logger, "Received headers: {}", headers_ss.str());
    }
    else
    {
        for (const auto & [header_name, header_value] : poco_response)
            response->AddHeader(header_name, header_value);
    }

    /// Request is successful but for some special requests we can have actual error message in body
    if (status_code >= SUCCESS_RESPONSE_MIN && status_code <= SUCCESS_RESPONSE_MAX
        && checkRequestCanReturn2xxAndErrorInBody(request))
    {
        std::string response_string(
            (std::istreambuf_iterator<char>(response_body_stream)),
            std::istreambuf_iterator<char>());

        /// Just trim string so it will not be so long
        LOG_TRACE(
            tracing_logger,
            "Got dangerous response with successful code {}, checking its body: '{}'",
            status_code,
            response_string.substr(0, 300));
        const static std::string_view needle = "<Error>";
        if (auto it = std::search(
                response_string.begin(),
                response_string.end(),
                std::default_searcher(needle.begin(), needle.end()));
            it != response_string.end())
        {
            LOG_WARNING(
                tracing_logger,
                "Response for request contain <Error> tag in body, settings internal server error (500 code)");
            response->SetResponseCode(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR);

            addMetric(request, S3MetricType::Errors);
            if (error_report)
                error_report(request_configuration);
        }

        /// Set response from string
        response->SetResponseBody(response_string);
    }
    else
    {
        if (status_code == 429 || status_code == 503)
        { // API throttling
            addMetric(request, S3MetricType::Throttling);
        }
        else if (status_code >= 300)
        {
            addMetric(request, S3MetricType::Errors);
            if (status_code >= 500 && error_report)
                error_report(request_configuration);
        }
        response->SetResponseBody(response_body_stream, session);
    }
    return std::nullopt;
}

} // namespace DB::S3
