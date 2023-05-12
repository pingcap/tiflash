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

#include <Common/PoolBase.h>
#include <IO/ConnectionTimeouts.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/URIStreamFactory.h>

#include <iostream>
#include <memory>
#include <mutex>


namespace DB
{


class HTTPException : public Exception
{
public:
    HTTPException(
        int code,
        const std::string & uri,
        Poco::Net::HTTPResponse::HTTPStatus http_status_,
        const std::string & reason,
        const std::string & body)
        : Exception(makeExceptionMessage(code, uri, http_status_, reason, body))
        , http_status(http_status_)
    {}

    HTTPException * clone() const override { return new HTTPException(*this); }
    void rethrow() const override { throw *this; }

    int getHTTPStatus() const { return http_status; }

private:
    Poco::Net::HTTPResponse::HTTPStatus http_status{};

    static Exception makeExceptionMessage(
        int code,
        const std::string & uri,
        Poco::Net::HTTPResponse::HTTPStatus http_status,
        const std::string & reason,
        const std::string & body);

    const char * name() const noexcept override { return "DB::HTTPException"; }
    const char * className() const noexcept override { return "DB::HTTPException"; }
};

using PooledHTTPSessionPtr = PoolBase<Poco::Net::HTTPClientSession>::Entry; // SingleEndpointHTTPSessionPool::Entry
using HTTPSessionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

void setResponseDefaultHeaders(Poco::Net::HTTPResponse & response, size_t keep_alive_timeout);

/// Create session object to perform requests and set required parameters.
HTTPSessionPtr makeHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts, bool resolve_host = true);

/// As previous method creates session, but tooks it from pool, without and with proxy uri.
PooledHTTPSessionPtr makePooledHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts, size_t per_endpoint_pool_size, bool resolve_host = true);
PooledHTTPSessionPtr makePooledHTTPSession(const Poco::URI & uri, const Poco::URI & proxy_uri, const ConnectionTimeouts & timeouts, size_t per_endpoint_pool_size, bool resolve_host = true);

bool isRedirect(Poco::Net::HTTPResponse::HTTPStatus status);

/** Used to receive response (response headers and possibly body)
  *  after sending data (request headers and possibly body).
  * Throws exception in case of non HTTP_OK (200) response code.
  * Returned istream lives in 'session' object.
  */
std::istream * receiveResponse(
    Poco::Net::HTTPClientSession & session,
    const Poco::Net::HTTPRequest & request,
    Poco::Net::HTTPResponse & response,
    bool allow_redirects);

void assertResponseIsOk(
    const Poco::Net::HTTPRequest & request,
    Poco::Net::HTTPResponse & response,
    std::istream & istr,
    bool allow_redirects = false);
} // namespace DB
