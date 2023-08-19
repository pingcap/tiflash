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

#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <common/logger_useful.h>

#include "HTTPHandler.h"
#include "IServer.h"
#include "NotFoundHandler.h"
#include "PingRequestHandler.h"
#include "RootRequestHandler.h"


namespace DB
{
template <typename HandlerType>
class HTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
private:
    IServer & server;
    Poco::Logger * log;
    std::string name;

public:
    HTTPRequestHandlerFactory(IServer & server_, const std::string & name_)
        : server(server_)
        , log(&Poco::Logger::get(name_))
        , name(name_)
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override
    {
        LOG_TRACE(
            log,
            "HTTP Request for {}. Method: {}, Address: {}, User-Agent: {}",
            name,
            request.getMethod(),
            request.clientAddress().toString(),
            (request.has("User-Agent") ? request.get("User-Agent") : "none"));

        const auto & uri = request.getURI();

        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD)
        {
            if (uri == "/")
                return new RootRequestHandler(server);
            if (uri == "/ping")
                return new PingRequestHandler(server);
        }

        if (uri.find('?') != std::string::npos || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
        {
            return new HandlerType(server);
        }

        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD
            || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
        {
            return new NotFoundHandler;
        }

        return nullptr;
    }
};

using HTTPHandlerFactory = HTTPRequestHandlerFactory<HTTPHandler>;

} // namespace DB
