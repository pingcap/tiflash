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

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <common/logger_useful.h>

#include "IServer.h"
#include "TCPHandler.h"

namespace Poco
{
class Logger;
}

namespace DB
{
class TCPHandlerFactory : public Poco::Net::TCPServerConnectionFactory
{
private:
    IServer & server;
    Poco::Logger * log;

public:
    explicit TCPHandlerFactory(IServer & server_, bool secure_ = false)
        : server(server_)
        , log(&Poco::Logger::get(std::string("TCP") + (secure_ ? "S" : "") + "HandlerFactory"))
    {}

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
    {
        LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());

        return new TCPHandler(server, socket);
    }
};

} // namespace DB
