// Copyright 2022 PingCAP, Ltd.
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
#include <Common/setThreadName.h>
#include <Poco/Net/HTTPServer.h>

namespace DB
{
struct HTTPServer : Poco::Net::HTTPServer
{
    HTTPServer(Poco::Net::HTTPRequestHandlerFactory::Ptr pFactory, Poco::ThreadPool & threadPool, const Poco::Net::ServerSocket & socket, Poco::Net::HTTPServerParams::Ptr pParams)
        : Poco::Net::HTTPServer(pFactory, threadPool, socket, pParams)
    {}

protected:
    void run() override
    {
        setThreadName("HTTPServer");
        Poco::Net::HTTPServer::run();
    }
};
}; // namespace DB