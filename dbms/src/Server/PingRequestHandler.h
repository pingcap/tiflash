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

#include "IServer.h"

#include <Poco/Net/HTTPRequestHandler.h>


namespace DB
{

/// Response with "Ok.\n". Used for availability checks.
class PingRequestHandler : public Poco::Net::HTTPRequestHandler
{
<<<<<<< HEAD:dbms/src/Server/PingRequestHandler.h
private:
    IServer & server;

public:
    explicit PingRequestHandler(IServer & server_) : server(server_)
    {
    }
=======
struct DataTypeWithTypeName
{
    DataTypeWithTypeName(const DataTypePtr & t, const String & n)
        : type(t)
        , name(n)
    {}
>>>>>>> 6638f2067b (Fix license and format coding style (#7962)):dbms/src/Flash/Coprocessor/CodecUtils.h

    void handleRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response) override;
};

}
