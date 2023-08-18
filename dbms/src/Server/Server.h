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

#include <daemon/BaseDaemon.h>
#include <Server/IServer.h>
#include <Server/ServerInfo.h>


/** Server provides three interfaces:
  * 1. HTTP - simple interface for any applications.
  * 2. TCP - interface for native clickhouse-client and for server to server internal communications.
  *    More rich and efficient, but less compatible
  *     - data is transferred by columns;
  *     - data is transferred compressed;
  *    Allows to get more information in response.
  */


namespace DB
{
class Server : public BaseDaemon
    , public IServer
{
public:
    Poco::Util::LayeredConfiguration & config() const override
    {
        return BaseDaemon::config();
    }

    virtual const TiFlashSecurityConfig & securityConfig() const override { return security_config; };

    Poco::Logger & logger() const override
    {
        return BaseDaemon::logger();
    }

    Context & context() const override
    {
        return *global_context;
    }

    bool isCancelled() const override
    {
        return BaseDaemon::isCancelled();
    }

protected:
    void initialize(Application & self) override;

    void uninitialize() override;

    int main(const std::vector<std::string> & args) override;

    std::string getDefaultCorePath() const override;

private:
    std::unique_ptr<Context> global_context;

    TiFlashSecurityConfig security_config;

    ServerInfo server_info;

    class FlashGrpcServerHolder;
    class TcpHttpServersHolder;
};

} // namespace DB
