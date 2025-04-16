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

#include <Common/Logger_fwd.h>
#include <Interpreters/Settings_fwd.h>
#include <Poco/Net/TCPServer.h>
#include <Poco/ThreadPool.h>
#include <Server/IServer.h>
#include <common/types.h>

namespace DB
{
class TiFlashSecurityConfig;
using TiFlashSecurityConfigPtr = std::shared_ptr<TiFlashSecurityConfig>;

class TCPServersHolder
{
public:
    TCPServersHolder(
        IServer & server_,
        const Settings & settings,
        const TiFlashSecurityConfigPtr & security_config,
        Int64 max_connections,
        const LoggerPtr & log_);

    // terminate all TCP servers when receive exit signal
    void onExit();

private:
    IServer & server;
    const LoggerPtr & log;
    Poco::ThreadPool server_pool;
    std::vector<std::unique_ptr<Poco::Net::TCPServer>> servers;
};
} // namespace DB
