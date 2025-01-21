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

#include <Common/Logger.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/setThreadName.h>
#include <Interpreters/Settings.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Server/TCPHandler.h>
#include <Server/TCPServersHolder.h>
#include <common/logger_useful.h>

#if Poco_NetSSL_FOUND
#include <Common/grpcpp.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SecureServerSocket.h>
#endif

namespace DB
{

struct TCPServer : Poco::Net::TCPServer
{
    TCPServer(
        Poco::Net::TCPServerConnectionFactory::Ptr pFactory,
        Poco::ThreadPool & threadPool,
        const Poco::Net::ServerSocket & socket,
        Poco::Net::TCPServerParams::Ptr pParams)
        : Poco::Net::TCPServer(pFactory, threadPool, socket, pParams)
    {}

protected:
    void run() override
    {
        setThreadName("TCPServer");
        Poco::Net::TCPServer::run();
    }
};

class TCPHandlerFactory : public Poco::Net::TCPServerConnectionFactory
{
private:
    IServer & server;
    LoggerPtr log;

public:
    explicit TCPHandlerFactory(IServer & server_, bool secure_ = false)
        : server(server_)
        , log(Logger::get(fmt::format("TCP{}HandlerFactory", (secure_ ? "S" : ""))))
    {}

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
    {
        LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());

        return new TCPHandler(server, socket);
    }
};

TCPServersHolder::TCPServersHolder(
    IServer & server_,
    const Settings & settings,
    const TiFlashSecurityConfigPtr & security_config,
    Int64 max_connections,
    const LoggerPtr & log_)
    : server(server_)
    , log(log_)
    , server_pool(1, max_connections)
{
    auto & config = server.config();

    std::vector<std::string> listen_hosts = DB::getMultipleValuesFromConfig(config, "", "listen_host");

    bool listen_try = false;
    if (listen_hosts.empty())
    {
        listen_hosts.emplace_back("0.0.0.0");
        listen_try = true;
    }

    auto make_socket_address = [&](const std::string & host, UInt16 port) {
        Poco::Net::SocketAddress socket_address;
        try
        {
            socket_address = Poco::Net::SocketAddress(host, port);
        }
        catch (const Poco::Net::DNSException & e)
        {
            const auto code = e.code();
            if (code == EAI_FAMILY
#if defined(EAI_ADDRFAMILY)
                || code == EAI_ADDRFAMILY
#endif
            )
            {
                LOG_ERROR(
                    log,
                    "Cannot resolve listen_host ({}), error {}: {}."
                    "If it is an IPv6 address and your host has disabled IPv6, then consider to "
                    "specify IPv4 address to listen in <listen_host> element of configuration "
                    "file. Example: <listen_host>0.0.0.0</listen_host>",
                    host,
                    e.code(),
                    e.message());
            }

            throw;
        }
        return socket_address;
    };

    auto socket_bind_listen = [&](auto & socket, const std::string & host, UInt16 port, bool secure = false) {
        auto address = make_socket_address(host, port);
#if !POCO_CLICKHOUSE_PATCH || POCO_VERSION <= 0x02000000 // TODO: fill correct version
        if (secure)
            /// Bug in old poco, listen() after bind() with reusePort param will fail because have no implementation in SecureServerSocketImpl
            /// https://github.com/pocoproject/poco/pull/2257
            socket.bind(address, /* reuseAddress = */ true);
        else
#endif
#if POCO_VERSION < 0x01080000
            socket.bind(address, /* reuseAddress = */ true);
#else
        socket.bind(
            address,
            /* reuseAddress = */ true,
            /* reusePort = */ config.getBool("listen_reuse_port", false));
#endif

        socket.listen(/* backlog = */ config.getUInt("listen_backlog", 64));

        return address;
    };

    for (const auto & listen_host : listen_hosts)
    {
        /// For testing purposes, user may omit tcp_port or http_port or https_port in configuration file.
        try
        {
            /// TCP
            if (config.has("tcp_port"))
            {
                if (security_config->hasTlsConfig())
                {
                    LOG_ERROR(log, "tls config is set but tcp_port_secure is not set.");
                }
                Poco::Net::ServerSocket socket;
                auto address = socket_bind_listen(socket, listen_host, config.getInt("tcp_port"));
                socket.setReceiveTimeout(settings.receive_timeout);
                socket.setSendTimeout(settings.send_timeout);
                servers.emplace_back(
                    new TCPServer(new TCPHandlerFactory(server), server_pool, socket, new Poco::Net::TCPServerParams));

                LOG_INFO(log, "Listening tcp: {}", address.toString());
            }
            else if (security_config->hasTlsConfig())
            {
                LOG_INFO(log, "tcp_port is closed because tls config is set");
            }

            // No TCP server is normal now because we only enable the TCP server
            // under testing deployment
            if (servers.empty())
                LOG_INFO(log, "No TCP server is created");
        }
        catch (const Poco::Net::NetException & e)
        {
            if (listen_try)
                LOG_ERROR(
                    log,
                    "Listen [{}]: {}: {}: {}"
                    "  If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, then consider to "
                    "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                    "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                    " Example for disabled IPv4: <listen_host>::</listen_host>",
                    listen_host,
                    e.code(),
                    e.what(),
                    e.message());
            else
                throw;
        }
    }

    for (auto & server : servers)
        server->start();
}

void TCPServersHolder::onExit()
{
    auto & config = server.config();

    LOG_INFO(log, "Received termination signal, stopping server...");
    LOG_INFO(log, "Waiting for current connections to close.");

    int current_connections = 0;
    for (auto & server : servers)
    {
        server->stop();
        current_connections += server->currentConnections();
    }

    String debug_msg = "Closed all listening sockets.";
    if (current_connections > 0)
    {
        LOG_INFO(log, "{} Waiting for {} outstanding connections.", debug_msg, current_connections);
        const int sleep_max_ms = 1000 * config.getInt("shutdown_wait_unfinished", 5);
        const int sleep_one_ms = 100;
        int sleep_current_ms = 0;
        while (sleep_current_ms < sleep_max_ms)
        {
            current_connections = 0;
            for (auto & server : servers)
                current_connections += server->currentConnections();
            if (!current_connections)
                break;
            sleep_current_ms += sleep_one_ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_one_ms));
        }
    }
    else
    {
        LOG_INFO(log, debug_msg);
    }

    debug_msg = "Closed connections.";
    if (current_connections > 0)
        LOG_INFO(log, "{} But {} remains.", debug_msg, current_connections);
    else
        LOG_INFO(log, debug_msg);
}
} // namespace DB
