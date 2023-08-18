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

#include <Client/Connection.h>
#include <Common/PoolBase.h>
#include <IO/ConnectionTimeouts.h>

namespace DB
{
/** Interface for connection pools.
  *
  * Usage (using the usual `ConnectionPool` example)
  * ConnectionPool pool(...);
  *
  *    void thread()
  *    {
  *          auto connection = pool.get();
  *        connection->sendQuery("SELECT 'Hello, world!' AS world");
  *    }
  */

class IConnectionPool : private boost::noncopyable
{
public:
    using Entry = PoolBase<Connection>::Entry;

    virtual ~IConnectionPool() = default;

    /// Selects the connection to work.
    /// If force_connected is false, the client must manually ensure that returned connection is good.
    Entry get(const Settings * settings = nullptr, bool force_connected = true)
    {
        return getImpl(settings, force_connected);
    }

protected:
    virtual Entry getImpl(const Settings * settings, bool force_connected) = 0;
};

using ConnectionPoolPtr = std::shared_ptr<IConnectionPool>;
using ConnectionPoolPtrs = std::vector<ConnectionPoolPtr>;

/** A common connection pool, without fault tolerance.
  */
class ConnectionPool
    : public IConnectionPool
    , private PoolBase<Connection>
{
public:
    using Entry = IConnectionPool::Entry;
    using Base = PoolBase<Connection>;

    ConnectionPool(
        unsigned max_connections_,
        const String & host_,
        UInt16 port_,
        const String & default_database_,
        const String & user_,
        const String & password_,
        const ConnectionTimeouts & timeouts,
        const String & client_name_ = "client",
        Protocol::Compression compression_ = Protocol::Compression::Enable,
        Protocol::Secure secure_ = Protocol::Secure::Disable)
        : Base(max_connections_, &Poco::Logger::get("ConnectionPool (" + host_ + ":" + toString(port_) + ")"))
        , host(host_)
        , port(port_)
        , default_database(default_database_)
        , user(user_)
        , password(password_)
        , resolved_address(host_, port_)
        , client_name(client_name_)
        , compression(compression_)
        , secure{secure_}
        , timeouts(timeouts)
    {}

    ConnectionPool(
        unsigned max_connections_,
        const String & host_,
        UInt16 port_,
        const Poco::Net::SocketAddress & resolved_address_,
        const String & default_database_,
        const String & user_,
        const String & password_,
        const ConnectionTimeouts & timeouts,
        const String & client_name_ = "client",
        Protocol::Compression compression_ = Protocol::Compression::Enable,
        Protocol::Secure secure_ = Protocol::Secure::Disable)
        : Base(max_connections_, &Poco::Logger::get("ConnectionPool (" + host_ + ":" + toString(port_) + ")"))
        , host(host_)
        , port(port_)
        , default_database(default_database_)
        , user(user_)
        , password(password_)
        , resolved_address(resolved_address_)
        , client_name(client_name_)
        , compression(compression_)
        , secure{secure_}
        , timeouts(timeouts)
    {}

    using IConnectionPool::get;

    Entry getImpl(const Settings * settings, bool force_connected) override
    {
        Entry entry;
        if (settings)
            entry = Base::get(settings->queue_max_wait_ms.totalMilliseconds());
        else
            entry = Base::get(-1);

        if (force_connected)
            entry->forceConnected();

        return entry;
    }

    const std::string & getHost() const { return host; }

protected:
    /** Creates a new object to put in the pool. */
    ConnectionPtr allocObject() override
    {
        return std::make_shared<Connection>(
            host,
            port,
            resolved_address,
            default_database,
            user,
            password,
            timeouts,
            client_name,
            compression,
            secure);
    }

private:
    String host;
    UInt16 port;
    String default_database;
    String user;
    String password;

    /** The address can be resolved in advance and passed to the constructor. Then `host` and `port` fields are meaningful only for logging.
      * Otherwise, address is resolved in constructor. That is, DNS balancing is not supported.
      */
    Poco::Net::SocketAddress resolved_address;

    String client_name;
    Protocol::Compression compression; /// Whether to compress data when interacting with the server.
    Protocol::Secure secure; /// Whether to encrypt data when interacting with the server.

    ConnectionTimeouts timeouts;
};

} // namespace DB
