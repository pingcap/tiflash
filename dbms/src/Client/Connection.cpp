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

#include <Client/Connection.h>
#include <Client/TimeoutSetter.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Common/NetException.h>
#include <Common/TiFlashBuildInfo.h>
#include <Common/config.h>
#include <Core/Defines.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/Buffer/ReadBufferFromPocoSocket.h>
#include <IO/Buffer/WriteBufferFromPocoSocket.h>
#include <IO/Compression/CompressedReadBuffer.h>
#include <IO/Compression/CompressedWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/ClientInfo.h>
#include <Poco/Net/NetException.h>

#if Poco_NetSSL_FOUND
#include <Poco/Net/SecureStreamSocket.h>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int NETWORK_ERROR;
extern const int SOCKET_TIMEOUT;
extern const int UNEXPECTED_PACKET_FROM_SERVER;
extern const int UNKNOWN_PACKET_FROM_SERVER;
extern const int SUPPORT_IS_DISABLED;
} // namespace ErrorCodes


void Connection::connect()
{
    try
    {
        if (connected)
            disconnect();

        LOG_TRACE(
            log_wrapper.get(),
            "Connecting. Database: {}. User: {}. {}, {}",
            (default_database.empty() ? "(not specified)" : default_database),
            user,
            (static_cast<bool>(secure) ? ". Secure" : ""),
            (static_cast<bool>(compression) ? "" : ". Uncompressed"));
        if (static_cast<bool>(secure))
        {
#if Poco_NetSSL_FOUND
            socket = std::make_unique<Poco::Net::SecureStreamSocket>();
#else
            throw Exception{
                "tcp_secure protocol is disabled because poco library was built without NetSSL support.",
                ErrorCodes::SUPPORT_IS_DISABLED};
#endif
        }
        else
        {
            socket = std::make_unique<Poco::Net::StreamSocket>();
        }
        socket->connect(resolved_address, timeouts.connection_timeout);
        socket->setReceiveTimeout(timeouts.receive_timeout);
        socket->setSendTimeout(timeouts.send_timeout);
        socket->setNoDelay(true);

        in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
        out = std::make_shared<WriteBufferFromPocoSocket>(*socket);

        connected = true;

        sendHello();
        receiveHello();

        LOG_TRACE(
            log_wrapper.get(),
            "Connected to {} server version {}.{}.{}.",
            server_name,
            server_version_major,
            server_version_minor,
            server_version_patch);
    }
    catch (Poco::Net::NetException & e)
    {
        disconnect();

        /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
        throw NetException(e.displayText(), "(" + getDescription() + ")", ErrorCodes::NETWORK_ERROR);
    }
    catch (Poco::TimeoutException & e)
    {
        disconnect();

        /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
        throw NetException(e.displayText(), "(" + getDescription() + ")", ErrorCodes::SOCKET_TIMEOUT);
    }
}


void Connection::disconnect()
{
    in = nullptr;
    out = nullptr; // can write to socket
    if (socket)
        socket->close();
    socket = nullptr;
    connected = false;
}


void Connection::sendHello()
{
    writeVarUInt(Protocol::Client::Hello, *out);
    writeStringBinary(fmt::format("{} {}", TiFlashBuildInfo::getName(), client_name), *out);
    writeVarUInt(TiFlashBuildInfo::getMajorVersion(), *out);
    writeVarUInt(TiFlashBuildInfo::getMinorVersion(), *out);
    writeVarUInt(TiFlashBuildInfo::getPatchVersion(), *out);
    writeStringBinary(default_database, *out);
    writeStringBinary(user, *out);
    writeStringBinary(password, *out);

    out->next();
}


void Connection::receiveHello()
{
    /// Receive hello packet.
    UInt64 packet_type = 0;

    readVarUInt(packet_type, *in);
    if (packet_type == Protocol::Server::Hello)
    {
        readStringBinary(server_name, *in);
        readVarUInt(server_version_major, *in);
        readVarUInt(server_version_minor, *in);
        readVarUInt(server_version_patch, *in);
        readStringBinary(server_timezone, *in);
        readStringBinary(server_display_name, *in);
    }
    else if (packet_type == Protocol::Server::Exception)
        receiveException()->rethrow();
    else
    {
        /// Close connection, to not stay in unsynchronised state.
        disconnect();
        throwUnexpectedPacket(packet_type, "Hello or Exception");
    }
}

void Connection::setDefaultDatabase(const String & database)
{
    default_database = database;
}

const String & Connection::getDefaultDatabase() const
{
    return default_database;
}

const String & Connection::getDescription() const
{
    return description;
}

const String & Connection::getHost() const
{
    return host;
}

UInt16 Connection::getPort() const
{
    return port;
}

void Connection::getServerVersion(String & name, UInt64 & version_major, UInt64 & version_minor, UInt64 & version_patch)
{
    if (!connected)
        connect();

    name = server_name;
    version_major = server_version_major;
    version_minor = server_version_minor;
    version_patch = server_version_patch;
}

const String & Connection::getServerTimezone()
{
    if (!connected)
        connect();

    return server_timezone;
}

const String & Connection::getServerDisplayName()
{
    if (!connected)
        connect();

    return server_display_name;
}

void Connection::forceConnected()
{
    if (!connected)
    {
        connect();
    }
    else if (!ping())
    {
        LOG_TRACE(log_wrapper.get(), "Connection was closed, will reconnect.");
        connect();
    }
}

bool Connection::ping()
{
    TimeoutSetter timeout_setter(*socket, sync_request_timeout, true);
    try
    {
        UInt64 pong = 0;
        writeVarUInt(Protocol::Client::Ping, *out);
        out->next();

        if (in->eof())
            return false;

        readVarUInt(pong, *in);

        /// Could receive late packets with progress. TODO: Maybe possible to fix.
        while (pong == Protocol::Server::Progress)
        {
            receiveProgress();

            if (in->eof())
                return false;

            readVarUInt(pong, *in);
        }

        if (pong != Protocol::Server::Pong)
            throwUnexpectedPacket(pong, "Pong");
    }
    catch (const Poco::Exception & e)
    {
        LOG_TRACE(log_wrapper.get(), "{}", e.displayText());
        return false;
    }

    return true;
}

TablesStatusResponse Connection::getTablesStatus(const TablesStatusRequest & request)
{
    if (!connected)
        connect();

    TimeoutSetter timeout_setter(*socket, sync_request_timeout, true);

    writeVarUInt(Protocol::Client::TablesStatusRequest, *out);
    request.write(*out);
    out->next();

    UInt64 response_type = 0;
    readVarUInt(response_type, *in);

    if (response_type == Protocol::Server::Exception)
        receiveException()->rethrow();
    else if (response_type != Protocol::Server::TablesStatusResponse)
        throwUnexpectedPacket(response_type, "TablesStatusResponse");

    TablesStatusResponse response;
    response.read(*in);
    return response;
}


void Connection::sendQuery(
    const String & query,
    const String & query_id_,
    UInt64 stage,
    const Settings * settings,
    const ClientInfo * client_info,
    bool with_pending_data)
{
    if (!connected)
        connect();

    compression_settings = settings ? CompressionSettings(*settings) : CompressionSettings(CompressionMethod::LZ4);

    query_id = query_id_;

    writeVarUInt(Protocol::Client::Query, *out);
    writeStringBinary(query_id, *out);

    /// Client info.
    {
        ClientInfo client_info_to_send;

        if (!client_info)
        {
            /// No client info passed - means this query initiated by me.
            client_info_to_send.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
            client_info_to_send.fillOSUserHostNameAndVersionInfo();
            client_info_to_send.client_name = fmt::format("{} {}", TiFlashBuildInfo::getName(), client_name);
        }
        else
        {
            /// This query is initiated by another query.
            client_info_to_send = *client_info;
            client_info_to_send.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        }

        client_info_to_send.write(*out);
    }

    /// Per query settings.
    if (settings)
        settings->serialize(*out);
    else
        writeStringBinary("", *out);

    writeVarUInt(stage, *out);
    writeVarUInt(static_cast<bool>(compression), *out);

    writeStringBinary(query, *out);

    maybe_compressed_in.reset();
    maybe_compressed_out.reset();
    block_in.reset();
    block_out.reset();

    /// Send empty block which means end of data.
    if (!with_pending_data)
    {
        sendData(Block());
        out->next();
    }
}


void Connection::sendCancel()
{
    writeVarUInt(Protocol::Client::Cancel, *out);
    out->next();
}


void Connection::sendData(const Block & block, const String & name)
{
    if (!block_out)
    {
        if (compression == Protocol::Compression::Enable)
            maybe_compressed_out = std::make_shared<CompressedWriteBuffer<>>(*out, compression_settings);
        else
            maybe_compressed_out = out;

        block_out = std::make_shared<NativeBlockOutputStream>(*maybe_compressed_out, 1, block.cloneEmpty());
    }

    writeVarUInt(Protocol::Client::Data, *out);
    writeStringBinary(name, *out);

    size_t prev_bytes = out->count();

    block_out->write(block);
    maybe_compressed_out->next();
    out->next();

    if (throttler)
        throttler->add(out->count() - prev_bytes);
}


void Connection::sendPreparedData(ReadBuffer & input, size_t size, const String & name)
{
    /// NOTE 'Throttler' is not used in this method (could use, but it's not important right now).

    writeVarUInt(Protocol::Client::Data, *out);
    writeStringBinary(name, *out);

    if (0 == size)
        copyData(input, *out);
    else
        copyData(input, *out, size);
    out->next();
}


void Connection::sendExternalTablesData(ExternalTablesData & data)
{
    if (data.empty())
    {
        /// Send empty block, which means end of data transfer.
        sendData(Block());
        return;
    }

    Stopwatch watch;
    size_t out_bytes = out ? out->count() : 0;
    size_t maybe_compressed_out_bytes = maybe_compressed_out ? maybe_compressed_out->count() : 0;
    size_t rows = 0;

    for (auto & elem : data)
    {
        elem.first->readPrefix();
        while (Block block = elem.first->read())
        {
            rows += block.rows();
            sendData(block, elem.second);
        }
        elem.first->readSuffix();
    }

    /// Send empty block, which means end of data transfer.
    sendData(Block());

    out_bytes = out->count() - out_bytes;
    maybe_compressed_out_bytes = maybe_compressed_out->count() - maybe_compressed_out_bytes;

    auto get_logging_msg = [&]() -> String {
        const double elapsed_seconds = watch.elapsedSeconds();

        FmtBuffer fmt_buf;
        fmt_buf.fmtAppend(
            "Sent data for {} external tables, total {} rows in {:.3f} sec., {:.3f} rows/sec., "
            "{:.3f} MiB ({:.3f} MiB/sec.)",
            data.size(),
            rows,
            elapsed_seconds,
            1.0 * rows / elapsed_seconds,
            maybe_compressed_out_bytes / 1048576.0,
            maybe_compressed_out_bytes / 1048576.0 / elapsed_seconds);

        if (compression == Protocol::Compression::Enable)
            fmt_buf.fmtAppend(
                ", compressed {:.3f} times to {:.3f} MiB ({:.3f} MiB/sec.)",
                1.0 * maybe_compressed_out_bytes / out_bytes,
                out_bytes / 1048576.0,
                out_bytes / 1048576.0 / elapsed_seconds);
        else
            fmt_buf.append(", no compression.");
        return fmt_buf.toString();
    };

    LOG_DEBUG(log_wrapper.get(), get_logging_msg());
}


bool Connection::poll(size_t timeout_microseconds)
{
    return static_cast<ReadBufferFromPocoSocket &>(*in).poll(timeout_microseconds);
}


bool Connection::hasReadBufferPendingData() const
{
    return static_cast<const ReadBufferFromPocoSocket &>(*in).hasPendingData();
}


Connection::Packet Connection::receivePacket()
{
    try
    {
        Packet res;
        readVarUInt(res.type, *in);

        switch (res.type)
        {
        case Protocol::Server::Data:
            res.block = receiveData();
            return res;

        case Protocol::Server::Exception:
            res.exception = receiveException();
            return res;

        case Protocol::Server::Progress:
            res.progress = receiveProgress();
            return res;

        case Protocol::Server::ProfileInfo:
            res.profile_info = receiveProfileInfo();
            return res;

        case Protocol::Server::Extremes:
            /// Same as above.
            res.block = receiveData();
            return res;

        case Protocol::Server::EndOfStream:
            return res;

        default:
            /// In unknown state, disconnect - to not leave unsynchronised connection.
            disconnect();
            throw Exception(
                "Unknown packet " + toString(res.type) + " from server " + getDescription(),
                ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
        }
    }
    catch (Exception & e)
    {
        /// Add server address to exception message, if need.
        if (e.code() != ErrorCodes::UNKNOWN_PACKET_FROM_SERVER)
            e.addMessage("while receiving packet from " + getDescription());

        throw;
    }
}


Block Connection::receiveData()
{
    initBlockInput();

    String external_table_name;
    readStringBinary(external_table_name, *in);

    size_t prev_bytes = in->count();

    /// Read one block from network.
    Block res = block_in->read();

    if (throttler)
        throttler->add(in->count() - prev_bytes);

    return res;
}


void Connection::initBlockInput()
{
    if (!block_in)
    {
        if (compression == Protocol::Compression::Enable)
            maybe_compressed_in = std::make_shared<CompressedReadBuffer<>>(*in);
        else
            maybe_compressed_in = in;

        block_in = std::make_shared<NativeBlockInputStream>(*maybe_compressed_in, 1);
    }
}


void Connection::setDescription()
{
    description = host + ":" + toString(resolved_address.port());
    auto ip_address = resolved_address.host().toString();

    if (host != ip_address)
        description += ", " + ip_address;
}


std::unique_ptr<Exception> Connection::receiveException()
{
    Exception e;
    readException(e, *in, "Received from " + getDescription());
    return std::unique_ptr<Exception>{e.clone()};
}


Progress Connection::receiveProgress()
{
    Progress progress;
    progress.read(*in);
    return progress;
}


BlockStreamProfileInfo Connection::receiveProfileInfo()
{
    BlockStreamProfileInfo profile_info;
    profile_info.read(*in);
    return profile_info;
}

void Connection::fillBlockExtraInfo(BlockExtraInfo & info) const
{
    info.is_valid = true;
    info.host = host;
    info.resolved_address = resolved_address.toString();
    info.port = port;
    info.user = user;
}

void Connection::throwUnexpectedPacket(UInt64 packet_type, const char * expected) const
{
    throw NetException(
        "Unexpected packet from server " + getDescription() + " (expected " + expected + ", got "
            + String(Protocol::Server::toString(packet_type)) + ")",
        ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
}

} // namespace DB
