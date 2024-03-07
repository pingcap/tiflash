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

#include <Common/Exception.h>
#include <Common/NetException.h>
#include <IO/Buffer/ReadBufferFromPocoSocket.h>
#include <Poco/Net/NetException.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NETWORK_ERROR;
extern const int SOCKET_TIMEOUT;
extern const int CANNOT_READ_FROM_SOCKET;
} // namespace ErrorCodes


bool ReadBufferFromPocoSocket::nextImpl()
{
    ssize_t bytes_read = 0;

    /// Add more details to exceptions.
    try
    {
        bytes_read = socket.impl()->receiveBytes(internal_buffer.begin(), internal_buffer.size());
    }
    catch (const Poco::Net::NetException & e)
    {
        throw NetException(
            e.displayText(),
            "while reading from socket (" + peer_address.toString() + ")",
            ErrorCodes::NETWORK_ERROR);
    }
    catch (const Poco::TimeoutException & e)
    {
        throw NetException(
            "Timeout exceeded while reading from socket (" + peer_address.toString() + ")",
            ErrorCodes::SOCKET_TIMEOUT);
    }
    catch (const Poco::IOException & e)
    {
        throw NetException(
            e.displayText(),
            "while reading from socket (" + peer_address.toString() + ")",
            ErrorCodes::NETWORK_ERROR);
    }

    if (bytes_read < 0)
        throw NetException(
            "Cannot read from socket (" + peer_address.toString() + ")",
            ErrorCodes::CANNOT_READ_FROM_SOCKET);

    if (bytes_read)
        working_buffer.resize(bytes_read);
    else
        return false;

    return true;
}

ReadBufferFromPocoSocket::ReadBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size)
    : BufferWithOwnMemory<ReadBuffer>(buf_size)
    , socket(socket_)
    , peer_address(socket.peerAddress())
{}

bool ReadBufferFromPocoSocket::poll(size_t timeout_microseconds)
{
    return offset() != buffer().size()
        || socket.poll(timeout_microseconds, Poco::Net::Socket::SELECT_READ | Poco::Net::Socket::SELECT_ERROR);
}

} // namespace DB
