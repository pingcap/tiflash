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
#include <IO/Buffer/WriteBufferFromPocoSocket.h>
#include <Poco/Net/NetException.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NETWORK_ERROR;
extern const int SOCKET_TIMEOUT;
extern const int CANNOT_WRITE_TO_SOCKET;
} // namespace ErrorCodes


void WriteBufferFromPocoSocket::nextImpl()
{
    if (!offset())
        return;

    size_t bytes_written = 0;
    while (bytes_written < offset())
    {
        ssize_t res = 0;

        /// Add more details to exceptions.
        try
        {
            res = socket.impl()->sendBytes(working_buffer.begin() + bytes_written, offset() - bytes_written);
        }
        catch (const Poco::Net::NetException & e)
        {
            throw NetException(
                e.displayText() + " while writing to socket (" + peer_address.toString() + ")",
                ErrorCodes::NETWORK_ERROR);
        }
        catch (const Poco::TimeoutException & e)
        {
            throw NetException(
                "Timeout exceeded while writing to socket (" + peer_address.toString() + ")",
                ErrorCodes::SOCKET_TIMEOUT);
        }
        catch (const Poco::IOException & e)
        {
            throw NetException(
                e.displayText(),
                " while reading from socket (" + peer_address.toString() + ")",
                ErrorCodes::NETWORK_ERROR);
        }

        if (res < 0)
            throw NetException(
                "Cannot write to socket (" + peer_address.toString() + ")",
                ErrorCodes::CANNOT_WRITE_TO_SOCKET);
        bytes_written += res;
    }
}

WriteBufferFromPocoSocket::WriteBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size)
    , socket(socket_)
    , peer_address(socket.peerAddress())
{}

WriteBufferFromPocoSocket::~WriteBufferFromPocoSocket()
{
    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

} // namespace DB
