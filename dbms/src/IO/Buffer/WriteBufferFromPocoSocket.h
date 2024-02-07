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

#include <IO/Buffer/BufferWithOwnMemory.h>
#include <IO/Buffer/WriteBuffer.h>
#include <Poco/Net/Socket.h>


namespace DB
{
/** Works with the ready Poco::Net::Socket. Blocking operations.
  */
class WriteBufferFromPocoSocket : public BufferWithOwnMemory<WriteBuffer>
{
protected:
    Poco::Net::Socket & socket;

    /** For error messages. It is necessary to receive this address in advance, because,
      *  for example, if the connection is broken, the address will not be received anymore
      *  (getpeername will return an error).
      */
    Poco::Net::SocketAddress peer_address;


    void nextImpl() override;

public:
    WriteBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    ~WriteBufferFromPocoSocket();
};

} // namespace DB
