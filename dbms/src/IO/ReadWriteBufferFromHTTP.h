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

#pragma once

#include <IO/ConnectionTimeouts.h>
#include <IO/ReadBuffer.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/URI.h>

#include <functional>

#define DEFAULT_HTTP_READ_BUFFER_TIMEOUT 1800
#define DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT 1

namespace DB
{
const int HTTP_TOO_MANY_REQUESTS = 429;


/** Perform HTTP POST request and provide response to read.
  */
class ReadWriteBufferFromHTTP : public ReadBuffer
{
private:
    Poco::URI uri;
    std::string method;
    ConnectionTimeouts timeouts;

    bool is_ssl;
    std::unique_ptr<Poco::Net::HTTPClientSession> session;
    std::istream * istr; /// owned by session
    std::unique_ptr<ReadBuffer> impl;

public:
    using OutStreamCallback = std::function<void(std::ostream &)>;

    explicit ReadWriteBufferFromHTTP(
        const Poco::URI & uri,
        const std::string & method = {},
        OutStreamCallback out_stream_callback = {},
        const ConnectionTimeouts & timeouts = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    bool nextImpl() override;
};

} // namespace DB
