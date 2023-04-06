// Copyright 2023 PingCAP, Ltd.
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

#include <iostream>
#include <common/logger_useful.h>

namespace DB::S3
{
/**
 * Wrapper of IOStream to store response stream and corresponding HTTP session.
 */
template <typename Session>
class SessionAwareIOStream : public std::iostream
{
public:
    SessionAwareIOStream(Session session_, std::streambuf * sb, const String & uri_)
        : std::iostream(sb)
        , session(std::move(session_))
        , uri(uri_)
    {
        const auto * network_exception = session->networkException();
        if (network_exception != nullptr)
        {
            LOG_ERROR(Logger::get(), "{} uri={} exception={}:{}:{}", __PRETTY_FUNCTION__, uri, network_exception->message(), network_exception->what(), network_exception->code());
        }        
    }

    ~SessionAwareIOStream()
    {
        const auto * network_exception = session->networkException();
        if (network_exception != nullptr)
        {
            LOG_ERROR(Logger::get(), "{} uri={} exception={}:{}:{}", __PRETTY_FUNCTION__, uri, network_exception->message(), network_exception->what(), network_exception->code());
        }  
    }

private:
    /// Poco HTTP session is holder of response stream.
    Session session;
    String uri;
};

} // namespace DB::S3
