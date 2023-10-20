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

#include <Interpreters/Settings.h>
#include <Poco/Timespan.h>

namespace DB
{
struct ConnectionTimeouts
{
    Poco::Timespan connection_timeout;
    Poco::Timespan send_timeout;
    Poco::Timespan receive_timeout;
    Poco::Timespan http_keep_alive_timeout{0};

    ConnectionTimeouts() = default;

    ConnectionTimeouts(
        const Poco::Timespan & connection_timeout_,
        const Poco::Timespan & send_timeout_,
        const Poco::Timespan & receive_timeout_)
        : connection_timeout(connection_timeout_)
        , send_timeout(send_timeout_)
        , receive_timeout(receive_timeout_)
    {}

    static Poco::Timespan saturate(const Poco::Timespan & timespan, const Poco::Timespan & limit)
    {
        if (limit.totalMicroseconds() == 0)
            return timespan;
        else
            return (timespan > limit) ? limit : timespan;
    }

    ConnectionTimeouts getSaturated(const Poco::Timespan & limit) const
    {
        return ConnectionTimeouts(
            saturate(connection_timeout, limit),
            saturate(send_timeout, limit),
            saturate(receive_timeout, limit));
    }

    /// Timeouts for the case when we have just single attempt to connect.
    static ConnectionTimeouts getTCPTimeoutsWithoutFailover(const Settings & settings)
    {
        return ConnectionTimeouts(settings.connect_timeout, settings.send_timeout, settings.receive_timeout);
    }

    /// Timeouts for the case when we will try many addresses in a loop.
    static ConnectionTimeouts getTCPTimeoutsWithFailover(const Settings & settings)
    {
        return ConnectionTimeouts(
            settings.connect_timeout_with_failover_ms,
            settings.send_timeout,
            settings.receive_timeout);
    }
};

} // namespace DB
