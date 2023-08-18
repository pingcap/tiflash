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

#include <vector>

#include "Poco/Channel.h"
#include "Poco/Foundation.h"
#include "Poco/Message.h"
#include "Poco/Mutex.h"


namespace Poco
{


class Foundation_API LevelFilterChannel : public Channel
/// This channel sends messages only higher then specified level
{
public:
    void log(const Message & msg);
    /// Sends the given Message to all
    /// attaches channels.

    void setProperty(const std::string & name, const std::string & value);
    /// Sets or changes a configuration property.
    ///
    /// Only the "level" property is supported, which allows setting desired level

    void setChannel(Channel * pChannel);
    /// Sets the destination channel to which the formatted
    /// messages are passed on.

    Channel * getChannel() const;
    /// Returns the channel to which the formatted
    /// messages are passed on.

    void open();
    /// Opens the attached channel.

    void close();
    /// Closes the attached channel.

    void setLevel(Message::Priority);
    /// Sets the Logger's log level.
    void setLevel(const std::string & value);
    /// Sets the Logger's log level using a symbolic value.
    Message::Priority getLevel() const;
    /// Returns the Logger's log level.

protected:
    ~LevelFilterChannel();

private:
    Channel * _channel = nullptr;
    Message::Priority _priority = Message::PRIO_ERROR;
};


} // namespace Poco
