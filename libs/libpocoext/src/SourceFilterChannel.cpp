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

#include <Poco/Ext/SourceFilterChannel.h>
#include <Poco/Message.h>

namespace Poco
{
SourceFilterChannel::~SourceFilterChannel()
{
    if (channel)
        channel->release();
}

void SourceFilterChannel::setChannel(Channel * channel_)
{
    if (channel)
        channel->release();
    channel = channel_;
    if (channel)
        channel->duplicate();
}

Channel * SourceFilterChannel::getChannel() const
{
    return channel;
}

void SourceFilterChannel::open()
{
    if (channel)
        channel->open();
}

void SourceFilterChannel::close()
{
    if (channel)
        channel->close();
}

void SourceFilterChannel::setSource(std::string value)
{
    target_source = std::move(value);
}

const std::string & SourceFilterChannel::getSource() const
{
    return target_source;
}

void SourceFilterChannel::log(const Message & msg)
{
    // There may be other identifiers in the msg source.
    // Let's just check whether target_source is contained.
    if (msg.getSource().find(target_source) != std::string::npos && channel)
        channel->log(msg);
}
} // namespace Poco