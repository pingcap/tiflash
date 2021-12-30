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
    if ((target_source == msg.getSource()) && channel)
        channel->log(msg);
}
} // namespace Poco