#include "MutableSplitterChannel.h"

#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/FormattingChannel.h>

#include <iostream>

#include "TiflashLogFileChannel.h"
namespace DB
{
void MutableSplitterChannel::setPropertiesRecursively(Poco::Channel * channel, Poco::Util::AbstractConfiguration & config)
{
    if (typeid(channel) == typeid(TiflashLogFileChannel))
    {
        TiflashLogFileChannel * fileChannel = dynamic_cast<TiflashLogFileChannel *>(channel);
        fileChannel->setProperty(Poco::FileChannel::PROP_ROTATION, config.getRawString("logger.size", "100M"));
        fileChannel->setProperty(Poco::FileChannel::PROP_PURGECOUNT, config.getRawString("logger.count", "1"));
        return;
    }
    if (typeid(channel) == typeid(Poco::LevelFilterChannel))
    {
        Poco::LevelFilterChannel * levelFilterChannel = dynamic_cast<Poco::LevelFilterChannel *>(channel);
        setPropertiesRecursively(levelFilterChannel->getChannel(), config);
        return;
    }
    if (typeid(channel) == typeid(Poco::FormattingChannel))
    {
        Poco::FormattingChannel * formattingChannel = dynamic_cast<Poco::FormattingChannel *>(channel);
        setPropertiesRecursively(formattingChannel->getChannel(), config);
        return;
    }
    std::cerr << "invalid channel type" << typeid(channel).name() << std::endl;
}
// only support FormattingChannel --> LevelFilterChannel --> TiflashLogFileChannel or LevelFilterChannel --> TiflashLogFileChannel or TiflashLogFileChannel
void MutableSplitterChannel::changeProperties(Poco::Util::AbstractConfiguration & config)
{
    for (auto it : _channels)
    {
        setPropertiesRecursively(it, config);
    }
}
} // namespace DB