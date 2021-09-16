#include "MutableSplitterChannel.h"

#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/FormattingChannel.h>
#include <fmt/format.h>

#include <iostream>

#include "TiflashLogFileChannel.h"
namespace DB
{
void MutableSplitterChannel::setPropertiesRecursively(Poco::Logger & logger, Poco::Channel & channel, Poco::Util::AbstractConfiguration & config)
{
    if (typeid(channel) == typeid(TiflashLogFileChannel))
    {
        TiflashLogFileChannel * fileChannel = dynamic_cast<TiflashLogFileChannel *>(&channel);
        std::string rotation_size = config.getRawString("logger.size", "100M");
        std::string purge_count = config.getRawString("logger.count", "1");
        logger.information(fmt::format("set channel rotation:{}, purgecount:{}", rotation_size, purge_count));
        fileChannel->setProperty(Poco::FileChannel::PROP_ROTATION, rotation_size);
        fileChannel->setProperty(Poco::FileChannel::PROP_PURGECOUNT, purge_count);
        return;
    }
    if (typeid(channel) == typeid(Poco::LevelFilterChannel))
    {
        Poco::LevelFilterChannel * levelFilterChannel = dynamic_cast<Poco::LevelFilterChannel *>(&channel);
        setPropertiesRecursively(logger, *levelFilterChannel->getChannel(), config);
        return;
    }
    if (typeid(channel) == typeid(Poco::FormattingChannel))
    {
        Poco::FormattingChannel * formattingChannel = dynamic_cast<Poco::FormattingChannel *>(&channel);
        setPropertiesRecursively(logger, *formattingChannel->getChannel(), config);
        return;
    }
    logger.information(fmt::format("invalid channel type:{}", typeid(channel).name()));
}
// only support FormattingChannel --> LevelFilterChannel --> TiflashLogFileChannel or LevelFilterChannel --> TiflashLogFileChannel or TiflashLogFileChannel
void MutableSplitterChannel::changeProperties(Poco::Logger & logger, Poco::Util::AbstractConfiguration & config)
{
    for (auto it : _channels)
    {
        setPropertiesRecursively(logger, *it, config);
    }
}
} // namespace DB