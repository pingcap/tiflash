#include <Common/ReloadableSplitterChannel.h>
#include <Common/TiFlashLogFileChannel.h>
#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <fmt/format.h>
namespace DB
{
void ReloadableSplitterChannel::setPropertiesRecursively(Poco::Logger & logger, Poco::Channel & channel, Poco::Util::AbstractConfiguration & config)
{
    if (typeid(channel) == typeid(TiFlashLogFileChannel))
    {
        TiFlashLogFileChannel * fileChannel = dynamic_cast<TiFlashLogFileChannel *>(&channel);
        std::string rotation_size = config.getRawString("logger.size", "100M");
        std::string purge_count = config.getRawString("logger.count", "1");
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
}
// only support FormattingChannel --> LevelFilterChannel --> TiFlashLogFileChannel or LevelFilterChannel --> TiFlashLogFileChannel or TiFlashLogFileChannel
void ReloadableSplitterChannel::changeProperties(Poco::Logger & logger, Poco::Util::AbstractConfiguration & config)
{
    Poco::FastMutex::ScopedLock lock(_mutex);
    for (auto * chan : _channels)
    {
        setPropertiesRecursively(logger, *chan, config);
    }
}
} // namespace DB