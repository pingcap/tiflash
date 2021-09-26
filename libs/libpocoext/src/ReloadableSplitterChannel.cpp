#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/Ext/ReloadableSplitterChannel.h>
#include <Poco/Ext/TiFlashLogFileChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Util/AbstractConfiguration.h>
namespace DB
{
void ReloadableSplitterChannel::setPropertiesRecursively(Poco::Channel & channel, Poco::Util::AbstractConfiguration & config)
{
    if (typeid(channel) == typeid(TiFlashLogFileChannel))
    {
        TiFlashLogFileChannel * fileChannel = dynamic_cast<TiFlashLogFileChannel *>(&channel);
        fileChannel->setProperty(Poco::FileChannel::PROP_ROTATION, config.getRawString("logger.size", "100M"));
        fileChannel->setProperty(Poco::FileChannel::PROP_PURGECOUNT, config.getRawString("logger.count", "1"));
        return;
    }
    if (typeid(channel) == typeid(Poco::LevelFilterChannel))
    {
        Poco::LevelFilterChannel * levelFilterChannel = dynamic_cast<Poco::LevelFilterChannel *>(&channel);
        setPropertiesRecursively(*levelFilterChannel->getChannel(), config);
        return;
    }
    if (typeid(channel) == typeid(Poco::FormattingChannel))
    {
        Poco::FormattingChannel * formattingChannel = dynamic_cast<Poco::FormattingChannel *>(&channel);
        setPropertiesRecursively(*formattingChannel->getChannel(), config);
        return;
    }
}
// only support FormattingChannel --> LevelFilterChannel --> TiFlashLogFileChannel or LevelFilterChannel --> TiFlashLogFileChannel or TiFlashLogFileChannel
void ReloadableSplitterChannel::changeProperties(Poco::Util::AbstractConfiguration & config)
{
    Poco::FastMutex::ScopedLock lock(_mutex);
    for (auto * chan : _channels)
    {
        setPropertiesRecursively(*chan, config);
    }
}
} // namespace DB