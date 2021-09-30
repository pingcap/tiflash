#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/Ext/ReloadableSplitterChannel.h>
#include <Poco/Ext/TiFlashLogFileChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Util/AbstractConfiguration.h>
namespace Poco
{
void ReloadableSplitterChannel::setPropertiesRecursively(Channel & channel, Util::AbstractConfiguration & config)
{
    if (typeid(channel) == typeid(TiFlashLogFileChannel))
    {
        TiFlashLogFileChannel * fileChannel = dynamic_cast<TiFlashLogFileChannel *>(&channel);
        fileChannel->setProperty(FileChannel::PROP_ROTATION, config.getRawString("logger.size", "100M"));
        fileChannel->setProperty(FileChannel::PROP_PURGECOUNT, config.getRawString("logger.count", "1"));
        return;
    }
    if (typeid(channel) == typeid(LevelFilterChannel))
    {
        LevelFilterChannel * levelFilterChannel = dynamic_cast<LevelFilterChannel *>(&channel);
        setPropertiesRecursively(*levelFilterChannel->getChannel(), config);
        return;
    }
    if (typeid(channel) == typeid(FormattingChannel))
    {
        FormattingChannel * formattingChannel = dynamic_cast<FormattingChannel *>(&channel);
        setPropertiesRecursively(*formattingChannel->getChannel(), config);
        return;
    }
}
// only support FormattingChannel --> LevelFilterChannel --> TiFlashLogFileChannel or LevelFilterChannel --> TiFlashLogFileChannel or TiFlashLogFileChannel
void ReloadableSplitterChannel::changeProperties(Util::AbstractConfiguration & config)
{
    FastMutex::ScopedLock lock(_mutex);
    for (auto * chan : _channels)
    {
        setPropertiesRecursively(*chan, config);
    }
}
} // namespace Poco