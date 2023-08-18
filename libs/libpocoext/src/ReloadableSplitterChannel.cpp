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

#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/Ext/ReloadableSplitterChannel.h>
#include <Poco/Ext/SourceFilterChannel.h>
#include <Poco/Ext/TiFlashLogFileChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Util/AbstractConfiguration.h>
namespace Poco
{
void ReloadableSplitterChannel::setPropertiesRecursively(Channel & channel, Util::AbstractConfiguration & config)
{
    if (typeid(channel) == typeid(TiFlashLogFileChannel))
    {
        TiFlashLogFileChannel * file_channel = dynamic_cast<TiFlashLogFileChannel *>(&channel);
        file_channel->setProperty(FileChannel::PROP_ROTATION, config.getRawString("logger.size", "100M"));
        file_channel->setProperty(FileChannel::PROP_PURGECOUNT, config.getRawString("logger.count", "10"));
        return;
    }
    if (typeid(channel) == typeid(LevelFilterChannel))
    {
        LevelFilterChannel * level_filter_channel = dynamic_cast<LevelFilterChannel *>(&channel);
        setPropertiesRecursively(*level_filter_channel->getChannel(), config);
        return;
    }
    if (typeid(channel) == typeid(Poco::SourceFilterChannel))
    {
        Poco::SourceFilterChannel * source_filter_channel = dynamic_cast<Poco::SourceFilterChannel *>(&channel);
        setPropertiesRecursively(*source_filter_channel->getChannel(), config);
        return;
    }
    if (typeid(channel) == typeid(FormattingChannel))
    {
        FormattingChannel * formatting_channel = dynamic_cast<FormattingChannel *>(&channel);
        setPropertiesRecursively(*formatting_channel->getChannel(), config);
        return;
    }
}
// only support FormattingChannel --> LevelFilterChannel/SourceFilterChannel --> ... --> LevelFilterChannel/SourceFilterChannel --> TiFlashLogFileChannel
void ReloadableSplitterChannel::changeProperties(Util::AbstractConfiguration & config)
{
    FastMutex::ScopedLock lock(_mutex);
    for (auto * chan : _channels)
    {
        setPropertiesRecursively(*chan, config);
    }
}
} // namespace Poco