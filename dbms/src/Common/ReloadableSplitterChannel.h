#pragma once


#include <Poco/SplitterChannel.h>

#include <functional>

namespace Poco
{
class Logger;
namespace Util
{
class AbstractConfiguration;
}
}; // namespace Poco
namespace DB
{
class ReloadableSplitterChannel : public Poco::SplitterChannel
{
public:
    using SplitterChannelValidator = std::function<void(Poco::Channel &, Poco::Util::AbstractConfiguration &)>;
    void changeProperties(Poco::Logger & logger, Poco::Util::AbstractConfiguration & config);
    // just for test now
    void setPropertiesValidator(SplitterChannelValidator validator) { properties_validator = validator; }
    void validateProperties(Poco::Util::AbstractConfiguration & expect_config)
    {
        Poco::FastMutex::ScopedLock lock(_mutex);
        for (auto it : _channels)
        {
            properties_validator(*it, expect_config);
        }
    }

protected:
    void setPropertiesRecursively(Poco::Logger & logger, Poco::Channel & channel, Poco::Util::AbstractConfiguration & config);
    SplitterChannelValidator properties_validator = nullptr; // just for test now
};
} // namespace DB