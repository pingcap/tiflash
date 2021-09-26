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
class ReloadableSplitterChannel : public SplitterChannel
{
public:
    using SplitterChannelValidator = std::function<void(Channel &, Util::AbstractConfiguration &)>;
    void changeProperties(Util::AbstractConfiguration & config);
    // just for test now
    void setPropertiesValidator(SplitterChannelValidator validator) { properties_validator = validator; }
    void validateProperties(Util::AbstractConfiguration & expect_config)
    {
        FastMutex::ScopedLock lock(_mutex);
        for (auto it : _channels)
        {
            properties_validator(*it, expect_config);
        }
    }

protected:
    void setPropertiesRecursively(Channel & channel, Util::AbstractConfiguration & config);
    SplitterChannelValidator properties_validator = nullptr; // just for test now
};
} // namespace Poco