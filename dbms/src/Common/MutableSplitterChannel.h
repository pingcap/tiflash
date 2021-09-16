#pragma once


#include <Poco/Logger.h>
#include <Poco/SplitterChannel.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <functional>
namespace DB
{
class MutableSplitterChannel : public Poco::SplitterChannel
{
public:
    using SplitterChannelValidator = std::function<void(Poco::Channel &, Poco::Util::AbstractConfiguration &)>;
    void changeProperties(Poco::Logger & logger, Poco::Util::AbstractConfiguration & config);
    // just for test now
    void setPropertiesValidator(SplitterChannelValidator validator) { properties_validator = validator; }
    void validateProperties(Poco::Util::AbstractConfiguration & expect_config)
    {
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