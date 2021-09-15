#pragma once


#include <Poco/SplitterChannel.h>
#include <Poco/Util/AbstractConfiguration.h>
namespace DB
{
class MutableSplitterChannel : public Poco::SplitterChannel
{
public:
    void changeProperties(Poco::Util::AbstractConfiguration & config);

protected:
    void setPropertiesRecursively(Poco::Channel * channel, Poco::Util::AbstractConfiguration & config);
};
} // namespace DB