#pragma once

#include <Poco/Channel.h>

namespace Poco
{
class Foundation_API SourceFilterChannel : public Channel
{
public:
    void log(const Message & msg) override;

    void setChannel(Channel * pChannel);

    Channel * getChannel() const;

    void open() override;

    void close() override;

    void setSource(std::string value);

    const std::string & getSource() const;

protected:
    ~SourceFilterChannel();

private:
    Channel * channel = nullptr;

    std::string target_source;
};
} // namespace Poco