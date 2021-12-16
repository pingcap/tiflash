#pragma once

#include <Common/LogWithPrefix.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Poco/Channel.h>

namespace DB
{
class MPPTaskTracingLogger
{
public:
    static constexpr auto tracing_log_source = "mpp_task_tracing";

    explicit MPPTaskTracingLogger(const MPPTaskId & mpp_task_id);

    void log(const std::string & msg);

private:
    LogWithPrefixPtr logger;
};
} // namespace DB

namespace Poco
{
class Foundation_API MPPTaskTracingChannel : public Channel
{
public:
    void log(const Message & msg) override;

    void setChannel(Channel * pChannel);

    Channel * getChannel() const;

    void open() override;

    void close() override;

protected:
    ~MPPTaskTracingChannel();

private:
    Channel * channel = nullptr;
};
} // namespace Poco