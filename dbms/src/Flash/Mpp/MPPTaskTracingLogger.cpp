#include <Flash/Mpp/MPPTaskTracingLogger.h>
#include <Flash/Mpp/getMPPTaskLog.h>

namespace DB
{
MPPTaskTracingLogger::MPPTaskTracingLogger(const MPPTaskId & mpp_task_id)
    : logger(getMPPTaskLog(tracing_log_source, mpp_task_id))
{}

void MPPTaskTracingLogger::log(const std::string & msg)
{
    logger->information(msg);
}
} // namespace DB

namespace Poco
{
MPPTaskTracingChannel::~MPPTaskTracingChannel()
{
    if (channel)
        channel->release();
}

void MPPTaskTracingChannel::setChannel(Channel * channel_)
{
    if (channel)
        channel->release();
    channel = channel_;
    if (channel)
        channel->duplicate();
}

Channel * MPPTaskTracingChannel::getChannel() const
{
    return channel;
}

void MPPTaskTracingChannel::open()
{
    if (channel)
        channel->open();
}

void MPPTaskTracingChannel::close()
{
    if (channel)
        channel->close();
}

void MPPTaskTracingChannel::log(const Message & msg)
{
    if ((DB::MPPTaskTracingLogger::tracing_log_source == msg.getSource()) && channel)
        channel->log(msg);
}
} // namespace Poco