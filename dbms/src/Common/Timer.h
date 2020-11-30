#pragma once

#include <Common/setThreadName.h>
#include <Poco/Util/Timer.h>

namespace DB
{

struct Timer : Poco::Util::Timer
{
    Timer(const char * name) : thread_worker_name(name) {}

protected:
    void run() override
    {
        setThreadName(thread_worker_name);
        Poco::Util::Timer::run();
    }

private:
    const char * thread_worker_name;
};

} // namespace DB
