#pragma once

#include <common/Exception.h>
#include <common/logger_useful.h>

#include <boost/noncopyable.hpp>
#include <string>

namespace DB
{

class LogWithPrefix : private boost::noncopyable
{
public:
    LogWithPrefix(Logger * log_, const String & prefix_) : log(log_), prefix(prefix_)
    {
        if (log == nullptr)
            throw Exception("LogWithPrefix receives nullptr");
    }

    bool trace() { return log->trace(); }

    void trace(const std::string & msg)
    {
        auto m = prefix + msg;
        log->trace(m);
    }

    bool debug() { return log->debug(); }

    void debug(const std::string & msg)
    {
        auto m = prefix + msg;
        log->debug(m);
    }

    bool information() { return log->information(); }

    void information(const std::string & msg)
    {
        auto m = prefix + msg;
        log->information(m);
    }

    bool warning() { return log->warning(); }

    void warning(const std::string & msg)
    {
        auto m = prefix + msg;
        log->warning(m);
    }

    bool error() { return log->error(); }

    void error(const std::string & msg)
    {
        auto m = prefix + msg;
        log->error(m);
    }

    Logger * getLog() const { return log; }

    using LogWithPrefixPtr = std::shared_ptr<LogWithPrefix>;

    static LogWithPrefixPtr append(const String & str) { return std::make_shared<LogWithPrefix>(log, prefix + str); }

private:
    Logger * log;
    const String prefix;
};

} // namespace DB
