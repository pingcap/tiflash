#pragma once

#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <common/types.h>

#include <boost/noncopyable.hpp>

namespace DB
{
class LogWithPrefix;
using LogWithPrefixPtr = std::shared_ptr<LogWithPrefix>;

/** LogWithPrefix could print formalized logs.
  * For example, adding prefix for a Poco::Logger with "[task 1 query 2333]" could help us find logs with LogSearch.
  * 
  * Moreover, we can append prefix at any time with the function "append(const String & str)".
  * For example, call append("[InputStream]") could print logs with prefix "[task 1 query 2333] [InputStream]".
  * 
  * Interfaces in LogWithPrefix are definitely the same with the Poco::Logger, so that they could use the same
  * macro such as LOG_INFO() etc.
  */
class LogWithPrefix : private boost::noncopyable
{
public:
    LogWithPrefix(Poco::Logger * log_, const String & prefix_)
        : log(log_)
        , prefix(prefix_)
    {
        if (log == nullptr)
            throw Exception("LogWithPrefix receives nullptr");
    }

    bool trace() const { return log->trace(); }

    void trace(const std::string & msg)
    {
        auto m = prefix + msg;
        log->trace(m);
    }

    bool debug() const { return log->debug(); }

    void debug(const std::string & msg)
    {
        auto m = prefix + msg;
        log->debug(m);
    }

    bool information() const { return log->information(); }

    void information(const std::string & msg)
    {
        auto m = prefix + msg;
        log->information(m);
    }

    bool warning() const { return log->warning(); }

    void warning(const std::string & msg)
    {
        auto m = prefix + msg;
        log->warning(m);
    }

    bool error() const { return log->error(); }

    void error(const std::string & msg)
    {
        auto m = prefix + msg;
        log->error(m);
    }

    Poco::Logger * getLog() const { return log; }

    LogWithPrefixPtr append(const String & str) const { return std::make_shared<LogWithPrefix>(log, prefix + " " + str); }

private:
    Poco::Logger * log;
    const String prefix;
};

} // namespace DB
