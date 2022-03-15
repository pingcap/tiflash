// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
        : logger(log_)
        , prefix(addSuffixSpace(prefix_))
    {
        if (logger == nullptr)
            throw Exception("LogWithPrefix receives nullptr");
    }

    bool trace() const { return logger->trace(); }

    void trace(const std::string & msg)
    {
        auto m = prefix + msg;
        logger->trace(m);
    }

    bool debug() const { return logger->debug(); }

    void debug(const std::string & msg)
    {
        auto m = prefix + msg;
        logger->debug(m);
    }

    bool information() const { return logger->information(); }

    void information(const std::string & msg)
    {
        auto m = prefix + msg;
        logger->information(m);
    }

    bool warning() const { return logger->warning(); }

    void warning(const std::string & msg)
    {
        auto m = prefix + msg;
        logger->warning(m);
    }

    bool error() const { return logger->error(); }

    void error(const std::string & msg)
    {
        auto m = prefix + msg;
        logger->error(m);
    }

    Poco::Logger * getLog() const { return logger; }

    LogWithPrefixPtr append(const String & str) const { return std::make_shared<LogWithPrefix>(logger, prefix + str); }


    void log(const Poco::Message & msg) { return logger->log(Poco::Message(msg, prefix + msg.getText())); }

    bool is(int level) const { return logger->is(level); }

    Poco::Channel * getChannel() const { return logger->getChannel(); }

    const std::string & name() const { return logger->name(); }

private:
    Poco::Logger * logger;
    // prefix must be ascii.
    const String prefix;

    static String addSuffixSpace(const String & str)
    {
        if (str.empty() || std::isspace(str.back()))
            return str;
        else
            return str + ' ';
    }
};

inline LogWithPrefixPtr getLogWithPrefix(const LogWithPrefixPtr & log = nullptr, const String & name = "name: N/A")
{
    if (log == nullptr)
    {
        return std::make_shared<LogWithPrefix>(&Poco::Logger::get(name), "");
    }

    return log->append(name);
}

} // namespace DB
