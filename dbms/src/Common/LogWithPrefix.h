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

#include <Poco/Logger.h>
#include <fmt/format.h>

#include <boost/noncopyable.hpp>

namespace DB
{
class LogWithPrefix;
using LogWithPrefixPtr = std::shared_ptr<LogWithPrefix>;

/** LogWithPrefix could print formalized logs.
  * For example, adding prefix for a Poco::Logger with "[task 1 query 2333]" could help us find logs with LogSearch.
  * 
  * Moreover, we can append prefix at any time with the function "append(const std::string & str)".
  * For example, call append("[InputStream]") could print logs with prefix "[task 1 query 2333] [InputStream]".
  * 
  * Interfaces in LogWithPrefix are definitely the same with the Poco::Logger, so that they could use the same
  * macro such as LOG_INFO() etc.
  */
class LogWithPrefix : private boost::noncopyable
{
public:
    LogWithPrefix(const std::string & source, const std::string & identifier)
        : logger(&Poco::Logger::get(source))
        , id(identifier)
    {
    }

#define M(level) \
    bool level() const { return logger->level(); }\
    void level(const std::string & msg) const\
    {\
        logger->level(wrapMsg(msg));\
    }

    M(trace)
    M(debug)
    M(information)
    M(warning)
    M(error)
    M(fatal)
#undef M

    void log(const Poco::Message & msg) const
    {
        return logger->log(Poco::Message(msg, wrapMsg(msg.getText())));
    }

    void log(Poco::Message & msg) const
    {
        msg.setText(wrapMsg(msg.getText()));
        return logger->log(msg);
    }

    bool is(int level) const { return logger->is(level); }

    Poco::Channel * getChannel() const { return logger->getChannel(); }

    const std::string & name() const { return logger->name(); }

    const std::string & identifier() const { return id; }

    Poco::Logger * getLog() const { return logger; }

private:
    std::string wrapMsg(const std::string & msg) const
    {
        if (!id.empty())
            return fmt::format("{} {}", id, msg);
        else
            return msg;
    }

    Poco::Logger * logger;
    const std::string id;
};

inline LogWithPrefixPtr getLogWithPrefix(const std::string & name, const std::string & identifier = "")
{
    return std::make_shared<LogWithPrefix>(name, identifier);
}

inline LogWithPrefixPtr getLogWithPrefix(const std::string & name, const LogWithPrefixPtr & log)
{
    if (log)
        return getLogWithPrefix(name, log->identifier());
    return getLogWithPrefix(name);
}

} // namespace DB
