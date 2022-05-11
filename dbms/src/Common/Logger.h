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

#include <Common/FmtUtils.h>
#include <Poco/Logger.h>

#include <boost/noncopyable.hpp>

namespace DB
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;

/**
 * Logger is to support identifiers based on Poco::Logger.
 *
 * Identifiers could be request_id, session_id, etc. They can be used in `LogSearch` when we want to
 * glob all logs related to one request/session/query.
 *
 * Logger will print all identifiers at the front of each log record (and after the `source`).
 *
 * Interfaces in Logger are definitely the same with the Poco::Logger, so that they could use the same
 * macro such as LOG_INFO() etc.
 */
class Logger : private boost::noncopyable
{
public:
    static LoggerPtr get(const std::string & source)
    {
        return std::make_shared<Logger>(source, "");
    }

    template <typename T, typename... Args>
    static LoggerPtr get(const std::string & source, T && first_identifier, Args &&... rest)
    {
        FmtBuffer buf;
        return getInternal(source, buf, std::forward<T>(first_identifier), std::forward<Args>(rest)...);
    }

    template <typename T, typename... Args>
    static LoggerPtr get(Poco::Logger * source_log, T && first_identifier, Args &&... rest)
    {
        FmtBuffer buf;
        return getInternal(source_log, buf, std::forward<T>(first_identifier), std::forward<Args>(rest)...);
    }

    Logger(const std::string & source, const std::string & identifier)
        : Logger(&Poco::Logger::get(source), identifier)
    {
    }

    Logger(Poco::Logger * source_log, const std::string & identifier)
        : logger(source_log)
        , id(identifier)
    {
    }

#define M(level)                                   \
    bool level() const { return logger->level(); } \
    void level(const std::string & msg) const      \
    {                                              \
        if (id.empty())                            \
            logger->level(msg);                    \
        else                                       \
            logger->level(wrapMsg(msg));           \
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
        if (id.empty())
            return logger->log(msg);
        else
            return logger->log(Poco::Message(msg, wrapMsg(msg.getText())));
    }

    void log(Poco::Message & msg) const
    {
        if (!id.empty())
            msg.setText(wrapMsg(msg.getText()));
        return logger->log(msg);
    }

    bool is(int level) const { return logger->is(level); }

    Poco::Channel * getChannel() const { return logger->getChannel(); }

    const std::string & name() const { return logger->name(); }

    const std::string & identifier() const { return id; }

    Poco::Logger * getLog() const { return logger; }

private:
    template <typename T, typename... Args>
    static LoggerPtr getInternal(const std::string & source, FmtBuffer & buf, T && first, Args &&... args)
    {
        buf.fmtAppend("{} ", std::forward<T>(first));
        return getInternal(source, buf, std::forward<Args>(args)...);
    }

    template <typename T>
    static LoggerPtr getInternal(const std::string & source, FmtBuffer & buf, T && identifier)
    {
        buf.fmtAppend("{}", std::forward<T>(identifier));
        return std::make_shared<Logger>(source, buf.toString());
    }

    template <typename T, typename... Args>
    static LoggerPtr getInternal(Poco::Logger * source_log, FmtBuffer & buf, T && first, Args &&... args)
    {
        buf.fmtAppend("{} ", std::forward<T>(first));
        return getInternal(source_log, buf, std::forward<Args>(args)...);
    }

    template <typename T>
    static LoggerPtr getInternal(Poco::Logger * source_log, FmtBuffer & buf, T && identifier)
    {
        buf.fmtAppend("{}", std::forward<T>(identifier));
        return std::make_shared<Logger>(source_log, buf.toString());
    }

    std::string wrapMsg(const std::string & msg) const
    {
        return fmt::format("{} {}", id, msg);
    }

    Poco::Logger * logger;
    const std::string id;
};

} // namespace DB
