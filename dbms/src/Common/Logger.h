// Copyright 2023 PingCAP, Inc.
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
 * `Logger` supports arbitrary "identifiers" based on `Poco::Logger`.
 *
 * Identifiers could be request_id, session_id, etc. They can be used in
 * `LogSearch` when we want to glob all logs related to one
 * request/session/query.
 */
class Logger : private boost::noncopyable
{
public:
    template <typename... Args>
    static LoggerPtr get(Args &&... args)
    {
        FmtBuffer buf;

        size_t i = 0;
        (
            [&] {
                if (fmt::formatted_size(FMT_COMPILE("{}"), args) == 0)
                    return;
                if (i++ > 0)
                    buf.append(" ");
                buf.fmtAppend(FMT_COMPILE("{}"), args);
            }(),
            ...);

        return std::make_shared<Logger>(buf.toString());
    }

    /**
     * Returns a child logger. The child logger inherits all identifiers from the parent logger.
     * Additionally, you can specify more identifiers.
     */
    template <typename... Args>
    LoggerPtr getChild(Args &&... args)
    {
        return Logger::get(identifier(), std::forward<Args>(args)...);
    }

    /**
     * Normally you should call `Logger::get` instead.
     */
    explicit Logger(const std::string & identifier)
        : logger(getPocoLogger())
        , id(identifier)
    {}

    void log(Poco::Message & msg) const { return logger->log(msg); }

    bool is(int level) const { return logger->is(level); }

    const std::string & identifier() const { return id; }

    const std::string & name() const
    {
        // This overwrites the default logger name with our identifier.
        return id;
    }

private:
    Poco::Logger * logger;
    const std::string id;

    static Poco::Logger * getPocoLogger()
    {
        // Always use the Root logger to avoid creating multiple loggers in the Poco logger chain.
        // In this way, our Logger can be easily created and dropped without worrying about
        // concurrency and memory leak.
        static Poco::Logger * l = &Poco::Logger::root();
        return l;
    }
};

} // namespace DB
