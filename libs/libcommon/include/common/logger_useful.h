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

/// Macros for convenient usage of Poco logger.

#include <Poco/Logger.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#ifndef QUERY_PREVIEW_LENGTH
#define QUERY_PREVIEW_LENGTH 160
#endif

namespace LogFmtDetails
{
template <typename... Ts>
inline constexpr size_t numArgs(Ts &&...)
{
    return sizeof...(Ts);
}
template <typename T, typename... Ts>
inline constexpr auto firstArg(T && x, Ts &&...)
{
    return std::forward<T>(x);
}

// https://stackoverflow.com/questions/8487986/file-macro-shows-full-path/54335644#54335644
template <typename T, size_t S>
inline constexpr size_t getFileNameOffset(const T (&str)[S], size_t i = S - 1)
{
    return (str[i] == '/' || str[i] == '\\') ? i + 1 : (i > 0 ? getFileNameOffset(str, i - 1) : 0);
}

template <typename T>
inline constexpr size_t getFileNameOffset(T (&/*str*/)[1])
{
    return 0;
}

template <typename S, typename Ignored, typename... Args>
std::string toCheckedFmtStr(const S & format, const Ignored &, Args &&... args)
{
    // The second arg is the same as `format`, just ignore
    // Apply `make_args_checked` for checks `format` validity at compile time.
    // https://fmt.dev/latest/api.html#argument-lists
    return fmt::vformat(format, fmt::make_args_checked<Args...>(format, args...));
}
} // namespace LogFmtDetails

/// Logs a message to a specified logger with that level.

#define LOG_IMPL(logger, PRIORITY, message)                                     \
    do                                                                          \
    {                                                                           \
        if ((logger)->is((PRIORITY)))                                           \
        {                                                                       \
            Poco::Message poco_message(                                         \
                /*source*/ (logger)->name(),                                    \
                /*text*/ message,                                               \
                /*prio*/ (PRIORITY),                                            \
                /*file*/ &__FILE__[LogFmtDetails::getFileNameOffset(__FILE__)], \
                /*line*/ __LINE__);                                             \
            (logger)->log(poco_message);                                        \
        }                                                                       \
    } while (false)

#define LOG_TRACE(logger, message) LOG_IMPL(logger, Poco::Message::PRIO_TRACE, message)
#define LOG_DEBUG(logger, message) LOG_IMPL(logger, Poco::Message::PRIO_DEBUG, message)
#define LOG_INFO(logger, message) LOG_IMPL(logger, Poco::Message::PRIO_INFORMATION, message)
#define LOG_WARNING(logger, message) LOG_IMPL(logger, Poco::Message::PRIO_WARNING, message)
#define LOG_ERROR(logger, message) LOG_IMPL(logger, Poco::Message::PRIO_ERROR, message)
#define LOG_FATAL(logger, message) LOG_IMPL(logger, Poco::Message::PRIO_FATAL, message)


/// Logs a message to a specified logger with that level.
/// If more than one argument is provided,
///  the first argument is interpreted as template with {}-substitutions
///  and the latter arguments treat as values to substitute.
/// If only one argument is provided, it is threat as message without substitutions.

#define LOG_GET_FIRST_ARG(arg, ...) arg
#define LOG_FMT_IMPL(logger, PRIORITY, ...)                                         \
    do                                                                              \
    {                                                                               \
        if ((logger)->is((PRIORITY)))                                               \
        {                                                                           \
            std::string formatted_message = LogFmtDetails::numArgs(__VA_ARGS__) > 1 \
                ? LogFmtDetails::toCheckedFmtStr(                                   \
                    FMT_STRING(LOG_GET_FIRST_ARG(__VA_ARGS__)),                     \
                    __VA_ARGS__)                                                    \
                : LogFmtDetails::firstArg(__VA_ARGS__);                             \
            Poco::Message poco_message(                                             \
                /*source*/ (logger)->name(),                                        \
                /*text*/ formatted_message,                                         \
                /*prio*/ (PRIORITY),                                                \
                /*file*/ &__FILE__[LogFmtDetails::getFileNameOffset(__FILE__)],     \
                /*line*/ __LINE__);                                                 \
            (logger)->log(poco_message);                                            \
        }                                                                           \
    } while (false)

#define LOG_FMT_TRACE(logger, ...) LOG_FMT_IMPL(logger, Poco::Message::PRIO_TRACE, __VA_ARGS__)
#define LOG_FMT_DEBUG(logger, ...) LOG_FMT_IMPL(logger, Poco::Message::PRIO_DEBUG, __VA_ARGS__)
#define LOG_FMT_INFO(logger, ...) LOG_FMT_IMPL(logger, Poco::Message::PRIO_INFORMATION, __VA_ARGS__)
#define LOG_FMT_WARNING(logger, ...) LOG_FMT_IMPL(logger, Poco::Message::PRIO_WARNING, __VA_ARGS__)
#define LOG_FMT_ERROR(logger, ...) LOG_FMT_IMPL(logger, Poco::Message::PRIO_ERROR, __VA_ARGS__)
#define LOG_FMT_FATAL(logger, ...) LOG_FMT_IMPL(logger, Poco::Message::PRIO_FATAL, __VA_ARGS__)
