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

/// Macros for convenient usage of Poco logger.

#include <common/MacroUtils.h>
#include <fmt/compile.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <fmt/std.h>

#ifndef QUERY_PREVIEW_LENGTH
#define QUERY_PREVIEW_LENGTH 160
#endif

namespace DB
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;

/// Tracing logs are filtered by SourceFilterChannel.
inline constexpr auto tracing_log_source = "mpp_task_tracing";
} // namespace DB

namespace LogFmtDetails
{
// https://stackoverflow.com/questions/8487986/file-macro-shows-full-path/54335644#54335644
template <typename T, size_t S>
inline constexpr size_t getFileNameOffset(const T (&str)[S], size_t i = S - 1)
{
    return (str[i] == '/' || str[i] == '\\') ? i + 1 : (i > 0 ? getFileNameOffset(str, i - 1) : 0);
}

template <typename T>
inline constexpr size_t getFileNameOffset(T (& /*str*/)[1])
{
    return 0;
}

} // namespace LogFmtDetails

/// Logs a message to a specified logger with that level.
/// If more than one argument is provided,
///  the first argument is interpreted as template with {}-substitutions
///  and the latter arguments treat as values to substitute.
/// If only one argument is provided, it is threat as message without substitutions.

#define LOG_INTERNAL(logger, PRIORITY, message)                             \
    do                                                                      \
    {                                                                       \
        Poco::Message poco_message(                                         \
            /*source*/ (logger)->name(),                                    \
            /*text*/ (message),                                             \
            /*prio*/ (PRIORITY),                                            \
            /*file*/ &__FILE__[LogFmtDetails::getFileNameOffset(__FILE__)], \
            /*line*/ __LINE__);                                             \
        (logger)->log(poco_message);                                        \
    } while (false)


#define LOG_IMPL_0(logger, PRIORITY, message)        \
    do                                               \
    {                                                \
        if ((logger)->is(PRIORITY))                  \
            LOG_INTERNAL(logger, PRIORITY, message); \
    } while (false)

#define LOG_IMPL_1(logger, PRIORITY, fmt_str, ...)                                          \
    do                                                                                      \
    {                                                                                       \
        if ((logger)->is(PRIORITY))                                                         \
        {                                                                                   \
            LOG_INTERNAL(logger, PRIORITY, fmt::format(FMT_COMPILE(fmt_str), __VA_ARGS__)); \
        }                                                                                   \
    } while (false)

#define LOG_IMPL_CHOSER(...) \
    TF_GET_29TH_ARG(         \
        __VA_ARGS__,         \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_1,          \
        LOG_IMPL_0)

// clang-format off
#define LOG_IMPL(logger, PRIORITY, ...) LOG_IMPL_CHOSER(__VA_ARGS__)(logger, PRIORITY, __VA_ARGS__)
// clang-format on

#define LOG_TRACE(logger, ...) LOG_IMPL(logger, Poco::Message::PRIO_TRACE, __VA_ARGS__)
#define LOG_DEBUG(logger, ...) LOG_IMPL(logger, Poco::Message::PRIO_DEBUG, __VA_ARGS__)
#define LOG_INFO(logger, ...) LOG_IMPL(logger, Poco::Message::PRIO_INFORMATION, __VA_ARGS__)
#define LOG_WARNING(logger, ...) LOG_IMPL(logger, Poco::Message::PRIO_WARNING, __VA_ARGS__)
#define LOG_ERROR(logger, ...) LOG_IMPL(logger, Poco::Message::PRIO_ERROR, __VA_ARGS__)
#define LOG_FATAL(logger, ...) LOG_IMPL(logger, Poco::Message::PRIO_FATAL, __VA_ARGS__)
