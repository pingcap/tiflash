#pragma once

/// Macros for convenient usage of Poco logger.

#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <fmt/format.h>


namespace details
{
template <typename... Ts>
constexpr size_t numArgs(Ts &&...)
{
    return sizeof...(Ts);
}
template <typename T, typename... Ts>
constexpr auto firstArg(T && x, Ts &&...)
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
} // namespace details


/// Logs a message to a specified logger with that level.
/// If more than one argument is provided,
///  the first argument is interpreted as template with {}-substitutions
///  and the latter arguments treat as values to substitute.
/// If only one argument is provided, it is threat as message without substitutions.

#define LOG_IMPL(logger, PRIORITY, ...)                                       \
    do                                                                        \
    {                                                                         \
        if ((logger)->is((PRIORITY)))                                         \
        {                                                                     \
            std::string formatted_message = details::numArgs(__VA_ARGS__) > 1 \
                ? fmt::format(__VA_ARGS__)                                    \
                : details::firstArg(__VA_ARGS__);                             \
            Poco::Message poco_message(                                       \
                /*source*/ (logger)->name(),                                  \
                /*text*/ formatted_message,                                   \
                /*prio*/ (PRIORITY),                                          \
                /*file*/ &__FILE__[details::getFileNameOffset(__FILE__)],     \
                /*line*/ __LINE__);                                           \
            (logger)->log(poco_message);                                      \
        }                                                                     \
    } while (false)

#define LOG_FMT_TRACE(logger, ...) LOG_IMPL(logger, Poco::Message::PRIO_TRACE, __VA_ARGS__)
#define LOG_FMT_DEBUG(logger, ...) LOG_IMPL(logger, Poco::Message::PRIO_DEBUG, __VA_ARGS__)
#define LOG_FMT_INFO(logger, ...) LOG_IMPL(logger, Poco::Message::PRIO_INFORMATION, __VA_ARGS__)
#define LOG_FMT_WARNING(logger, ...) LOG_IMPL(logger, Poco::Message::PRIO_WARNING, __VA_ARGS__)
#define LOG_FMT_ERROR(logger, ...) LOG_IMPL(logger, Poco::Message::PRIO_ERROR, __VA_ARGS__)
