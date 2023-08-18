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

#include <Common/StackTrace.h>
#include <Poco/Exception.h>
#include <fmt/format.h>

#include <cerrno>
#include <memory>
#include <vector>


namespace Poco
{
class Logger;
}


namespace DB
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;

class Exception : public Poco::Exception
{
public:
    Exception() = default; /// For deferred initialization.
    explicit Exception(const std::string & msg, int code = 0)
        : Poco::Exception(msg, code)
    {}

    // Format message with fmt::format, like the logging functions.
    template <typename... Args>
    Exception(int code, const std::string & fmt, Args &&... args)
        : Exception(fmt::format(fmt, std::forward<Args>(args)...), code)
    {}

    Exception(const std::string & msg, const std::string & arg, int code = 0)
        : Poco::Exception(msg, arg, code)
    {}
    Exception(const std::string & msg, const Exception & exc, int code = 0)
        : Poco::Exception(msg, exc, code)
        , trace(exc.trace)
    {}
    explicit Exception(const Poco::Exception & exc)
        : Poco::Exception(exc.displayText())
    {}

    const char * name() const throw() override { return "DB::Exception"; }
    const char * className() const throw() override { return "DB::Exception"; }
    DB::Exception * clone() const override { return new DB::Exception(*this); }
    void rethrow() const override { throw *this; }

    /// Add something to the existing message.
    void addMessage(const std::string & arg) { extendedMessage(arg); }

    const StackTrace & getStackTrace() const { return trace; }

private:
    StackTrace trace;
};


/// Contains an additional member `saved_errno`. See the throwFromErrno function.
class ErrnoException : public Exception
{
public:
    explicit ErrnoException(const std::string & msg, int code = 0, int saved_errno_ = 0)
        : Exception(msg, code)
        , saved_errno(saved_errno_)
    {}
    ErrnoException(const std::string & msg, const std::string & arg, int code = 0, int saved_errno_ = 0)
        : Exception(msg, arg, code)
        , saved_errno(saved_errno_)
    {}
    ErrnoException(const std::string & msg, const Exception & exc, int code = 0, int saved_errno_ = 0)
        : Exception(msg, exc, code)
        , saved_errno(saved_errno_)
    {}

    int getErrno() const { return saved_errno; }

private:
    int saved_errno;
};


using Exceptions = std::vector<std::exception_ptr>;


[[noreturn]] void throwFromErrno(const std::string & s, int code = 0, int e = errno);


/** Try to write an exception to the log (and forget about it).
<<<<<<< HEAD
  * Can be used in destructors in the catch-all block.
  */
void tryLogCurrentException(const char * log_name, const std::string & start_of_message = "");
void tryLogCurrentException(const LoggerPtr & logger, const std::string & start_of_message = "");
void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message = "");


/** Prints current exception in canonical format.
  * with_stacktrace - prints stack trace for DB::Exception.
  * check_embedded_stacktrace - if DB::Exception has embedded stacktrace then
  *  only this stack trace will be printed.
  */
=======
 * Can be used in destructors in the catch-all block.
 */
void tryLogCurrentException(const char * log_name, const std::string & start_of_message = "");
void tryLogCurrentException(const LoggerPtr & logger, const std::string & start_of_message = "");
void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message = "");

/** Prints current exception in canonical format.
 * with_stacktrace - prints stack trace for DB::Exception.
 * check_embedded_stacktrace - if DB::Exception has embedded stacktrace then
 *  only this stack trace will be printed.
 */
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
std::string getCurrentExceptionMessage(bool with_stacktrace, bool check_embedded_stacktrace = false);

/// Returns error code from ErrorCodes
int getCurrentExceptionCode();


/// An execution status of any piece of code, contains return code and optional error
struct ExecutionStatus
{
    int code = 0;
    std::string message;

    ExecutionStatus() = default;

    explicit ExecutionStatus(int return_code, const std::string & exception_message = "")
        : code(return_code)
        , message(exception_message)
    {}

    static ExecutionStatus fromCurrentException(const std::string & start_of_message = "");

    std::string serializeText() const;

    void deserializeText(const std::string & data);

    bool tryDeserializeText(const std::string & data);
};


std::string getExceptionMessage(const Exception & e, bool with_stacktrace, bool check_embedded_stacktrace = false);
std::string getExceptionMessage(std::exception_ptr e, bool with_stacktrace);


void rethrowFirstException(const Exceptions & exceptions);


template <typename T>
std::enable_if_t<std::is_pointer_v<T>, T> exception_cast(std::exception_ptr e)
{
    try
    {
        std::rethrow_exception(std::move(e));
    }
    catch (std::remove_pointer_t<T> & concrete)
    {
        return &concrete;
    }
    catch (...)
    {
        return nullptr;
    }
}

namespace exception_details
{
template <typename T, typename... Args>
inline std::string generateLogMessage(const char * condition, T && fmt_str, Args &&... args)
{
<<<<<<< HEAD
    return fmt::format(std::forward<T>(fmt_str), condition, std::forward<Args>(args)...);
}
} // namespace exception_details

#define RUNTIME_CHECK(condition, ExceptionType, ...) \
    do                                               \
    {                                                \
        if (unlikely(!(condition)))                  \
            throw ExceptionType(__VA_ARGS__);        \
=======
    return fmt::format("Assert {} fail!", condition);
}

template <typename... Args>
inline std::string generateFormattedMessage(const char * condition, const char * fmt_str, Args &&... args)
{
    return FmtBuffer()
        .fmtAppend("Assert {} fail! ", condition)
        .fmtAppend(fmt::runtime(fmt_str), std::forward<Args>(args)...)
        .toString();
}

template <typename... Args>
inline Poco::Message generateLogMessage(
    const std::string & logger_name,
    const char * filename,
    int lineno,
    const char * condition,
    Args &&... args)
{
    return Poco::Message(
        logger_name,
        generateFormattedMessage(condition, std::forward<Args>(args)...),
        Poco::Message::PRIO_FATAL,
        filename,
        lineno);
}

const LoggerPtr & getDefaultFatalLogger();

template <typename... Args>
inline void logAndTerminate(
    const char * filename,
    int lineno,
    const char * condition,
    const LoggerPtr & logger,
    Args &&... args)
{
    auto message = generateLogMessage(logger->name(), filename, lineno, condition, std::forward<Args>(args)...);
    if (logger->is(Poco::Message::Priority::PRIO_FATAL))
        logger->log(message);
    try
    {
        throw Exception(message.getText());
    }
    catch (...)
    {
        std::terminate();
    }
}

inline void logAndTerminate(const char * filename, int lineno, const char * condition)
{
    logAndTerminate(filename, lineno, condition, getDefaultFatalLogger());
}

template <typename... Args>
inline void logAndTerminate(
    const char * filename,
    int lineno,
    const char * condition,
    const char * fmt_str,
    Args &&... args)
{
    logAndTerminate(filename, lineno, condition, getDefaultFatalLogger(), fmt_str, std::forward<Args>(args)...);
}

/**
 * Roughly check whether the passed in string may be a string literal expression.
 */
constexpr auto maybeStringLiteralExpr(const std::string_view sv)
{
    return sv.front() == '"' && sv.back() == '"';
}
} // namespace exception_details

#define INTERNAL_RUNTIME_CHECK_APPEND_ARG(r, data, elem)                                                   \
    .fmtAppend(                                                                                            \
        [] {                                                                                               \
            static_assert(!::DB::exception_details::maybeStringLiteralExpr(                                \
                BOOST_PP_STRINGIZE(elem)),                                                                 \
                "Unexpected " BOOST_PP_STRINGIZE(elem) ": Seems that you may be passing a string literal " \
                                                       "to RUNTIME_CHECK? Use RUNTIME_CHECK_MSG instead. " \
                                                       "See tiflash/pull/5777 for details.");              \
            return ", " BOOST_PP_STRINGIZE(elem) " = {}";                                                  \
        }(),                                                                                               \
        elem)

#define INTERNAL_RUNTIME_CHECK_APPEND_ARGS(...) \
    BOOST_PP_SEQ_FOR_EACH(INTERNAL_RUNTIME_CHECK_APPEND_ARG, _, BOOST_PP_VARIADIC_TO_SEQ(__VA_ARGS__))

#define INTERNAL_RUNTIME_CHECK_APPEND_ARGS_OR_NONE(...)            \
    BOOST_PP_IF(                                                   \
        BOOST_PP_EQUAL(BOOST_PP_TUPLE_SIZE((, ##__VA_ARGS__)), 1), \
        BOOST_PP_EXPAND,                                           \
        INTERNAL_RUNTIME_CHECK_APPEND_ARGS)                        \
    (__VA_ARGS__)

/**
 * RUNTIME_CHECK checks the condition, and throws a `DB::Exception` with `LOGICAL_ERROR` error code
 * when the condition does not meet. It is a good practice to check expected conditions in your
 * program frequently to detect errors as early as possible.
 *
 * Unlike RUNTIME_ASSERT, this function is softer that it does not terminate the application.
 *
 * Usually you use this function to verify your code logic, instead of verifying external inputs, as
 * this function exposes internal variables and the conditions you are checking with, while external
 * callers are expecting some non-internal descriptions for their invalid inputs.
 *
 * Examples:
 *
 * ```
 * RUNTIME_CHECK(a != b);         // DB::Exception("Check a != b failed")
 * RUNTIME_CHECK(a != b, a, b);   // DB::Exception("Check a != b failed, a = .., b = ..")
 * ```
 */
#define RUNTIME_CHECK(condition, ...)                                                                              \
    do                                                                                                             \
    {                                                                                                              \
        if (unlikely(!(condition)))                                                                                \
        {                                                                                                          \
            throw ::DB::Exception(                                                                                 \
                ::DB::FmtBuffer()                                                                                  \
                    .append("Check " #condition " failed") INTERNAL_RUNTIME_CHECK_APPEND_ARGS_OR_NONE(__VA_ARGS__) \
                    .toString(),                                                                                   \
                ::DB::ErrorCodes::LOGICAL_ERROR);                                                                  \
        }                                                                                                          \
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
    } while (false)

#define RUNTIME_ASSERT(condition, logger, ...)                                                                      \
    do                                                                                                              \
    {                                                                                                               \
        if (unlikely(!(condition)))                                                                                 \
        {                                                                                                           \
            LOG_FATAL((logger), exception_details::generateLogMessage(#condition, "Assert {} fail! " __VA_ARGS__)); \
            std::terminate();                                                                                       \
        }                                                                                                           \
    } while (false)

} // namespace DB
