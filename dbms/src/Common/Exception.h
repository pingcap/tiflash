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

#include <Common/Logger.h>
#include <Common/StackTrace.h>
#include <Poco/Exception.h>
#include <common/defines.h>
#include <common/logger_useful.h>

#include <boost/preprocessor/comparison/equal.hpp>
#include <boost/preprocessor/control/if.hpp>
#include <boost/preprocessor/facilities/expand.hpp>
#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/stringize.hpp>
#include <boost/preprocessor/tuple/size.hpp>
#include <boost/preprocessor/variadic/to_seq.hpp>
#include <cerrno>
#include <memory>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ERROR_DURING_HASH_TABLE_OR_ARENA_RESIZE;
} // namespace ErrorCodes

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
        : Exception(FmtBuffer().fmtAppend(fmt::runtime(fmt), std::forward<Args>(args)...).toString(), code)
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

/// Contains an additional member `saved_errno`. See the throwFromErrno
/// function.
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

class ResizeException : public Exception
{
public:
    explicit ResizeException(const std::string & msg)
        : Exception(msg, ErrorCodes::ERROR_DURING_HASH_TABLE_OR_ARENA_RESIZE)
    {}
};

using Exceptions = std::vector<std::exception_ptr>;

[[noreturn]] void throwFromErrno(const std::string & s, int code = 0, int e = errno);

/** Try to write an exception to the log (and forget about it).
 * Can be used in destructors in the catch-all block.
 */
void tryLogCurrentException(const char * log_name, const std::string & start_of_message = "");
void tryLogCurrentException(const LoggerPtr & logger, const std::string & start_of_message = "");
void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message = "");

void tryLogCurrentFatalException(const char * log_name, const std::string & start_of_message = "");
void tryLogCurrentFatalException(const LoggerPtr & logger, const std::string & start_of_message = "");
void tryLogCurrentFatalException(Poco::Logger * logger, const std::string & start_of_message = "");

/** Prints current exception in canonical format.
 * with_stacktrace - prints stack trace for DB::Exception.
 * check_embedded_stacktrace - if DB::Exception has embedded stacktrace then
 *  only this stack trace will be printed.
 */
std::string getCurrentExceptionMessage(bool with_stacktrace, bool check_embedded_stacktrace = false);

/// Returns error code from ErrorCodes
int getCurrentExceptionCode();

/// An execution status of any piece of code, contains return code and optional
/// error
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
inline std::string generateFormattedMessage(const char * condition)
{
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
    } while (false)

/**
 * Examples:
 *
 * ```
 * RUNTIME_CHECK_MSG(a != b, "invariant not meet");
 * RUNTIME_CHECK_MSG(a != b, "invariant not meet, a = {}, b = {}", a, b);
 * ```
 */
#define RUNTIME_CHECK_MSG(condition, fmt, ...)                                                                      \
    do                                                                                                              \
    {                                                                                                               \
        if (unlikely(!(condition)))                                                                                 \
        {                                                                                                           \
            throw ::DB::Exception(                                                                                  \
                ::DB::FmtBuffer().append("Check " #condition " failed: ").fmtAppend(fmt, ##__VA_ARGS__).toString(), \
                ::DB::ErrorCodes::LOGICAL_ERROR);                                                                   \
        }                                                                                                           \
    } while (false)

/**
 * RUNTIME_ASSERT checks the condition, and triggers a std::terminate to stop the application
 * immediately when the condition does not meet.
 *
 * Examples:
 *
 * ```
 * RUNTIME_ASSERT(a != b);
 * RUNTIME_ASSERT(a != b, "fail");
 * RUNTIME_ASSERT(a != b, "{} does not equal to {}", a, b);
 * RUNTIME_ASSERT(a != b, logger);
 * RUNTIME_ASSERT(a != b, logger, "{} does not equal to {}", a, b);
 * ```
 */
#define RUNTIME_ASSERT(condition, ...)                                 \
    do                                                                 \
    {                                                                  \
        if (unlikely(!(condition)))                                    \
        {                                                              \
            ::DB::exception_details::logAndTerminate(                  \
                &__FILE__[LogFmtDetails::getFileNameOffset(__FILE__)], \
                __LINE__,                                              \
                #condition,                                            \
                ##__VA_ARGS__);                                        \
            /*to make c++ compiler happy.*/                            \
            std::terminate();                                          \
        }                                                              \
    } while (false)

} // namespace DB
