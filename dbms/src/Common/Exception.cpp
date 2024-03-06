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

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Common/Logger.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Poco/String.h>
#include <common/demangle.h>
#include <common/logger_useful.h>
#include <cxxabi.h>
#include <errno.h>
#include <string.h>

namespace DB
{
namespace ErrorCodes
{
extern const int POCO_EXCEPTION;
extern const int STD_EXCEPTION;
extern const int UNKNOWN_EXCEPTION;
extern const int CANNOT_TRUNCATE_FILE;
} // namespace ErrorCodes

void throwFromErrno(const std::string & s, int code, int e)
{
    const size_t buf_size = 128;
    char buf[buf_size];
#ifndef _GNU_SOURCE
    int rc = strerror_r(e, buf, buf_size);
#ifdef __APPLE__
    if (rc != 0 && rc != EINVAL)
#else
    if (rc != 0)
#endif
    {
        std::string tmp = std::to_string(code);
        const char * code = tmp.c_str();
        const char * unknown_message = "Unknown error ";
        strcpy(buf, unknown_message);
        strcpy(buf + strlen(unknown_message), code);
    }
    throw ErrnoException(s + ", errno: " + toString(e) + ", strerror: " + std::string(buf), code, e);
#else
    throw ErrnoException(
        s + ", errno: " + toString(e) + ", strerror: " + std::string(strerror_r(e, buf, sizeof(buf))),
        code,
        e);
#endif
}

void tryLogCurrentException(const char * log_name, const std::string & start_of_message)
{
    tryLogCurrentException(&Poco::Logger::get(log_name), start_of_message);
}

void tryLogCurrentFatalException(const char * log_name, const std::string & start_of_message)
{
    tryLogCurrentFatalException(&Poco::Logger::get(log_name), start_of_message);
}

#define TRY_LOG_CURRENT_EXCEPTION(logger, prio, start_of_message) \
    try                                                           \
    {                                                             \
        LOG_IMPL(                                                 \
            (logger),                                             \
            prio,                                                 \
            "{}{}{}",                                             \
            (start_of_message),                                   \
            ((start_of_message).empty() ? "" : ": "),             \
            getCurrentExceptionMessage(true));                    \
    }                                                             \
    catch (...)                                                   \
    {}

void tryLogCurrentException(const LoggerPtr & logger, const std::string & start_of_message)
{
    TRY_LOG_CURRENT_EXCEPTION(logger, Poco::Message::PRIO_ERROR, start_of_message);
}

void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message)
{
    TRY_LOG_CURRENT_EXCEPTION(logger, Poco::Message::PRIO_ERROR, start_of_message);
}

void tryLogCurrentFatalException(const LoggerPtr & logger, const std::string & start_of_message)
{
    TRY_LOG_CURRENT_EXCEPTION(logger, Poco::Message::PRIO_FATAL, start_of_message);
}

void tryLogCurrentFatalException(Poco::Logger * logger, const std::string & start_of_message)
{
    TRY_LOG_CURRENT_EXCEPTION(logger, Poco::Message::PRIO_FATAL, start_of_message);
}

#undef TRY_LOG_CURRENT_EXCEPTION

std::string getCurrentExceptionMessage(bool with_stacktrace, bool check_embedded_stacktrace)
{
    FmtBuffer buffer;

    try
    {
        throw;
    }
    catch (const Exception & e)
    {
        buffer.append(getExceptionMessage(e, with_stacktrace, check_embedded_stacktrace));
    }
    catch (const Poco::Exception & e)
    {
        try
        {
            buffer.fmtAppend(
                "Poco::Exception. Code: {}, e.code() = {}, e.displayText() = {}, e.what() = {}",
                ErrorCodes::POCO_EXCEPTION,
                e.code(),
                e.displayText(),
                e.what());
        }
        catch (...)
        {}
    }
    catch (const std::exception & e)
    {
        try
        {
            int status = 0;
            auto name = demangle(typeid(e).name(), status);

            if (status)
                name += " (demangling status: " + toString(status) + ")";

            buffer.fmtAppend(
                "std::exception. Code: {}, type: {}, e.what() = {}",
                ErrorCodes::STD_EXCEPTION,
                name,
                e.what());
        }
        catch (...)
        {}
    }
    catch (...)
    {
        try
        {
            int status = 0;
            auto name = demangle(abi::__cxa_current_exception_type()->name(), status);

            if (status)
                name += " (demangling status: " + toString(status) + ")";

            buffer.fmtAppend("Unknown exception. Code: {}, type: {}", ErrorCodes::UNKNOWN_EXCEPTION, name);
        }
        catch (...)
        {}
    }

    return buffer.toString();
}

int getCurrentExceptionCode()
{
    try
    {
        throw;
    }
    catch (const Exception & e)
    {
        return e.code();
    }
    catch (const Poco::Exception & e)
    {
        return ErrorCodes::POCO_EXCEPTION;
    }
    catch (const std::exception & e)
    {
        return ErrorCodes::STD_EXCEPTION;
    }
    catch (...)
    {
        return ErrorCodes::UNKNOWN_EXCEPTION;
    }
}

void rethrowFirstException(const Exceptions & exceptions)
{
    for (const auto & exception : exceptions)
        if (exception)
            std::rethrow_exception(exception);
}

std::string getExceptionMessage(const Exception & e, bool with_stacktrace, bool check_embedded_stacktrace)
{
    FmtBuffer buffer;

    try
    {
        std::string text = e.displayText();

        bool has_embedded_stack_trace = false;
        if (check_embedded_stacktrace)
        {
            auto embedded_stack_trace_pos = text.find("Stack trace");
            has_embedded_stack_trace = embedded_stack_trace_pos != std::string::npos;
            if (!with_stacktrace && has_embedded_stack_trace)
            {
                text.resize(embedded_stack_trace_pos);
                Poco::trimRightInPlace(text);
            }
        }

        buffer.fmtAppend("Code: {}, e.displayText() = {}, e.what() = {}", e.code(), text, e.what());

        if (with_stacktrace && !has_embedded_stack_trace)
            buffer.append(", Stack trace:\n\n").append(e.getStackTrace().toString());
    }
    catch (...)
    {}

    return buffer.toString();
}

std::string getExceptionMessage(std::exception_ptr e, bool with_stacktrace)
{
    try
    {
        std::rethrow_exception(std::move(e));
    }
    catch (...)
    {
        return getCurrentExceptionMessage(with_stacktrace);
    }
}

std::string ExecutionStatus::serializeText() const
{
    WriteBufferFromOwnString wb;
    wb << code << "\n" << escape << message;
    return wb.str();
}

void ExecutionStatus::deserializeText(const std::string & data)
{
    ReadBufferFromString rb(data);
    rb >> code >> "\n" >> escape >> message;
}

bool ExecutionStatus::tryDeserializeText(const std::string & data)
{
    try
    {
        deserializeText(data);
    }
    catch (...)
    {
        return false;
    }

    return true;
}

ExecutionStatus ExecutionStatus::fromCurrentException(const std::string & start_of_message)
{
    String msg = (start_of_message.empty() ? "" : (start_of_message + ": ")) + getCurrentExceptionMessage(false, true);
    return ExecutionStatus(getCurrentExceptionCode(), msg);
}

namespace exception_details
{
const LoggerPtr & getDefaultFatalLogger()
{
    static const auto logger = Logger::get("Fatal");
    return logger;
}
} // namespace exception_details

} // namespace DB
