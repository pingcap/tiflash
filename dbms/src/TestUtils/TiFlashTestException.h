#pragma once

#include <vector>
#include <memory>

#include <Poco/Exception.h>

#include <Common/StackTrace.h>

namespace Poco { class Logger; }


namespace DB
{
namespace tests
{

/// TiFlashTestException is used for test helper functions where gtest's ASSERT macros don't fit.
/// TiFlashTestException is intended not derived from DB::Exception so that production code hardly
/// catch it mistakenly.
class TiFlashTestException : public Poco::Exception
{
public:
    TiFlashTestException() {}  /// For deferred initialization.
    TiFlashTestException(const std::string & msg, int code = 0) : Poco::Exception(msg, code) {}
    TiFlashTestException(const std::string & msg, const std::string & arg, int code = 0) : Poco::Exception(msg, arg, code) {}
    TiFlashTestException(const std::string & msg, const DB::Exception & exc, int code = 0) : Poco::Exception(msg, exc, code), trace(exc.getStackTrace()) {}
    TiFlashTestException(const std::string & msg, const TiFlashTestException & exc, int code = 0) : Poco::Exception(msg, exc, code), trace(exc.trace) {}
    explicit TiFlashTestException(const Poco::Exception & exc) : Poco::Exception(exc.displayText()) {}

    const char * name() const throw() override { return "DB::tests::TiFlashTestException"; }
    const char * className() const throw() override { return "DB::tests::TiFlashException"; }
    TiFlashTestException * clone() const override { return new TiFlashTestException(*this); }
    void rethrow() const override { throw *this; }

    /// Add something to the existing message.
    void addMessage(const std::string & arg) { extendedMessage(arg); }

    const StackTrace & getStackTrace() const { return trace; }

private:
    StackTrace trace;
};


} // namespace tests
} // namespace DB

