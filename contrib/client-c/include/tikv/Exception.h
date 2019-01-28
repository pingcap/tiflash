#pragma once

#include <exception>
#include <string>
#include <Poco/Exception.h>

namespace pingcap {
namespace kv {

const int MismatchClusterIDCode = 1;

class Exception : public Poco::Exception
{
public:
    Exception() {}  /// For deferred initialization.
    Exception(const std::string & msg, int code = 0) : Poco::Exception(msg, code) {}
    Exception(const std::string & msg, const std::string & arg, int code = 0) : Poco::Exception(msg, arg, code) {}
    Exception(const std::string & msg, const Exception & exc, int code = 0) : Poco::Exception(msg, exc, code) {}
    explicit Exception(const Poco::Exception & exc) : Poco::Exception(exc.displayText()) {}

    Exception * clone() const override { return new Exception(*this); }
    void rethrow() const override { throw *this; }

};


}
}
