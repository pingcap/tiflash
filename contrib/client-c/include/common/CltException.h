#pragma once

#include <Poco/Exception.h>
#include <exception>
#include <string>

namespace pingcap
{

const int MismatchClusterIDCode = 1;
const int GRPCErrorCode = 2;
const int InitClusterIDFailed = 3;
const int UpdatePDLeaderFailed = 4;
const int TimeoutError = 5;
const int RegionUnavailable = 6;
const int LogicalError = 7;
const int LockError = 8;

class Exception : public Poco::Exception
{
public:
    Exception() {} /// For deferred initialization.
    Exception(const std::string & msg, int code = 0) : Poco::Exception(msg, code) {}
    Exception(const std::string & msg, const std::string & arg, int code = 0) : Poco::Exception(msg, arg, code) {}
    Exception(const std::string & msg, const Exception & exc, int code = 0) : Poco::Exception(msg, exc, code) {}
    explicit Exception(const Poco::Exception & exc) : Poco::Exception(exc.displayText()) {}

    Exception * clone() const override { return new Exception(*this); }
    void rethrow() const override { throw *this; }
};

} // namespace pingcap
