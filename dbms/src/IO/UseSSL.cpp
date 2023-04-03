#include <Common/config.h>
#include <IO/UseSSL.h>

#if Poco_NetSSL_FOUND
#include <Poco/Net/SSLManager.h>
#endif

namespace DB
{
UseSSL::UseSSL()
{
#if Poco_NetSSL_FOUND
    Poco::Net::initializeSSL();
#endif
}

UseSSL::~UseSSL()
{
#if Poco_NetSSL_FOUND
    Poco::Net::uninitializeSSL();
#endif
}
} // namespace DB
