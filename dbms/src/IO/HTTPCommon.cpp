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

#include <Common/config.h>
#include <IO/HTTPCommon.h>
#if Poco_NetSSL_FOUND
#include <Poco/Net/AcceptCertificateHandler.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/InvalidCertificateHandler.h>
#include <Poco/Net/PrivateKeyPassphraseHandler.h>
#include <Poco/Net/RejectCertificateHandler.h>
#include <Poco/Net/SSLManager.h>
#endif
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/Application.h>


namespace DB
{
void setResponseDefaultHeaders(Poco::Net::HTTPServerResponse & response, unsigned keep_alive_timeout)
{
    if (!response.getKeepAlive())
        return;

    Poco::Timespan timeout(keep_alive_timeout, 0);
    if (timeout.totalSeconds())
        response.set("Keep-Alive", "timeout=" + std::to_string(timeout.totalSeconds()));
}

std::once_flag ssl_init_once;

void SSLInit()
{
    // http://stackoverflow.com/questions/18315472/https-request-in-c-using-poco
#if Poco_NetSSL_FOUND
    Poco::Net::initializeSSL();
#endif
}
} // namespace DB
